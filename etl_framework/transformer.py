"""Reusable PySpark transformer helpers.

These functions map common ETL transformation concepts to plain PySpark
DataFrame operations while staying cloud agnostic. They do not depend on
Databricks-only APIs and can run anywhere Spark runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any, Iterable, Mapping, Optional, Sequence, Union

ColumnLike = Union[str, Any]


class TransformerError(Exception):
    """Raised when a transformation cannot be built."""


@dataclass(frozen=True)
class RouteRule:
    """Rule used by ``router`` to split a DataFrame into named outputs."""

    name: str
    condition: ColumnLike


@dataclass(frozen=True)
class UpdateStrategyRule:
    """Rule used by ``update_strategy`` to assign a DML operation."""

    operation: str
    condition: ColumnLike


def _require_columns(df: Any, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise TransformerError(f"Missing required column(s): {', '.join(missing)}")


def _functions() -> Any:
    try:
        return import_module("pyspark.sql.functions")
    except ImportError as exc:
        raise TransformerError("PySpark is required for transformations.") from exc


def _window() -> Any:
    try:
        return import_module("pyspark.sql.window").Window
    except ImportError as exc:
        raise TransformerError("PySpark is required for window transformations.") from exc


def _as_column(value: ColumnLike) -> Any:
    if isinstance(value, str):
        return _functions().expr(value)
    return value


def _normalize_columns(columns: Union[str, Sequence[str]]) -> list[str]:
    if isinstance(columns, str):
        return [columns]
    return list(columns)


def rank(
    df: Any,
    order_by: Union[str, Sequence[str]],
    partition_by: Optional[Union[str, Sequence[str]]] = None,
    rank_column: str = "rank",
    method: str = "row_number",
) -> Any:
    """Add a rank column using a Spark window.

    ``method`` supports ``row_number``, ``rank``, and ``dense_rank``.
    Prefix an order column with ``-`` for descending order, for example
    ``order_by="-amount"``.
    """

    order_columns = _normalize_columns(order_by)
    partition_columns = _normalize_columns(partition_by) if partition_by else []
    _require_columns(df, [column.lstrip("-") for column in order_columns])
    _require_columns(df, partition_columns)

    F = _functions()
    Window = _window()

    order_exprs = [
        F.col(column[1:]).desc() if column.startswith("-") else F.col(column).asc()
        for column in order_columns
    ]
    window_spec = Window.partitionBy(*partition_columns).orderBy(*order_exprs)

    rankers = {
        "row_number": F.row_number,
        "rank": F.rank,
        "dense_rank": F.dense_rank,
    }
    if method not in rankers:
        raise TransformerError(
            "Rank method must be one of: row_number, rank, dense_rank."
        )

    return df.withColumn(rank_column, rankers[method]().over(window_spec))


def router(
    df: Any,
    rules: Sequence[RouteRule],
    default_name: Optional[str] = "default",
) -> dict[str, Any]:
    """Split a DataFrame into named outputs based on filter rules."""

    if not rules:
        raise TransformerError("At least one route rule is required.")

    outputs: dict[str, Any] = {}
    matched_condition = None

    for rule in rules:
        if not rule.name:
            raise TransformerError("Route rule name cannot be empty.")
        condition = _as_column(rule.condition)
        outputs[rule.name] = df.filter(condition)
        matched_condition = condition if matched_condition is None else (
            matched_condition | condition
        )

    if default_name:
        outputs[default_name] = df.filter(~matched_condition)

    return outputs


def sorter(
    df: Any,
    order_by: Union[str, Sequence[str]],
) -> Any:
    """Sort a DataFrame by one or more columns.

    Prefix a column with ``-`` for descending order.
    """

    columns = _normalize_columns(order_by)
    _require_columns(df, [column.lstrip("-") for column in columns])

    F = _functions()
    sort_exprs = [
        F.col(column[1:]).desc() if column.startswith("-") else F.col(column).asc()
        for column in columns
    ]
    return df.orderBy(*sort_exprs)


def aggregator(
    df: Any,
    group_by: Union[str, Sequence[str]],
    aggregations: Mapping[str, str],
) -> Any:
    """Group rows and calculate aggregate expressions.

    ``aggregations`` maps output column names to SQL expressions, for example:
    ``{"total_amount": "sum(amount)", "order_count": "count(*)"}``.
    """

    group_columns = _normalize_columns(group_by)
    _require_columns(df, group_columns)
    if not aggregations:
        raise TransformerError("At least one aggregation is required.")

    F = _functions()
    expressions = [
        F.expr(expression).alias(alias) for alias, expression in aggregations.items()
    ]
    return df.groupBy(*group_columns).agg(*expressions)


def lookup(
    df: Any,
    lookup_df: Any,
    on: Union[str, Sequence[str], Mapping[str, str]],
    how: str = "left",
    broadcast_lookup: bool = False,
) -> Any:
    """Join to a lookup DataFrame, defaulting to a left lookup join."""

    return joiner(
        df,
        lookup_df,
        on=on,
        how=how,
        broadcast_right=broadcast_lookup,
    )


def filter_rows(df: Any, condition: ColumnLike) -> Any:
    """Filter rows using a SQL condition string or PySpark Column."""

    return df.filter(_as_column(condition))


def joiner(
    left_df: Any,
    right_df: Any,
    on: Union[str, Sequence[str], Mapping[str, str]],
    how: str = "inner",
    broadcast_right: bool = False,
) -> Any:
    """Join two DataFrames.

    ``on`` may be a column name, a list of same-named columns, or a mapping of
    left-column to right-column names.
    """

    supported_join_types = {
        "inner",
        "left",
        "left_outer",
        "right",
        "right_outer",
        "full",
        "full_outer",
        "outer",
        "left_semi",
        "left_anti",
        "cross",
    }
    if how not in supported_join_types:
        raise TransformerError(f"Unsupported join type: {how}")

    right = _functions().broadcast(right_df) if broadcast_right else right_df

    if isinstance(on, Mapping):
        if not on:
            raise TransformerError("Join column mapping cannot be empty.")
        _require_columns(left_df, on.keys())
        _require_columns(right_df, on.values())

        condition = None
        for left_column, right_column in on.items():
            next_condition = left_df[left_column] == right[right_column]
            condition = (
                next_condition if condition is None else condition & next_condition
            )
        return left_df.join(right, condition, how)

    columns = _normalize_columns(on)
    _require_columns(left_df, columns)
    _require_columns(right_df, columns)
    return left_df.join(right, columns, how)


def unioner(
    df: Any,
    others: Sequence[Any],
    allow_missing_columns: bool = False,
    distinct: bool = False,
) -> Any:
    """Union two or more DataFrames by column name.

    By default, all DataFrames must contain the same columns. Set
    ``allow_missing_columns=True`` to use Spark's schema-aligned union behavior
    for evolving schemas.
    """

    if not others:
        raise TransformerError("At least one DataFrame is required for union.")

    base_columns = set(df.columns)
    result = df
    for other in others:
        if allow_missing_columns:
            missing_in_other = []
        else:
            missing_in_other = sorted(base_columns - set(other.columns))
        if missing_in_other:
            raise TransformerError(
                "Union DataFrame is missing required column(s): "
                + ", ".join(missing_in_other)
            )

        try:
            result = result.unionByName(
                other,
                allowMissingColumns=allow_missing_columns,
            )
        except TypeError:
            if allow_missing_columns:
                raise TransformerError(
                    "Current Spark version does not support allowMissingColumns."
                )
            result = result.unionByName(other)

    return result.distinct() if distinct else result


def update_strategy(
    df: Any,
    rules: Sequence[UpdateStrategyRule],
    strategy_column: str = "_change_operation",
    default_operation: str = "I",
) -> Any:
    """Add a DML operation column from ordered update-strategy rules.

    Common operation values are ``I`` for insert, ``U`` for update, ``D`` for
    delete, and ``R`` for reject. Rules are evaluated in order, and the first
    matching condition wins.
    """

    if not rules:
        raise TransformerError("At least one update strategy rule is required.")
    if not strategy_column:
        raise TransformerError("Strategy column cannot be empty.")

    F = _functions()
    expression = None

    for rule in rules:
        if not rule.operation:
            raise TransformerError("Update strategy operation cannot be empty.")
        condition = _as_column(rule.condition)
        operation_value = F.lit(rule.operation)
        expression = (
            F.when(condition, operation_value)
            if expression is None
            else expression.when(condition, operation_value)
        )

    return df.withColumn(strategy_column, expression.otherwise(F.lit(default_operation)))


__all__ = [
    "RouteRule",
    "TransformerError",
    "UpdateStrategyRule",
    "aggregator",
    "filter_rows",
    "joiner",
    "lookup",
    "rank",
    "router",
    "sorter",
    "unioner",
    "update_strategy",
]

"""Common ETL loading patterns implemented with PySpark DataFrames."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


class PatternError(Exception):
    """Raised when a load pattern cannot be applied."""


def stage_load(df: Any, audit_columns: Mapping[str, str] | None = None) -> Any:
    """Apply lightweight staging conventions to a DataFrame."""

    if not audit_columns:
        return df

    from pyspark.sql import functions as F

    staged = df
    for column, expression in audit_columns.items():
        staged = staged.withColumn(column, F.expr(expression))
    return staged


def fact_load(df: Any, required_keys: Sequence[str]) -> Any:
    """Drop rows with null required fact keys."""

    missing = [column for column in required_keys if column not in df.columns]
    if missing:
        raise PatternError(f"Missing fact key column(s): {', '.join(missing)}")

    filtered = df
    for column in required_keys:
        filtered = filtered.filter(f"{column} is not null")
    return filtered


def scd_type2_merge_sql(
    target_table: str,
    source_view: str,
    business_keys: Sequence[str],
    compare_columns: Sequence[str],
    effective_from_column: str = "effective_from",
    effective_to_column: str = "effective_to",
    current_flag_column: str = "is_current",
) -> str:
    """Build portable Delta Lake SCD Type 2 merge SQL."""

    if not business_keys:
        raise PatternError("SCD Type 2 merge requires business keys.")
    if not compare_columns:
        raise PatternError("SCD Type 2 merge requires compare columns.")

    key_match = " AND ".join([f"t.{key} = s.{key}" for key in business_keys])
    change_match = " OR ".join(
        [f"NOT (t.{column} <=> s.{column})" for column in compare_columns]
    )
    insert_columns = list(dict.fromkeys([*business_keys, *compare_columns]))
    insert_target = ", ".join([*insert_columns, effective_from_column, effective_to_column, current_flag_column])
    insert_values = ", ".join(
        [*(f"s.{column}" for column in insert_columns), "current_timestamp()", "NULL", "true"]
    )

    return f"""
MERGE INTO {target_table} AS t
USING {source_view} AS s
ON {key_match} AND t.{current_flag_column} = true
WHEN MATCHED AND ({change_match}) THEN
  UPDATE SET
    t.{effective_to_column} = current_timestamp(),
    t.{current_flag_column} = false
WHEN NOT MATCHED THEN
  INSERT ({insert_target})
  VALUES ({insert_values})
""".strip()


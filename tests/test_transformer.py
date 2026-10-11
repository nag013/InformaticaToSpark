import sys
import unittest
from types import SimpleNamespace
from unittest import mock

from etl_framework import transformer


class FakeExpression:
    def __init__(self, text):
        self.text = text

    def __or__(self, other):
        return FakeExpression(f"({self.text} OR {other.text})")

    def __and__(self, other):
        return FakeExpression(f"({self.text} AND {other.text})")

    def __invert__(self):
        return FakeExpression(f"NOT ({self.text})")

    def __eq__(self, other):
        return FakeExpression(f"{self.text} = {other.text}")

    def asc(self):
        return FakeExpression(f"{self.text} ASC")

    def desc(self):
        return FakeExpression(f"{self.text} DESC")

    def alias(self, alias):
        return FakeExpression(f"{self.text} AS {alias}")

    def over(self, window):
        return FakeExpression(f"{self.text} OVER {window.text}")

    def when(self, condition, value):
        return FakeExpression(f"{self.text} WHEN {condition.text} THEN {value.text}")

    def otherwise(self, value):
        return FakeExpression(f"{self.text} ELSE {value.text}")


class FakeGroupedData:
    def __init__(self, df, group_columns):
        self.df = df
        self.group_columns = group_columns

    def agg(self, *expressions):
        self.df.operation = ("agg", self.group_columns, expressions)
        return self.df


class FakeDataFrame:
    def __init__(self, columns, name="df"):
        self.columns = columns
        self.name = name
        self.operation = None

    def __getitem__(self, column):
        return FakeExpression(f"{self.name}.{column}")

    def filter(self, condition):
        result = FakeDataFrame(self.columns, self.name)
        result.operation = ("filter", condition.text)
        return result

    def orderBy(self, *expressions):
        self.operation = ("orderBy", tuple(expression.text for expression in expressions))
        return self

    def groupBy(self, *columns):
        return FakeGroupedData(self, columns)

    def join(self, right, on, how):
        self.operation = ("join", right.name, on, how)
        return self

    def withColumn(self, column, expression):
        self.operation = ("withColumn", column, expression.text)
        return self

    def unionByName(self, other, allowMissingColumns=False):
        self.operation = ("unionByName", other.name, allowMissingColumns)
        return self

    def distinct(self):
        self.operation = ("distinct",)
        return self


class FakeWindow:
    def __init__(self, partitions=()):
        self.partitions = partitions
        self.text = ""

    @classmethod
    def partitionBy(cls, *columns):
        return cls(columns)

    def orderBy(self, *expressions):
        order_text = ",".join(expression.text for expression in expressions)
        self.text = f"PARTITION {self.partitions} ORDER {order_text}"
        return self


class TransformerTests(unittest.TestCase):
    def setUp(self):
        fake_functions = SimpleNamespace(
            broadcast=lambda df: df,
            col=lambda name: FakeExpression(f"col({name})"),
            dense_rank=lambda: FakeExpression("dense_rank()"),
            expr=lambda expression: FakeExpression(f"expr({expression})"),
            lit=lambda value: FakeExpression(f"lit({value})"),
            rank=lambda: FakeExpression("rank()"),
            row_number=lambda: FakeExpression("row_number()"),
            when=lambda condition, value: FakeExpression(
                f"WHEN {condition.text} THEN {value.text}"
            ),
        )
        self.module_patch = mock.patch.dict(
            sys.modules,
            {
                "pyspark.sql.functions": fake_functions,
                "pyspark.sql.window": SimpleNamespace(Window=FakeWindow),
            },
        )
        self.module_patch.start()

    def tearDown(self):
        self.module_patch.stop()

    def test_rank_adds_rank_column(self):
        df = FakeDataFrame(["customer_id", "amount"])

        result = transformer.rank(
            df,
            partition_by="customer_id",
            order_by="-amount",
            method="dense_rank",
            rank_column="amount_rank",
        )

        self.assertIs(result, df)
        self.assertEqual(
            df.operation,
            (
                "withColumn",
                "amount_rank",
                "dense_rank() OVER PARTITION ('customer_id',) ORDER col(amount) DESC",
            ),
        )

    def test_router_creates_named_outputs_and_default(self):
        df = FakeDataFrame(["status"])
        routes = transformer.router(
            df,
            [
                transformer.RouteRule("active", FakeExpression("status = 'A'")),
                transformer.RouteRule("inactive", FakeExpression("status = 'I'")),
            ],
        )

        self.assertEqual(routes["active"].operation, ("filter", "status = 'A'"))
        self.assertEqual(routes["inactive"].operation, ("filter", "status = 'I'"))
        self.assertEqual(
            routes["default"].operation,
            ("filter", "NOT ((status = 'A' OR status = 'I'))"),
        )

    def test_sorter_orders_columns(self):
        df = FakeDataFrame(["name", "amount"])

        transformer.sorter(df, ["name", "-amount"])

        self.assertEqual(
            df.operation,
            ("orderBy", ("col(name) ASC", "col(amount) DESC")),
        )

    def test_aggregator_groups_and_aggregates(self):
        df = FakeDataFrame(["customer_id", "amount"])

        transformer.aggregator(
            df,
            group_by="customer_id",
            aggregations={"total_amount": "sum(amount)"},
        )

        self.assertEqual(df.operation[0], "agg")
        self.assertEqual(df.operation[1], ("customer_id",))
        self.assertEqual(df.operation[2][0].text, "expr(sum(amount)) AS total_amount")

    def test_lookup_defaults_to_left_join(self):
        df = FakeDataFrame(["id", "amount"], name="left")
        lookup_df = FakeDataFrame(["id", "name"], name="right")

        transformer.lookup(df, lookup_df, on="id")

        self.assertEqual(df.operation, ("join", "right", ["id"], "left"))

    def test_filter_rows_accepts_sql_expression(self):
        df = FakeDataFrame(["amount"])

        result = transformer.filter_rows(df, "amount > 0")

        self.assertEqual(result.operation, ("filter", "expr(amount > 0)"))

    def test_joiner_supports_different_column_names(self):
        left = FakeDataFrame(["customer_id"], name="left")
        right = FakeDataFrame(["id"], name="right")

        transformer.joiner(left, right, on={"customer_id": "id"}, how="inner")

        self.assertEqual(left.operation[0], "join")
        self.assertEqual(left.operation[1], "right")
        self.assertEqual(left.operation[2].text, "left.customer_id = right.id")
        self.assertEqual(left.operation[3], "inner")

    def test_missing_columns_raise_transformer_error(self):
        df = FakeDataFrame(["id"])

        with self.assertRaisesRegex(transformer.TransformerError, "Missing"):
            transformer.sorter(df, "missing")

    def test_unioner_unions_dataframes_by_name(self):
        left = FakeDataFrame(["id", "amount"], name="left")
        right = FakeDataFrame(["amount", "id"], name="right")

        result = transformer.unioner(left, [right])

        self.assertIs(result, left)
        self.assertEqual(left.operation, ("unionByName", "right", False))

    def test_unioner_allows_missing_columns_when_requested(self):
        left = FakeDataFrame(["id", "amount"], name="left")
        right = FakeDataFrame(["id"], name="right")

        transformer.unioner(left, [right], allow_missing_columns=True)

        self.assertEqual(left.operation, ("unionByName", "right", True))

    def test_unioner_rejects_missing_columns_by_default(self):
        left = FakeDataFrame(["id", "amount"], name="left")
        right = FakeDataFrame(["id"], name="right")

        with self.assertRaisesRegex(transformer.TransformerError, "missing required"):
            transformer.unioner(left, [right])

    def test_unioner_can_return_distinct_rows(self):
        left = FakeDataFrame(["id"], name="left")
        right = FakeDataFrame(["id"], name="right")

        transformer.unioner(left, [right], distinct=True)

        self.assertEqual(left.operation, ("distinct",))

    def test_update_strategy_adds_operation_column(self):
        df = FakeDataFrame(["is_deleted", "is_changed"], name="orders")

        transformer.update_strategy(
            df,
            [
                transformer.UpdateStrategyRule("D", "is_deleted = true"),
                transformer.UpdateStrategyRule("U", "is_changed = true"),
            ],
            strategy_column="dml_type",
            default_operation="I",
        )

        self.assertEqual(
            df.operation,
            (
                "withColumn",
                "dml_type",
                "WHEN expr(is_deleted = true) THEN lit(D) "
                "WHEN expr(is_changed = true) THEN lit(U) ELSE lit(I)",
            ),
        )

    def test_update_strategy_requires_rules(self):
        df = FakeDataFrame(["id"], name="orders")

        with self.assertRaisesRegex(transformer.TransformerError, "At least one"):
            transformer.update_strategy(df, [])


if __name__ == "__main__":
    unittest.main()

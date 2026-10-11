import sys
import unittest
from types import SimpleNamespace
from unittest import mock

from etl_framework import readers


class FakeCatalog:
    def __init__(self, existing_tables):
        self.existing_tables = set(existing_tables)
        self.checked = []

    def tableExists(self, table_name):
        self.checked.append(table_name)
        return table_name in self.existing_tables


class FakeDataFrame:
    def __init__(self, columns):
        self.columns = columns
        self.filter_expression = None

    def filter(self, expression):
        self.filter_expression = expression
        return self


class FakeReader:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.loaded_path = None

    def format(self, format_name):
        self.format_name = format_name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self, path):
        self.loaded_path = path
        return FakeDataFrame(["id", "updated_at"])


class FakeSpark:
    def __init__(self, existing_tables=None):
        self.catalog = FakeCatalog(existing_tables or [])
        self.read = FakeReader()
        self.table_calls = []
        self.df = FakeDataFrame(["id", "updated_at"])

    def table(self, table_name):
        self.table_calls.append(table_name)
        return self.df


class ReaderTests(unittest.TestCase):
    def test_build_fqn_supports_table_schema_and_catalog(self):
        self.assertEqual(readers.build_fqn("orders"), "orders")
        self.assertEqual(readers.build_fqn("orders", schema="sales"), "sales.orders")
        self.assertEqual(
            readers.build_fqn("orders", schema="sales", catalog="lakehouse"),
            "lakehouse.sales.orders",
        )

    def test_build_fqn_rejects_unsafe_identifier(self):
        with self.assertRaisesRegex(readers.ReaderError, "Invalid Spark identifier"):
            readers.build_fqn("orders;drop")

    def test_read_table_validates_and_reads_existing_table(self):
        spark = FakeSpark(existing_tables=["lakehouse.sales.orders"])

        df = readers.read_table(
            "orders",
            schema="sales",
            catalog="lakehouse",
            spark=spark,
        )

        self.assertIs(df, spark.df)
        self.assertEqual(spark.catalog.checked, ["lakehouse.sales.orders"])
        self.assertEqual(spark.table_calls, ["lakehouse.sales.orders"])

    def test_read_table_raises_when_missing(self):
        spark = FakeSpark(existing_tables=[])

        with self.assertRaisesRegex(readers.ReaderError, "does not exist"):
            readers.read_table("orders", schema="sales", spark=spark)

    def test_read_path_uses_format_options_and_path(self):
        spark = FakeSpark()

        readers.read_path(
            "s3://bucket/orders",
            fmt="parquet",
            options={"mergeSchema": "true"},
            spark=spark,
        )

        self.assertEqual(spark.read.format_name, "parquet")
        self.assertEqual(spark.read.options, {"mergeSchema": "true"})
        self.assertEqual(spark.read.loaded_path, "s3://bucket/orders")

    def test_read_target_requires_exactly_one_target(self):
        with self.assertRaisesRegex(readers.ReaderError, "exactly one"):
            readers.ReadTarget(table="orders", path="s3://bucket/orders")

        with self.assertRaisesRegex(readers.ReaderError, "exactly one"):
            readers.ReadTarget()

    def test_incremental_full_load_returns_unfiltered_dataframe(self):
        spark = FakeSpark(existing_tables=["orders"])

        df = readers.read_table_incremental(
            "orders",
            increment_column="updated_at",
            end_value=None,
            spark=spark,
        )

        self.assertIs(df, spark.df)
        self.assertIsNone(spark.df.filter_expression)

    def test_incremental_rejects_missing_increment_column(self):
        spark = FakeSpark(existing_tables=["orders"])

        with self.assertRaisesRegex(readers.ReaderError, "Increment column"):
            readers.read_table_incremental(
                "orders",
                increment_column="missing_column",
                end_value="2026-01-01T00:00:00Z",
                spark=spark,
            )

    def test_incremental_applies_filter(self):
        spark = FakeSpark(existing_tables=["orders"])

        class FakeColumn:
            def __init__(self, text):
                self.text = text

            def __gt__(self, other):
                return f"{self.text} > {other.text}"

        fake_functions = SimpleNamespace(
            col=lambda name: FakeColumn(f"col({name})"),
            lit=lambda value: FakeColumn(f"lit({value})"),
        )

        with mock.patch.dict(sys.modules, {"pyspark.sql.functions": fake_functions}):
            df = readers.read_table_incremental(
                "orders",
                increment_column="updated_at",
                end_value="2026-01-01T00:00:00Z",
                spark=spark,
            )

        self.assertEqual(
            df.filter_expression,
            "col(updated_at) > lit(2026-01-01T00:00:00Z)",
        )


if __name__ == "__main__":
    unittest.main()

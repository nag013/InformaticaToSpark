import unittest

from etl_framework.patterns import fact_load, scd_type2_merge_sql


class FakeDataFrame:
    def __init__(self, columns):
        self.columns = columns
        self.filters = []

    def filter(self, condition):
        self.filters.append(condition)
        return self


class PatternTests(unittest.TestCase):
    def test_fact_load_filters_null_keys(self):
        df = FakeDataFrame(["order_id", "customer_id"])

        result = fact_load(df, ["order_id", "customer_id"])

        self.assertIs(result, df)
        self.assertEqual(
            df.filters,
            ["order_id is not null", "customer_id is not null"],
        )

    def test_scd_type2_merge_sql_contains_merge_parts(self):
        sql = scd_type2_merge_sql(
            target_table="dim_customer",
            source_view="updates",
            business_keys=["customer_id"],
            compare_columns=["name", "email"],
        )

        self.assertIn("MERGE INTO dim_customer AS t", sql)
        self.assertIn("USING updates AS s", sql)
        self.assertIn("t.customer_id = s.customer_id", sql)
        self.assertIn("NOT (t.name <=> s.name)", sql)


if __name__ == "__main__":
    unittest.main()


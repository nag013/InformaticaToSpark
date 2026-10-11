import unittest

from etl_framework.audit import build_event
from etl_framework.watermark import max_watermark


class FakeRow(dict):
    pass


class FakeDataFrame:
    columns = ["updated_at"]

    def selectExpr(self, expression):
        self.expression = expression
        return self

    def first(self):
        return FakeRow(watermark="2026-05-03T00:00:00Z")


class AuditWatermarkTests(unittest.TestCase):
    def test_build_event_populates_fields(self):
        event = build_event("orders", "pipeline", "STARTED", "run-1")

        self.assertEqual(event.pipeline_name, "orders")
        self.assertEqual(event.status, "STARTED")
        self.assertEqual(event.run_id, "run-1")
        self.assertTrue(event.event_ts)

    def test_max_watermark_returns_string_value(self):
        df = FakeDataFrame()

        value = max_watermark(df, "updated_at")

        self.assertEqual(value, "2026-05-03T00:00:00Z")
        self.assertEqual(df.expression, "max(updated_at) as watermark")


if __name__ == "__main__":
    unittest.main()


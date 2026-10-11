import unittest

from etl_framework.config import ConfigError, pipeline_from_mapping


class ConfigTests(unittest.TestCase):
    def test_pipeline_from_mapping_builds_valid_config(self):
        config = pipeline_from_mapping(
            {
                "name": "orders",
                "source": {"path": "s3://raw/orders", "fmt": "parquet"},
                "target": {"table": "orders"},
                "load_type": "append",
                "watermark_column": "updated_at",
            }
        )

        self.assertEqual(config.name, "orders")
        self.assertEqual(config.source.path, "s3://raw/orders")
        self.assertEqual(config.target.table, "orders")

    def test_upsert_requires_primary_keys(self):
        with self.assertRaisesRegex(ConfigError, "requires primary_keys"):
            pipeline_from_mapping(
                {
                    "name": "orders",
                    "source": {"table": "source_orders"},
                    "target": {"table": "target_orders"},
                    "load_type": "upsert",
                }
            )


if __name__ == "__main__":
    unittest.main()


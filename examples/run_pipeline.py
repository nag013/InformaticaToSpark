"""Run a configured ETL pipeline with Spark.

Usage:
    spark-submit examples/run_pipeline.py configs/sample_pipeline.json
"""

from __future__ import annotations

import logging
import sys

from etl_framework.config import load_pipeline_config
from etl_framework.orchestrator import run_pipeline
from etl_framework.readers import get_spark_session


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO)
    if len(argv) != 2:
        print("Usage: spark-submit examples/run_pipeline.py <config.json>")
        return 2

    spark = get_spark_session("CloudAgnosticETL")
    config = load_pipeline_config(argv[1])
    run_id = run_pipeline(spark, config)
    print(f"Pipeline completed. run_id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


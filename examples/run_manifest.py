"""Run staging, dimension, and fact pipelines from a manifest.

Usage:
    spark-submit examples/run_manifest.py configs/table_manifest.json
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Any

from etl_framework.config import pipeline_from_mapping
from etl_framework.orchestrator import run_pipeline
from etl_framework.readers import get_spark_session


PIPELINE_GROUPS = ("staging", "dimensions", "facts")


def _load_manifest(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO)
    if len(argv) != 2:
        print("Usage: spark-submit examples/run_manifest.py <table_manifest.json>")
        return 2

    spark = get_spark_session("CloudAgnosticETLManifest")
    manifest = _load_manifest(argv[1])

    for group in PIPELINE_GROUPS:
        for raw_pipeline in manifest.get(group, []):
            config = pipeline_from_mapping(raw_pipeline)
            run_id = run_pipeline(spark, config)
            print(f"{group}.{config.name} completed. run_id={run_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


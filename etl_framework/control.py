"""Control-table domain model and repository helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


class ControlTableError(Exception):
    """Raised when control-table operations fail."""


@dataclass(frozen=True)
class ControlRecord:
    """Control metadata for one pipeline task."""

    pipeline_name: str
    source_name: str
    target_name: str
    load_type: str
    enabled: bool = True
    watermark_column: Optional[str] = None
    last_watermark: Optional[str] = None


class ControlTableRepository:
    """Small Spark SQL repository for control-table records."""

    def __init__(self, spark: Any, table_name: str) -> None:
        self.spark = spark
        self.table_name = table_name

    def enabled_records(self) -> list[ControlRecord]:
        rows = (
            self.spark.table(self.table_name)
            .filter("enabled = true")
            .collect()
        )
        return [
            ControlRecord(
                pipeline_name=row["pipeline_name"],
                source_name=row["source_name"],
                target_name=row["target_name"],
                load_type=row["load_type"],
                enabled=row["enabled"],
                watermark_column=row["watermark_column"],
                last_watermark=row["last_watermark"],
            )
            for row in rows
        ]

    def update_watermark(self, pipeline_name: str, watermark: str) -> None:
        escaped_pipeline = pipeline_name.replace("'", "''")
        escaped_watermark = watermark.replace("'", "''")
        self.spark.sql(
            f"""
            UPDATE {self.table_name}
            SET last_watermark = '{escaped_watermark}',
                updated_at = current_timestamp()
            WHERE pipeline_name = '{escaped_pipeline}'
            """
        )


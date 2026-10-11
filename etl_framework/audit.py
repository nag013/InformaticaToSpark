"""Audit event helpers for Spark ETL jobs."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from uuid import uuid4

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditEvent:
    """Single auditable ETL lifecycle event."""

    run_id: str
    pipeline_name: str
    task_name: str
    status: str
    event_ts: str
    message: Optional[str] = None
    metrics: Optional[Mapping[str, Any]] = None


def new_run_id() -> str:
    """Create an opaque run identifier."""

    return str(uuid4())


def utc_now_iso() -> str:
    """Return a timezone-aware UTC timestamp string."""

    return datetime.now(timezone.utc).isoformat()


def build_event(
    pipeline_name: str,
    task_name: str,
    status: str,
    run_id: str,
    message: Optional[str] = None,
    metrics: Optional[Mapping[str, Any]] = None,
) -> AuditEvent:
    """Build a normalized audit event."""

    return AuditEvent(
        run_id=run_id,
        pipeline_name=pipeline_name,
        task_name=task_name,
        status=status,
        event_ts=utc_now_iso(),
        message=message,
        metrics=metrics,
    )


class AuditLogger:
    """Audit sink that can log to Python logging and optionally to a Spark table."""

    def __init__(self, spark: Optional[Any] = None, table: Optional[str] = None) -> None:
        self.spark = spark
        self.table = table

    def emit(self, event: AuditEvent) -> None:
        payload = asdict(event)
        LOGGER.info("audit_event=%s", payload)
        if not self.spark or not self.table:
            return

        df = self.spark.createDataFrame([payload])
        df.write.mode("append").format("delta").saveAsTable(self.table)


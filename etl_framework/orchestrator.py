"""Config-driven ETL orchestration."""

from __future__ import annotations

import logging
from typing import Any

from etl_framework.audit import AuditLogger, build_event, new_run_id
from etl_framework.config import PipelineConfig
from etl_framework.patterns import fact_load, stage_load
from etl_framework.readers import ReadTarget, read_data, read_incremental
from etl_framework.watermark import max_watermark
from etl_framework.writers import WriteTarget, write_data

LOGGER = logging.getLogger(__name__)


class OrchestrationError(Exception):
    """Raised when a pipeline orchestration fails."""


def _read_target_from_config(config: PipelineConfig) -> ReadTarget:
    return ReadTarget(
        table=config.source.table,
        path=config.source.path,
        schema=config.source.schema,
        catalog=config.source.catalog,
        fmt=config.source.fmt,
        options=config.source.options,
    )


def _write_target_from_config(config: PipelineConfig) -> WriteTarget:
    mode = "append" if config.load_type in {"append", "upsert", "scd2"} else config.load_type
    return WriteTarget(
        table=config.target.table,
        path=config.target.path,
        fmt=config.target.fmt,
        mode=mode,
        options=config.target.options,
    )


def run_pipeline(
    spark: Any,
    config: PipelineConfig,
    audit_logger: AuditLogger | None = None,
) -> str:
    """Run a configured pipeline and return the run id."""

    run_id = new_run_id()
    audit = audit_logger or AuditLogger()
    audit.emit(build_event(config.name, "pipeline", "STARTED", run_id))

    try:
        source = _read_target_from_config(config)
        if config.watermark_column:
            df = read_incremental(
                source,
                increment_column=config.watermark_column,
                end_value=config.options.get("last_watermark"),
                spark=spark,
            )
        else:
            df = read_data(source, spark=spark)

        df = stage_load(df, config.options.get("audit_columns"))
        if config.load_type in {"upsert", "scd2"}:
            df = fact_load(df, config.primary_keys)

        write_data(df, _write_target_from_config(config))

        metrics = {}
        if config.watermark_column:
            metrics["max_watermark"] = max_watermark(df, config.watermark_column)

        audit.emit(build_event(config.name, "pipeline", "SUCCEEDED", run_id, metrics=metrics))
        return run_id
    except Exception as exc:
        LOGGER.exception("Pipeline failed: %s", config.name)
        audit.emit(build_event(config.name, "pipeline", "FAILED", run_id, message=str(exc)))
        raise OrchestrationError(f"Pipeline failed: {config.name}") from exc

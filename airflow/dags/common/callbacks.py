"""Shared callbacks. Connect these to the approved notification service."""

from __future__ import annotations

import logging
from typing import Any


LOGGER = logging.getLogger(__name__)


def log_task_failure(context: dict[str, Any]) -> None:
    """Log useful failure context without logging credentials."""
    task_instance = context.get("task_instance")
    LOGGER.error(
        "Airflow task failed: dag_id=%s task_id=%s run_id=%s",
        getattr(task_instance, "dag_id", "unknown"),
        getattr(task_instance, "task_id", "unknown"),
        context.get("run_id", "unknown"),
    )


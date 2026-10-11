"""Daily orchestration for the Varanasi dbt project."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.callbacks import log_task_failure
from common.dbt_task_factory import create_dbt_task


REPO_ROOT = os.environ.get("DBT_REPO_ROOT", "/opt/airflow/repo")

with DAG(
    dag_id="varanasi_daily_pipeline",
    description="Run Varanasi source validation, history, and daily curations.",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "resource_y",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": log_task_failure,
    },
    tags=["varanasi", "dbt", "curation", "daily"],
) as dag:
    ingestion_complete = EmptyOperator(task_id="ingestion_complete")

    source_freshness = create_dbt_task(
        task_id="source_freshness",
        repo_root=REPO_ROOT,
        project_name="varanasi",
        command="source freshness",
    )
    snapshot = create_dbt_task(
        task_id="snapshot",
        repo_root=REPO_ROOT,
        project_name="varanasi",
        command="snapshot",
    )
    daily_curation = create_dbt_task(
        task_id="daily_curation",
        repo_root=REPO_ROOT,
        project_name="varanasi",
        command="build",
        selector="daily_curation",
    )

    ingestion_complete >> source_freshness >> snapshot >> daily_curation


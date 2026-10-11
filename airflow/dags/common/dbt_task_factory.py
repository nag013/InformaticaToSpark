"""Shared Airflow task construction for repository dbt projects."""

from __future__ import annotations

from pathlib import PurePosixPath

from airflow.operators.bash import BashOperator


def create_dbt_task(
    *,
    task_id: str,
    repo_root: str,
    project_name: str,
    command: str,
    selector: str | None = None,
) -> BashOperator:
    """Create a dbt CLI task without embedding credentials in the DAG."""
    project_dir = PurePosixPath(repo_root) / "projects" / project_name
    selection = f" --selector {selector}" if selector else ""

    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"dbt {command}"
            f" --project-dir {project_dir}"
            f" --profiles-dir $DBT_PROFILES_DIR"
            f"{selection}"
        ),
        env={
            "DBT_PROFILES_DIR": "{{ var.value.get('dbt_profiles_dir', '/opt/airflow/dbt') }}",
        },
        append_env=True,
    )


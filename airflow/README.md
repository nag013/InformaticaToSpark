# Airflow orchestration

The repository contains independent daily DAGs for RRR and Varanasi. Shared task construction and callbacks live under `airflow/dags/common`; project schedules and ownership remain in project-specific DAG files.

## Runtime layout

The examples assume the repository is available to workers at `/opt/airflow/repo`. Override this with `DBT_REPO_ROOT`. Configure the Airflow variable `dbt_profiles_dir` to the directory containing the secure `profiles.yml`; it defaults to `/opt/airflow/dbt`.

Credentials must be provided by the Airflow secret backend, environment, or platform integration. Do not place them in DAG source code.

Airflow normally adds its DAG directory to `PYTHONPATH`, allowing the DAG-local `common` imports to resolve. Deploy the complete `airflow/dags` directory, including `common`.

## DAGs

- `rrr_daily_pipeline`: resource X; scheduled at 02:00 UTC.
- `varanasi_daily_pipeline`: resource Y; scheduled at 03:00 UTC.

Change the schedules to the approved business time zone and SLA before production. Both DAGs use `max_active_runs=1` to prevent overlapping project runs.

The `ingestion_complete` task is a placeholder. Replace it with the applicable ingestion task, dataset dependency, or external-task sensor.

## Execution sequence

```text
ingestion_complete
  -> source_freshness
  -> snapshot
  -> daily_curation
```

The daily curation uses each project's `selectors.yml`. Models tagged `daily` are selected with their upstream dependencies.

## Installation

Airflow should be installed using the official constraints file matching the chosen Airflow and Python versions. Avoid an unconstrained production installation from `requirements.txt`.


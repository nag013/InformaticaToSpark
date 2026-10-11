# Informatica ETL Transition: Spark Ingestion + dbt Transformations

This repository is a migration starter for moving Informatica workloads to a modern ELT design. The existing cloud-agnostic Spark framework handles ingestion and non-SQL data movement. Two independent dbt projects handle warehouse transformations, tests, documentation, lineage, incremental loads, and snapshots.

Start with the step-by-step [Informatica to dbt migration guide](docs/INFORMATICA_TO_DBT_MIGRATION.md).

## dbt project layout

- `projects/rrr/`: independently runnable RRR dbt project, owned by resource X.
- `projects/varanasi/`: independently runnable Varanasi dbt project, owned by resource Y.
- Each project contains its own `dbt_project.yml`, models, sources, snapshots, tests, macros, and migration inventory.
- `airflow/dags/rrr/`: RRR project orchestration.
- `airflow/dags/varanasi/`: Varanasi project orchestration.
- `airflow/dags/common/`: shared task factories and callbacks.
- `profiles.yml.example`: credential-free Databricks profile template.

Quick start after configuring credentials:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dbt.txt
dbt debug --project-dir projects/rrr
dbt build --project-dir projects/rrr

dbt debug --project-dir projects/varanasi
dbt build --project-dir projects/varanasi
```

The projects are isolated: X can work in `projects/rrr` while Y works in `projects/varanasi`. Changes to root files, connection profiles, CI, or shared ingestion code must be coordinated.

Airflow setup and deployment assumptions are documented in [airflow/README.md](airflow/README.md). The DAGs use project-specific dbt selectors and tags:

```powershell
dbt build --project-dir projects/rrr --selector daily_curation
dbt build --project-dir projects/varanasi --selector daily_curation
```

### Recommended branch ownership

```text
feature/rrr-varanasi-base
├── feature/rrr-x       -> projects/rrr/**
└── feature/varanasi-y  -> projects/varanasi/**
```

## Existing Spark ingestion framework

Production-style starter repo for Spark ETL pipelines that can run on Databricks, EMR, Glue, Synapse, HDInsight, Kubernetes Spark, or local Spark.

## What Is Included

- `etl_framework/readers.py`: table and path readers with incremental watermark support.
- `etl_framework/writers.py`: table and path writers.
- `etl_framework/transformer.py`: reusable transformations for rank, router, sorter, aggregator, lookup, filter, and joiner.
- `etl_framework/control.py`: control-table model and Spark SQL repository.
- `etl_framework/audit.py`: audit event model and audit sink.
- `etl_framework/watermark.py`: max-watermark helper.
- `etl_framework/patterns.py`: stage, fact, and SCD Type 2 patterns.
- `etl_framework/orchestrator.py`: config-driven pipeline runner.
- `configs/sample_pipeline.json`: example pipeline config.
- `ddl/control_tables.sql`: sample control and audit table DDL.
- `examples/run_pipeline.py`: `spark-submit` entrypoint.
- `tests/`: unit tests that do not require a Spark cluster.

## Install

```bash
pip install -e .
```

For a real Spark runtime, install dependencies from `pyproject.toml` or use your managed Spark platform's built-in PySpark runtime.

## Run Tests

```bash
python -m unittest discover -s tests
```

## Run A Pipeline

```bash
spark-submit examples/run_pipeline.py configs/sample_pipeline.json
```

## Run All Staging, Dimension, And Fact Tables

Put your table list in `configs/table_manifest.json`.

The manifest has three sections:

- `staging`: raw to stage tables
- `dimensions`: dimension tables, including SCD-style loads
- `facts`: fact tables

Run everything in order:

```bash
spark-submit examples/run_manifest.py configs/table_manifest.json
```

Inside Docker:

```bash
docker build -t cloud-etl .
docker run --rm cloud-etl
```

For real Spark execution in a container, use a Spark image or mount this repo
into your Spark runtime and call:

```bash
spark-submit /app/examples/run_manifest.py /app/configs/table_manifest.json
```

The sample config uses cloud paths, but the same code works with tables:

```json
{
  "name": "orders_table_to_table",
  "source": {
    "catalog": "main",
    "schema": "bronze",
    "table": "orders"
  },
  "target": {
    "catalog": "main",
    "schema": "silver",
    "table": "orders"
  },
  "load_type": "append"
}
```

## Transformer Example

```python
from etl_framework.transformer import RouteRule, aggregator, filter_rows, joiner, rank, router

orders = spark.table("bronze.orders")
customers = spark.table("silver.customers")

valid_orders = filter_rows(orders, "amount > 0")
ranked = rank(valid_orders, partition_by="customer_id", order_by="-amount")

summary = aggregator(
    ranked,
    group_by="customer_id",
    aggregations={
        "total_amount": "sum(amount)",
        "order_count": "count(*)"
    },
)

enriched = joiner(summary, customers, on="customer_id", how="left")

routes = router(
    enriched,
    [
        RouteRule("high_value", "total_amount >= 1000"),
        RouteRule("standard", "total_amount < 1000")
    ],
)
```

## Cloud Agnostic Design

The framework does not hard-code a cloud provider. Use whichever Spark-supported storage URI your platform provides:

- AWS: `s3://bucket/path`
- Azure: `abfs://container@account.dfs.core.windows.net/path`
- GCP: `gs://bucket/path`
- HDFS: `hdfs://namenode/path`
- Local/dev: `file:///tmp/path`

Table reads and writes use Spark catalogs/metastores when available.

## Notes

- Avoiding eager actions like `count()` keeps pipeline planning cheap.
- Unit tests use fakes so they run without Spark installed.
- SCD Type 2 helper currently emits merge SQL; run it on a Delta-compatible Spark runtime.

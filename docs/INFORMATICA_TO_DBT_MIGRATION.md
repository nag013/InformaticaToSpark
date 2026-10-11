# Informatica to dbt migration guide

## What changes

Informatica commonly performs extraction, transformation, orchestration, and loading in one workflow. dbt owns transformations inside the warehouse. Keep file/API extraction and raw ingestion in an ingestion tool or the existing Spark framework, then let dbt transform landed raw tables.

| Informatica object | dbt equivalent |
|---|---|
| Folder / repository | Git repository and dbt project |
| Workflow / worklet | Orchestrator job and dbt selectors |
| Mapping | One or more SQL models connected with `ref()` |
| Source Qualifier | `source()` plus a staging model |
| Expression | SQL expressions in staging/intermediate models |
| Filter / Router | `where`, `case`, or separate downstream models |
| Lookup | SQL join to a `ref()` or `source()` |
| Joiner | SQL join |
| Aggregator | `group by` model |
| Sorter / Rank | `order by` / window functions |
| Sequence Generator | Warehouse identity or deterministic surrogate-key macro |
| Update Strategy | Incremental model with merge/delete+insert strategy |
| SCD Type 2 | dbt snapshot or an incremental dimension model |
| Parameter file | Environment variables and dbt `vars` |
| Session log / row checks | dbt artifacts, tests, and orchestrator logs |

## Migration steps

### 1. Inventory and freeze the current behavior

Export Informatica workflow and mapping XML, parameter files, schedules, SQL overrides, pre/post-SQL, lookup caches, reusable transformations, and target DDL. Record every item in `seeds/informatica_inventory.csv`, including its owner, SLA, row volumes, restart behavior, and downstream consumers. Capture representative source/target row counts and business totals before changing anything.

### 2. Classify responsibilities

For each workflow, separate:

- Extraction and file/API handling: keep in the ingestion/Spark layer.
- Raw loading: land immutable or replayable raw tables.
- Set-based transformation: move to dbt.
- Scheduling, retries, and alerts: configure in the orchestrator.
- Data quality: express as dbt tests, reconciliations, and freshness checks.

### 3. Configure the target platform

Copy `profiles.yml.example` to `~/.dbt/profiles.yml` (or set `DBT_PROFILES_DIR` to a secure external directory). Both `projects/rrr` and `projects/varanasi` use this profile. Select the correct dbt adapter for the warehouse. The included example is Databricks; for Snowflake, BigQuery, Redshift, or Fabric, replace the adapter and profile fields. Never commit tokens or passwords.

Required environment variables for the example are `DBT_HOST`, `DBT_HTTP_PATH`, `DBT_TOKEN`, `DBT_CATALOG`, and `DBT_SCHEMA`. Raw locations can be overridden with `DBT_RAW_CATALOG` and `DBT_RAW_SCHEMA`.

### 4. Translate one vertical slice

Start with one medium-complexity mapping. Build it in this order:

1. Declare landed tables in `models/sources.yml`.
2. Put renaming, casting, trimming, and source filters in `models/staging/`.
3. Put reusable joins and business transformations in `models/intermediate/`.
4. Put business-facing facts, dimensions, and aggregates in `models/marts/`.
5. Add schema tests beside each model and singular reconciliation tests for business rules.
6. Use incremental materialization only after the full-refresh result matches Informatica.

### 5. Reconcile in parallel

Run Informatica and dbt for the same closed input window. Compare row counts, key uniqueness, null rates, sums, minimum/maximum dates, rejected rows, and a deterministic row hash. Investigate differences in null handling, string padding, decimal rounding, time zones, lookup defaults, update ordering, and duplicate source keys.

Do not cut over based only on row counts. Obtain business-owner approval for material totals and sampled records. Retain the comparison evidence.

### 6. Operationalize

Run `dbt build` in CI for pull requests against an isolated development schema. In production, run ingestion first, then `dbt source freshness` (after freshness rules are configured), `dbt snapshot`, and `dbt build`. Store `manifest.json`, `run_results.json`, and logs as deployment artifacts. Configure retries and notifications in the orchestrator.

### 7. Cut over safely

Pause changes to the Informatica mapping, execute a final parallel run, reconcile, redirect consumers, and monitor at least one normal business cycle. Keep a time-boxed rollback path. After approval, disable the Informatica schedule; archive exports and runbooks rather than deleting them immediately.

## Local commands

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dbt.txt
Copy-Item profiles.yml.example "$HOME\.dbt\profiles.yml"
dbt debug --project-dir projects/rrr
dbt seed --project-dir projects/rrr
dbt snapshot --project-dir projects/rrr
dbt build --project-dir projects/rrr

dbt debug --project-dir projects/varanasi
dbt seed --project-dir projects/varanasi
dbt snapshot --project-dir projects/varanasi
dbt build --project-dir projects/varanasi
```

`dbt parse` validates project structure without querying the warehouse. `dbt build` runs models, tests, seeds, and snapshots in dependency order and requires working credentials.

## Definition of done for each migrated workflow

- Every source, model, and important column is documented.
- Primary keys, relationships, accepted values, and business rules have tests.
- Full-load and incremental paths are reconciled.
- Late-arriving records, deletes, reruns, and schema changes have explicit behavior.
- Runtime meets the SLA and warehouse cost is accepted.
- Ownership, alerting, rollback, and lineage documentation are complete.
- Business and technical owners approve cutover.


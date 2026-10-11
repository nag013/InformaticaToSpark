CREATE TABLE IF NOT EXISTS etl_control.pipeline_control (
  pipeline_name STRING NOT NULL,
  source_name STRING NOT NULL,
  target_name STRING NOT NULL,
  load_type STRING NOT NULL,
  enabled BOOLEAN NOT NULL,
  watermark_column STRING,
  last_watermark STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS etl_control.pipeline_audit (
  run_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  task_name STRING NOT NULL,
  status STRING NOT NULL,
  event_ts STRING NOT NULL,
  message STRING,
  metrics MAP<STRING, STRING>
)
USING DELTA;

INSERT INTO etl_control.pipeline_control
SELECT
  'orders_incremental_to_delta',
  's3://example-raw/orders',
  's3://example-curated/orders',
  'append',
  true,
  'updated_at',
  '2026-01-01T00:00:00Z',
  current_timestamp(),
  current_timestamp();


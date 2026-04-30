# Architecture

This project models a compact manufacturing data platform around the Bosch
production-line defect dataset.

## Data Flow

```text
Local Bosch CSVs
      |
      v
Profiling and sample generation
      |
      +--> Kafka sample event feeds
      |
      v
Snowflake RAW tables
      |
      v
dbt STAGING views
      |
      v
dbt MARTS feature table
      |
      v
Data quality reconciliation checks
```

## Layers

- `src/manufacturing_pipeline/profiling`: reads large local CSV files, generates
  development samples, and writes profile metadata.
- `src/manufacturing_pipeline/ingestion`: publishes sample rows to Kafka and
  validates consumed topic messages.
- `src/manufacturing_pipeline/warehouse`: runs Snowflake raw loads and dbt builds
  from local environment configuration.
- `src/manufacturing_pipeline/validation`: reconciles local samples, raw
  Snowflake tables, and dbt mart outputs.
- `pipelines/airflow/dags`: defines the orchestration DAG for the implemented
  sample pipeline.
- `warehouse/dbt`: contains dbt sources, staging models, mart models, and tests.
- `warehouse/snowflake/ddl`: contains raw-layer Snowflake SQL.

## Execution Paths

- `scripts/run_local_pipeline.sh`: runs the implemented sample pipeline locally.
- `scripts/run_phase3_kafka_check.sh`: runs the Kafka smoke check separately.
- `scripts/run_snowflake_raw_load.sh`: loads raw sample tables into Snowflake.
- `scripts/run_dbt_build.sh`: builds dbt staging and mart models.
- `scripts/run_data_quality_checks.sh`: runs reconciliation checks.

Kafka is optional in the local all-in-one script because it requires Docker.
Set `RUN_KAFKA=1` when you want to include it.

# Manufacturing Defect Data Platform

End-to-end data engineering project for the Bosch manufacturing defect dataset.
The project profiles large local CSV files, simulates production-line event
streams with Kafka, lands raw data in Snowflake, and prepares the foundation for
Airflow orchestration, dbt transformations, and data quality checks.

## Project Goal

Build a readable, reproducible data platform that shows practical data
engineering work:

- Profile multi-GB manufacturing source files without committing raw data.
- Generate small local samples for development and testing.
- Publish sampled rows to Kafka topics as controlled event feeds.
- Validate Kafka message count and schema before downstream loading.
- Define Snowflake raw-layer objects and sample load SQL.
- Prepare for Airflow scheduling, dbt modeling, and data quality validation.

## Dataset

The project uses the Bosch production-line defect data files:

- `train_numeric.csv`
- `train_date.csv`
- `train_categorical.csv`

These files are multi-GB local source files and are intentionally ignored by git.
Generated samples under `data/sample/` and profiling outputs under
`data/profiling/` are also ignored so the GitHub repo stays lightweight.

## Stack

- Kafka for simulated streaming ingestion
- Snowflake for warehouse storage
- Airflow for orchestration
- dbt for transformations
- Python for profiling, ingestion helpers, and tests
- Data quality checks for pipeline validation

## Project Layout

```text
.
├── data/                  # Raw, sample, and profiling data outputs
├── docs/                  # Architecture and phase documentation
├── pipelines/             # Airflow DAGs and Kafka producers
├── src/                   # Reusable Python package code
├── tests/                 # Local tests and validation checks
└── warehouse/             # Snowflake SQL and dbt models
```

## Current Status

Implemented:

- Phase 1: repository scaffold and ignored local data areas
- Phase 2: CSV profiling and sample generation
- Phase 3: Kafka producer, local Kafka smoke test, and topic validation
- Phase 4: Snowflake raw database/stage/table/load/validation SQL
- Phase 5: Airflow DAG definition for local pipeline orchestration
- Phase 6: dbt project with staging models, joined mart, and schema tests
- Phase 7: local and Snowflake data quality reconciliation checks

## Local Setup

Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

Place the Bosch CSV files in the repository root:

```text
train_numeric.csv
train_date.csv
train_categorical.csv
```

Generate development samples and profiling metadata:

```bash
scripts/run_profile.sh 1000
```

## Check Progress

Run the current local verification suite:

```bash
scripts/show_progress.sh
```

Useful focused checks:

```bash
scripts/run_profile.sh 1000
scripts/run_kafka_producer_dry_run.sh numeric 2
PYTHONPATH=src python -m unittest discover -s tests
```

Kafka validation with clean local topics:

```bash
scripts/reset_kafka_topics.sh
scripts/produce_to_kafka.sh numeric 5
scripts/validate_kafka_topic.sh numeric 5
```

Full Phase 3 Kafka smoke test:

```bash
scripts/run_phase3_kafka_check.sh
```

## Snowflake Raw Load

Phase 4 SQL lives in `warehouse/snowflake/ddl/`:

```text
001_create_raw_objects.sql   # database, raw schema, file format, stage, audit table
002_create_raw_tables.sql    # schema-inferred raw tables from staged sample files
003_copy_sample_data.sql     # COPY staged sample CSVs into raw tables
004_validate_raw_load.sql    # row count and ID alignment checks
```

Run the raw load after generating local samples and filling in local Snowflake
credentials in `.env`:

```bash
scripts/run_snowflake_raw_load.sh
```

The script creates raw Snowflake objects, stages generated sample CSVs, infers
wide raw tables, loads the samples, and prints validation query results.

## Airflow Orchestration

The Airflow DAG is defined at:

```text
pipelines/airflow/dags/manufacturing_defect_pipeline.py
```

It orchestrates the current sample pipeline:

1. Generate profiling outputs and sample CSVs.
2. Run the Python unit test suite.
3. Run the Kafka smoke check.
4. Load and validate Snowflake raw sample tables.
5. Build dbt staging views and the feature mart.
6. Run data quality reconciliation checks.

Airflow is kept in `requirements-airflow.txt` so the default local test
environment stays lightweight. Install it only when running the DAG locally.

## dbt Models

The dbt project lives in `warehouse/dbt/`.

Current models:

- `stg_bosch_numeric`: selected numeric features and the defect target
- `stg_bosch_date`: selected date/time station features
- `stg_bosch_categorical`: selected categorical station features
- `mart_defect_features`: joined feature mart keyed by `defect_id`

dbt builds staging views in `STAGING` and the joined feature mart in `MARTS`.

Install dbt dependencies only when running dbt:

```bash
python -m pip install -r requirements-dbt.txt
```

Then run:

```bash
scripts/run_dbt_build.sh
```

The committed `warehouse/dbt/profiles.yml` reads Snowflake credentials from
environment variables, so secrets stay in your ignored local `.env`.

## Data Quality

Run local and Snowflake reconciliation checks:

```bash
scripts/run_data_quality_checks.sh
```

The checks compare generated local sample metadata with Snowflake raw tables and
the dbt mart: row counts, column counts, duplicate/null IDs, ID alignment, and
response distribution.

## Phase Plan

1. Project scaffold and repository layout
2. Local data profiling and sample extraction
3. Kafka producer for controlled row streaming
4. Snowflake raw table design and load path
5. Airflow DAG for orchestration
6. dbt staging and mart models
7. Data quality checks
8. Final analytics output and resume documentation

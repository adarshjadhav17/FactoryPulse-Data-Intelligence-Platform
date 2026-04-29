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

Planned next:

- Phase 5: Airflow DAGs for orchestration
- Phase 6: dbt staging and mart models
- Phase 7: data quality reconciliation checks

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
002_put_sample_files.sql     # PUT generated local samples to the Snowflake stage
003_create_raw_tables.sql    # schema-inferred raw tables from staged sample files
004_copy_sample_data.sql     # COPY staged sample CSVs into raw tables
005_validate_raw_load.sql    # row count and ID alignment checks
```

Run the scripts in order after generating local samples. Snowflake requires
absolute local paths for `PUT`, so replace the placeholder path in
`002_put_sample_files.sql` with your repository path before running it in
SnowSQL.

## Phase Plan

1. Project scaffold and repository layout
2. Local data profiling and sample extraction
3. Kafka producer for controlled row streaming
4. Snowflake raw table design and load path
5. Airflow DAG for orchestration
6. dbt staging and mart models
7. Data quality checks
8. Final analytics output and resume documentation

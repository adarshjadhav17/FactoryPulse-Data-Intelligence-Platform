# Manufacturing Defect Data Platform

End-to-end data engineering project using Bosch production-line data.

## Goal

Build a resume-ready pipeline that ingests large manufacturing CSV files, streams/schedules ingestion, stores curated data in Snowflake, applies transformations, validates data quality, and prepares analytics-ready defect features.

## Dataset

Local raw files:

- `train_numeric.csv`
- `train_date.csv`
- `train_categorical.csv`

These files are intentionally not copied into project subfolders because they are multi-GB source data.

## Planned Stack

- Kafka for simulated streaming ingestion
- Airflow for orchestration
- Snowflake for warehouse storage
- dbt for transformations
- Data quality checks for validation
- Optional dashboard after curated tables are built

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

Kafka validation after starting Kafka and producing messages:

```bash
scripts/validate_kafka_topic.sh numeric 5
```

Full Phase 3 Kafka smoke test:

```bash
scripts/run_phase3_kafka_check.sh
```

## Phase Plan

1. Project scaffold and repository layout
2. Local data profiling and sample extraction
3. Kafka producer for controlled row streaming
4. Snowflake raw table design and load path
5. Airflow DAG for orchestration
6. dbt staging and mart models
7. Data quality checks
8. Final analytics output and resume documentation

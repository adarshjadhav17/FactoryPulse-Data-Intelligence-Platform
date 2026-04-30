# Manufacturing Defect Data Platform

End-to-end data engineering project for the large manufacturing defect dataset.
The project profiles large local CSV files, simulates production-line event
streams with Kafka, lands raw data in Snowflake, orchestrates the sample
pipeline with Airflow, transforms data with dbt, and validates outputs with data
quality checks.

## Why This Project Matters

Manufacturing defect data is wide, sparse, and operationally messy. The machine
dataset used here spans **three multi-GB CSV files** with thousands of production
line features. This project turns that raw local data into a reproducible
warehouse-backed pipeline with streaming simulation, orchestration,
transformations, and quality gates.

The goal is not just to load files. The goal is to demonstrate the engineering
work expected in a real analytics platform:

- Profile multi-GB manufacturing source files without committing raw data.
- Generate small local samples for development and testing.
- Publish sampled rows to Kafka topics as controlled event feeds.
- Validate Kafka message count and schema before downstream loading.
- Define Snowflake raw-layer objects and sample load SQL.
- Orchestrate the sample pipeline with Airflow.
- Transform raw data into a dbt feature mart.
- Reconcile local samples, raw warehouse data, and modeled outputs.

## What This Demonstrates

- **Large-file handling:** works with source files over 2 GB each
- **Streaming simulation:** publishes sampled manufacturing rows to Kafka topics
  for numeric, date, and categorical feeds.
- **Warehouse loading:** creates Snowflake raw objects, stages files, infers wide
  schemas, and loads sample data.
- **Analytics modeling:** builds dbt staging views and a joined mart keyed by
  manufacturing row ID.
- **Orchestration:** provides an Airflow DAG for the implemented sample pipeline.
- **Quality controls:** validates local samples, Kafka messages, raw warehouse
  tables, dbt outputs, row counts, IDs, schemas, and response distribution.

## Pipeline Flow

```text
Machinery CSVs -> profiling/samples -> Kafka smoke feed
           -> Snowflake RAW -> dbt STAGING -> dbt MARTS -> data quality checks
```

See [docs/architecture.md](docs/architecture.md) for the system layout and
[docs/project_summary.md](docs/project_summary.md) for the portfolio summary.

## Dataset

The project uses the Bosch production-line defect data files:

- `train_numeric.csv`
- `train_date.csv`
- `train_categorical.csv`

These files are multi-GB local source files
Generated samples under `data/sample/` and profiling outputs under
`data/profiling/` are ignored so the GitHub repo stays lightweight.

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
├── data/                  # Ignored local sample and profiling outputs
├── docs/                  # Architecture, phase, and portfolio docs
├── pipelines/             # Airflow DAGs
├── scripts/               # Local execution and validation commands
├── src/                   # Reusable Python package code
├── tests/                 # Unit and structural tests
└── warehouse/             # Snowflake SQL and dbt project
```

## Current Status

Implemented and verified:

- **100,000-row development sample** generated from each Bosch source file.
- **3 Kafka topics** for numeric, date, and categorical sample feeds.
- **3 Snowflake RAW tables** loaded and reconciled.
- **3 dbt staging views** plus **1 joined feature mart**.
- **22 dbt models/tests executed successfully**.
- **32 Python tests** covering profiling, Kafka, Snowflake, dbt, Airflow, data
  quality, and project packaging.
- **11 end-to-end quality checks** comparing local samples, Snowflake RAW, and
  dbt MARTS outputs.

Latest verified sample run:

```text
RAW numeric rows:       100,000
RAW date rows:          100,000
RAW categorical rows:   100,000
Aligned IDs:            100,000
Response distribution:  99,687 non-defects, 313 defects
dbt build:              PASS=22 WARN=0 ERROR=0 SKIP=0
```

## Local Setup

Install base Python dependencies:

```bash
python -m pip install -r requirements.txt
```

Install dbt dependencies when running the full local sample pipeline:

```bash
python -m pip install -r requirements-dbt.txt
```

Place the CSV files in the repository root:

```text
train_numeric.csv
train_date.csv
train_categorical.csv
```

Generate development samples and profiling metadata:

```bash
scripts/run_profile.sh 100000
```

## Run The Local Sample Pipeline

After installing base and dbt dependencies, placing the CSVs in the repo
root, and filling in `.env`, run:

```bash
scripts/run_local_pipeline.sh
```

This profiles source files, regenerates samples, runs unit tests, loads
Snowflake raw tables, builds dbt models, and runs data quality checks.

Kafka requires Docker, so it is opt-in for the all-in-one command:

```bash
RUN_KAFKA=1 scripts/run_local_pipeline.sh
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

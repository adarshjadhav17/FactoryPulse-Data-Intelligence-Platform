# Project Summary

## Problem

Manufacturing defect datasets are wide, sparse, and difficult to inspect by hand.
This project builds a reproducible data engineering workflow that profiles Bosch
production-line CSV files, simulates event ingestion, loads raw data into
Snowflake, transforms selected features with dbt, and validates the results.

## Implemented Capabilities

- Profiles multi-GB local source CSV files without committing raw data.
- Creates small, ignored sample files for repeatable development.
- Streams sample rows to Kafka topics and validates message counts and schemas.
- Creates Snowflake raw objects and loads sample data.
- Builds dbt staging views and a joined feature mart.
- Runs data quality checks across local samples, Snowflake raw tables, and dbt
  marts.
- Provides an Airflow DAG for orchestration.

## Validation Results

The current sample run validates:

- 1,000 rows in each raw Snowflake table.
- 1,000 aligned IDs across numeric, date, and categorical tables.
- No null or duplicate IDs in raw tables or mart output.
- Response distribution preserved from local sample to dbt mart.
- dbt build passing with 22 successful models/tests.

## GitHub Notes

Raw data, generated samples, profile summaries, local credentials, dbt artifacts,
and runtime logs are intentionally excluded from git. The committed files are the
pipeline code, SQL, dbt project, Airflow DAG, tests, and documentation needed to
understand and reproduce the project locally.

# Phase Plan

## Phase 1: Scaffold

Create the basic project layout and document how each folder will be used.

Test:

- Confirm expected directories exist.
- Confirm raw CSV files are ignored by git.

## Phase 2: Data Profiling

Profile file sizes, row counts, column counts, null density, target distribution, and sample records.

Test:

- Run profiling script on a small row limit first:

```bash
scripts/run_profile.sh 1000
```

- Confirm sample files and `data/profiling/profile_summary.json` are created.
- Run an optional full scan when exact row counts and full target distribution are needed:

```bash
PYTHONPATH=src python -m manufacturing_pipeline.profiling.profile_data --sample-size 1000 --full-scan
```

## Phase 3: Kafka Producer

Stream sampled CSV rows into Kafka topics that represent production-line event feeds.

Test:

- Run the local dry-run producer first:

```bash
scripts/run_kafka_producer_dry_run.sh numeric 5
```

- Run unit tests:

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

- Start Kafka locally.
- Produce a fixed number of rows.
- Consume from each topic and verify message count and schema.

```bash
scripts/check_local_env.sh
scripts/start_kafka.sh
scripts/produce_to_kafka.sh numeric 10
scripts/check_kafka_offsets.sh bosch.train.numeric
scripts/validate_kafka_topic.sh numeric 10
scripts/consume_kafka_topic.sh bosch.train.numeric 10
```

Or run the Phase 3 smoke test:

```bash
scripts/run_phase3_kafka_check.sh
```

If Docker Compose is unavailable, the scripts fall back to plain `docker run` and
`docker exec` for the local Kafka container.

## Progress Check

Show the current project status, generated samples, profile summary, tests, producer
dry-run output, and Kafka offsets when Kafka is running:

```bash
scripts/show_progress.sh
```

## Phase 4: Snowflake Raw Load

Create Snowflake database, schemas, stages, file formats, and raw tables.

Test:

- Load a small sample file first.
- Validate row counts and column counts in Snowflake.

## Phase 5: Airflow Orchestration

Create DAGs to coordinate profiling, sample generation, ingestion, Snowflake load, and validation.

Test:

- Run DAG tasks manually.
- Validate task logs and output row counts.

## Phase 6: dbt Models

Create staging models and mart tables joining numeric, date, categorical, and response data by `Id`.

Test:

- Run `dbt build`.
- Validate uniqueness and not-null tests on keys.

## Phase 7: Data Quality

Add checks for duplicate IDs, missingness, schema drift, row-count reconciliation, and response distribution.

Test:

- Run checks on sample and loaded warehouse data.
- Force a bad sample to confirm checks fail.

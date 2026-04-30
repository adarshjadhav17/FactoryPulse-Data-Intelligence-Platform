#!/usr/bin/env bash
set -euo pipefail

RUN_KAFKA="${RUN_KAFKA:-0}"
SAMPLE_SIZE="${SAMPLE_SIZE:-1000}"

echo "Profiling source files and generating samples"
scripts/run_profile.sh "${SAMPLE_SIZE}"

echo
echo "Running unit tests"
PYTHONPATH=src python -m unittest discover -s tests

if [ "${RUN_KAFKA}" = "1" ]; then
  echo
  echo "Running Kafka smoke check"
  scripts/run_phase3_kafka_check.sh
else
  echo
  echo "Skipping Kafka smoke check. Set RUN_KAFKA=1 to include it."
fi

echo
echo "Loading Snowflake raw sample tables"
scripts/run_snowflake_raw_load.sh

echo
echo "Building dbt models"
scripts/run_dbt_build.sh

echo
echo "Running data quality checks"
scripts/run_data_quality_checks.sh

#!/usr/bin/env bash
set -euo pipefail

echo "Project files"
echo "-------------"
find . \
  -path ./.git -prune -o \
  -name .DS_Store -prune -o \
  -path '*/__pycache__/*' -prune -o \
  -path './data/sample/*.csv' -prune -o \
  -path './data/profiling/*.json' -prune -o \
  -path './train_numeric.csv' -prune -o \
  -path './train_date.csv' -prune -o \
  -path './train_categorical.csv' -prune -o \
  -maxdepth 5 -type f -print | sort

echo
echo "Sample files"
echo "------------"
if ls data/sample/*_sample.csv >/dev/null 2>&1; then
  wc -l data/sample/*_sample.csv
else
  echo "No sample files found. Run: scripts/run_profile.sh 1000"
fi

echo
echo "Profile summary"
echo "---------------"
if [ -f data/profiling/profile_summary.json ]; then
  python - <<'PY'
import json
from pathlib import Path

summary = json.loads(Path("data/profiling/profile_summary.json").read_text())
print(f"sample_size: {summary['sample_size']}")
print(f"full_scan: {summary['full_scan']}")
print(f"response_distribution: {summary['response_distribution']}")
print(f"id_alignment: {summary['id_alignment']}")
print("files:")
for name, profile in summary["files"].items():
    print(f"  {name}: {profile['columns']} columns, {profile['size_bytes']} bytes")
print("sampled_null_ratio:")
for name, profile in summary["sampled_null_summary"].items():
    print(f"  {name}: {profile['null_cell_ratio']}")
PY
else
  echo "No profile summary found. Run: scripts/run_profile.sh 1000"
fi

echo
echo "Unit tests"
echo "----------"
PYTHONPATH=src python -m unittest discover -s tests

echo
echo "Kafka producer dry run"
echo "----------------------"
scripts/run_kafka_producer_dry_run.sh numeric 2

echo
echo "Kafka offsets"
echo "-------------"
if ! docker ps >/dev/null 2>&1; then
  echo "Docker is not accessible from this shell. If Kafka is running, retry in your terminal."
elif docker ps --filter name=manufacturing-kafka --format '{{.Names}}' | grep -qx manufacturing-kafka; then
  scripts/check_kafka_offsets.sh bosch.train.numeric
  scripts/check_kafka_offsets.sh bosch.train.date
  scripts/check_kafka_offsets.sh bosch.train.categorical
  echo
  echo "Kafka schema validation"
  echo "-----------------------"
  scripts/validate_kafka_topic.sh numeric 5
  scripts/validate_kafka_topic.sh date 3
  scripts/validate_kafka_topic.sh categorical 3
else
  echo "Kafka container is not running. Run: scripts/start_kafka.sh"
fi

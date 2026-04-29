#!/usr/bin/env bash
set -euo pipefail

DATASET="${1:-numeric}"
LIMIT="${2:-10}"

PYTHONPATH=src python -m manufacturing_pipeline.ingestion.kafka_csv_producer \
  --dataset "${DATASET}" \
  --limit "${LIMIT}" \
  --bootstrap-servers localhost:9092


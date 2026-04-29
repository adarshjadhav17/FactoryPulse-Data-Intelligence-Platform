#!/usr/bin/env bash
set -euo pipefail

DATASET="${1:-numeric}"
LIMIT="${2:-5}"

PYTHONPATH=src python -m manufacturing_pipeline.ingestion.kafka_topic_validator \
  --dataset "${DATASET}" \
  --limit "${LIMIT}" \
  --bootstrap-servers localhost:9092


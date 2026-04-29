#!/usr/bin/env bash
set -euo pipefail

SAMPLE_SIZE="${1:-1000}"

PYTHONPATH=src python -m manufacturing_pipeline.profiling.profile_data \
  --sample-size "${SAMPLE_SIZE}"


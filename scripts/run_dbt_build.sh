#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH=src python -m manufacturing_pipeline.warehouse.dbt_runner

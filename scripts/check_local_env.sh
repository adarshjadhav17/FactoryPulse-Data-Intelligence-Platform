#!/usr/bin/env bash
set -euo pipefail

echo "Checking Python..."
python --version

echo "Checking unit tests..."
PYTHONPATH=src python -m unittest discover -s tests

echo "Checking kafka-python..."
python - <<'PY'
try:
    import kafka
except ImportError:
    raise SystemExit("kafka-python is not installed. Run: python -m pip install -r requirements.txt")
print(f"kafka-python {kafka.__version__}")
PY

echo "Checking Docker..."
docker --version
docker info >/dev/null

if docker compose version >/dev/null 2>&1; then
  echo "Docker Compose plugin is available."
elif command -v docker-compose >/dev/null 2>&1; then
  echo "Legacy docker-compose is available."
else
  echo "Docker Compose is not installed. Scripts will use raw docker fallback."
fi

echo "Local environment looks ready."

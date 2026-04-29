#!/usr/bin/env bash
set -euo pipefail

TOPICS=()
while IFS= read -r topic; do
  TOPICS+=("${topic}")
done < <(
  PYTHONPATH="src${PYTHONPATH:+:${PYTHONPATH}}" python - <<'PY'
from manufacturing_pipeline.utils.datasets import DATASETS, dataset_names

for name in dataset_names():
    print(DATASETS[name]["topic"])
PY
)

if [ "${#TOPICS[@]}" -eq 0 ]; then
  echo "No Kafka topics configured." >&2
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  EXEC=(docker compose exec -T kafka)
  BOOTSTRAP="kafka:9092"
elif command -v docker-compose >/dev/null 2>&1; then
  EXEC=(docker-compose exec -T kafka)
  BOOTSTRAP="kafka:9092"
else
  EXEC=(docker exec manufacturing-kafka)
  BOOTSTRAP="localhost:9092"
fi

for topic in "${TOPICS[@]}"; do
  "${EXEC[@]}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --delete \
    --if-exists \
    --topic "${topic}" >/dev/null
done

topics_remaining=1
for _ in {1..30}; do
  existing_topics="$("${EXEC[@]}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --list)"

  topics_remaining=0
  for topic in "${TOPICS[@]}"; do
    if grep -Fxq "${topic}" <<<"${existing_topics}"; then
      topics_remaining=1
    fi
  done

  if [ "${topics_remaining}" -eq 0 ]; then
    break
  fi

  sleep 1
done

if [ "${topics_remaining}" -ne 0 ]; then
  echo "Timed out waiting for Kafka topics to reset." >&2
  exit 1
fi

for topic in "${TOPICS[@]}"; do
  "${EXEC[@]}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1
done

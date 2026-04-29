#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-bosch.train.numeric}"
LIMIT="${2:-10}"

if docker compose version >/dev/null 2>&1; then
  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages "${LIMIT}"
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages "${LIMIT}"
else
  docker exec manufacturing-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" \
    --from-beginning \
    --max-messages "${LIMIT}"
fi

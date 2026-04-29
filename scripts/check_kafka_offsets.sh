#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-bosch.train.numeric}"

docker exec manufacturing-kafka /opt/kafka/bin/kafka-run-class.sh \
  org.apache.kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic "${TOPIC}"

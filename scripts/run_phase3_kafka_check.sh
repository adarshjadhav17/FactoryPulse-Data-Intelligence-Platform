#!/usr/bin/env bash
set -euo pipefail

NUMERIC_LIMIT="${1:-5}"
DATE_LIMIT="${2:-3}"
CATEGORICAL_LIMIT="${3:-3}"

scripts/start_kafka.sh
scripts/reset_kafka_topics.sh

scripts/produce_to_kafka.sh numeric "${NUMERIC_LIMIT}"
scripts/produce_to_kafka.sh date "${DATE_LIMIT}"
scripts/produce_to_kafka.sh categorical "${CATEGORICAL_LIMIT}"

echo
echo "Kafka offsets"
echo "-------------"
scripts/check_kafka_offsets.sh bosch.train.numeric
scripts/check_kafka_offsets.sh bosch.train.date
scripts/check_kafka_offsets.sh bosch.train.categorical

echo
echo "Kafka schema validation"
echo "-----------------------"
scripts/validate_kafka_topic.sh numeric "${NUMERIC_LIMIT}"
scripts/validate_kafka_topic.sh date "${DATE_LIMIT}"
scripts/validate_kafka_topic.sh categorical "${CATEGORICAL_LIMIT}"

#!/usr/bin/env bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
  docker compose up -d kafka kafka-init
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose up -d kafka kafka-init
else
  if docker ps -a --format '{{.Names}}' | grep -qx manufacturing-kafka; then
    docker start manufacturing-kafka >/dev/null
  else
    docker run -d \
      --name manufacturing-kafka \
      -p 9092:29092 \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://localhost:9092 \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      apache/kafka:3.7.0 >/dev/null
  fi

  for _ in {1..30}; do
    if docker exec manufacturing-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done

  docker exec manufacturing-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic bosch.train.numeric --partitions 3 --replication-factor 1
  docker exec manufacturing-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic bosch.train.date --partitions 3 --replication-factor 1
  docker exec manufacturing-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic bosch.train.categorical --partitions 3 --replication-factor 1
fi

#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/bitnami/kafka/bin:${PATH}"

BROKERS="${KAFKA_BROKERS:-kafka:9092}"
TOPIC="${1:-market.trades.raw}"
MAX="${2:-5}"

echo "Consuming ${MAX} messages from ${TOPIC} on ${BROKERS} (from beginning)..."
kafka-console-consumer.sh --bootstrap-server "${BROKERS}" --topic "${TOPIC}" \
  --from-beginning --max-messages "${MAX}"

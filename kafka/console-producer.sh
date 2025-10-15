#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/bitnami/kafka/bin:${PATH}"

BROKERS="${KAFKA_BROKERS:-kafka:9092}"
TOPIC="${1:-market.trades.raw}"

echo "Reading stdin and producing to ${TOPIC} on ${BROKERS}..."
# Usage: echo '{"foo":"bar"}' | ./console-producer.sh market.trades.raw
kafka-console-producer.sh --bootstrap-server "${BROKERS}" --topic "${TOPIC}"

#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/bitnami/kafka/bin:${PATH}"

BROKERS="${KAFKA_BROKERS:-kafka:9092}"

create_topic () {
  local topic="$1"; shift || true
  local partitions="${1:-6}"; shift || true
  local rf="${1:-1}"; shift || true
  kafka-topics.sh --bootstrap-server "${BROKERS}" --create --if-not-exists \
    --topic "${topic}" --partitions "${partitions}" --replication-factor "${rf}" "$@"
}

# Existing topics
create_topic "market.trades.raw"      6 1 --config retention.ms=86400000
create_topic "market.agg.ohlcv.1m"    6 1 --config cleanup.policy=compact,delete --config retention.ms=604800000
create_topic "market.errors"          1 1 --config retention.ms=604800000

# NEW: signals topic (7 days retention)
create_topic "market.signals"         3 1 --config cleanup.policy=delete --config retention.ms=604800000

echo "Topics created (or already existed)."

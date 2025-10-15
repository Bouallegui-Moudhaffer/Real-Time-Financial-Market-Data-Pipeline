#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/bitnami/kafka/bin:${PATH}"

BROKERS="${KAFKA_BROKERS:-kafka:9092}"
TOPICS="${TOPICS:-market.trades.raw market.agg.ohlcv.1m market.errors}"

echo "== Describing topics on ${BROKERS} =="
for t in ${TOPICS}; do
  echo "--- Topic: ${t}"
  kafka-topics.sh --bootstrap-server "${BROKERS}" --describe --topic "${t}" | sed 's/^/  /'
  echo "  Effective configs:"
  kafka-configs.sh --bootstrap-server "${BROKERS}" --describe --entity-type topics --entity-name "${t}" --all | sed 's/^/    /'
done

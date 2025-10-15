#!/usr/bin/env bash
set -euo pipefail
export PATH="/opt/bitnami/kafka/bin:${PATH}"

BROKERS="${KAFKA_BROKERS:-kafka:9092}"

# Desired configs (aligns with the Plan)
TRADES_TOPIC="market.trades.raw"
AGG_1M_TOPIC="market.agg.ohlcv.1m"
ERRORS_TOPIC="market.errors"

echo "== Ensuring topic configurations on ${BROKERS} =="

alter_cfg () {
  local topic="$1"; shift
  local cfg="$1"; shift || true
  echo "--- Altering $topic with: $cfg"
  kafka-configs.sh --bootstrap-server "${BROKERS}" \
    --alter --entity-type topics --entity-name "${topic}" --add-config "${cfg}" || true
}

# market.trades.raw: delete policy, 24h retention
alter_cfg "${TRADES_TOPIC}" "cleanup.policy=delete,retention.ms=86400000"

# market.agg.ohlcv.1m: compact+delete, 7d retention
alter_cfg "${AGG_1M_TOPIC}" "cleanup.policy=compact,delete,retention.ms=604800000"

# market.errors: delete, 7d retention
alter_cfg "${ERRORS_TOPIC}" "cleanup.policy=delete,retention.ms=604800000"

echo "== Done altering topic configs. Current state: =="
/opt/bitnami/kafka/bin/kafka-configs.sh --bootstrap-server "${BROKERS}" --describe --entity-type topics --all | sed 's/^/  /'

#!/usr/bin/env bash
set -euo pipefail

# Windows-friendly wrapper to apply schema repeatedly.
# Requires the 'cassandra' container from docker compose to be running.

echo "[migrate] Applying schema.cql and init.cql..."
docker compose exec cassandra cqlsh -f /cql/schema.cql
if docker compose exec cassandra bash -lc 'test -s /cql/init.cql'; then
  docker compose exec cassandra cqlsh -f /cql/init.cql
fi
echo "[migrate] Done."

# Optional: run sample queries
if [[ "${RUN_SAMPLES:-0}" == "1" ]]; then
  echo "[migrate] Running sample queries..."
  docker compose exec cassandra cqlsh -f /cql/sample_queries.cql
fi

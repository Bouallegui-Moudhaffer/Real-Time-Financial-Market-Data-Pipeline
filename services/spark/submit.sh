#!/usr/bin/env bash
set -euo pipefail

JOB_LOCAL="services/spark/job_ohlcv.py"
JOB_REMOTE="/opt/spark/work-dir/job_ohlcv.py"
SERVICE="spark-master"

# Host-side config (safe under PowerShell/Git-Bash/WSL)
KAFKA_BROKERS="${KAFKA_BROKERS:-kafka:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-market.trades.raw}"
KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-${KAFKA_STARTING_OFF:-latest}}"
SPARK_MAX_OFFSETS_PER_TRIGGER="${SPARK_MAX_OFFSETS_PER_TRIGGER:-5000}"
SPARK_WATERMARK_SECONDS="${SPARK_WATERMARK_SECONDS:-30}"
CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE:-market_ks}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-5 seconds}"
SPARK_SHUFFLE_PARTS="${SPARK_SHUFFLE_PARTS:-8}"
SPARK_LOG_LEVEL="${SPARK_LOG_LEVEL:-WARN}"

# unique checkpoint root per run (override by exporting CHECKPOINT_ROOT)
RUN_ID="$(date +%Y%m%d-%H%M%S)"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/streaming-checkpoints/run-${RUN_ID}}"

# 1) ensure Spark is up
env MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker compose up -d spark-master spark-worker >/dev/null

CID="$(docker compose ps -q "${SERVICE}" || true)"
if [[ -z "${CID}" ]]; then
  echo "[submit] ERROR: spark-master not running"
  docker compose ps
  exit 1
fi

# 2) copy the job into the container
docker cp "${JOB_LOCAL}" "${CID}:${JOB_REMOTE}"
echo "[submit] Copied job to ${SERVICE}:${JOB_REMOTE}"

# 3) run spark-submit directly (FOREGROUND) — no inner bash, no background, no tail
#    Put Ivy home under /tmp to avoid permission issues inside container
echo "[submit] Launching spark-submit (foreground) and streaming logs..."
env MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker compose exec -T \
  -e HOME="/tmp/spark-ivy" \
  -e SPARK_SUBMIT_OPTS="-Duser.home=/tmp/spark-ivy" \
  -e CHECKPOINT_ROOT="${CHECKPOINT_ROOT}" \
  -e KAFKA_BROKERS="${KAFKA_BROKERS}" \
  -e KAFKA_TOPIC="${KAFKA_TOPIC}" \
  -e KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS}" \
  -e SPARK_MAX_OFFSETS_PER_TRIGGER="${SPARK_MAX_OFFSETS_PER_TRIGGER}" \
  -e SPARK_WATERMARK_SECONDS="${SPARK_WATERMARK_SECONDS}" \
  -e CASSANDRA_HOST="${CASSANDRA_HOST}" \
  -e CASSANDRA_PORT="${CASSANDRA_PORT}" \
  -e CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE}" \
  -e TRIGGER_INTERVAL="${TRIGGER_INTERVAL}" \
  -e SPARK_SHUFFLE_PARTS="${SPARK_SHUFFLE_PARTS}" \
  -e SPARK_LOG_LEVEL="${SPARK_LOG_LEVEL}" \
  "${SERVICE}" \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.cassandra.connection.host="${CASSANDRA_HOST}" \
    --conf spark.cassandra.connection.port="${CASSANDRA_PORT}" \
    --conf spark.sql.shuffle.partitions="${SPARK_SHUFFLE_PARTS}" \
    --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=true \
    --conf spark.ui.showConsoleProgress=true \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    "${JOB_REMOTE}"

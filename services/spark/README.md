# Spark Streaming Job — OHLCV

## What it does
Consumes `market.trades.raw` from Kafka, computes 1s and 1m OHLCV + VWAP, and writes to Cassandra:
- Tables: `market_ks.ohlcv_1s`, `market_ks.ohlcv_1m`, `market_ks.indicators_1m`
- Checkpoints: `/opt/checkpoints/{ohlcv_1s,ohlcv_1m,indicators_1m}`

## How to run

### 1) Preconditions
- Kafka/Cassandra up, topics created, schema migrated.
- Producer running:
  - **Replay**: `docker compose run --rm -e PRODUCER_MODE=replay producer`
  - **Live**: `docker compose up -d producer` (requires FINNHUB_API_TOKEN)

### 2) Submit the job
```bash
services/spark/submit.sh

# RTMKT — Real‑Time Market Data (Live Mode)

Finnhub → Kafka → Spark Structured Streaming → Cassandra → **FastAPI (api)** → Grafana

This guide takes you from a fresh clone to a fully working **live** pipeline.  
Everything (topics, schema, jobs) is created by **Compose** and project scripts—no ad‑hoc commands.

---

## 0) Prerequisites

- Docker & Docker Compose
- Open host ports: **8000** (API), **3000** (Grafana), **9042** (Cassandra), **9092** (Kafka), **7077/8081** (Spark)
- A **Finnhub API token** for live data
- (Windows) Use Git Bash or PowerShell. Our scripts handle MSYS path quirks.

---

## 1) Clone & optional `.env`

```bash
git clone <your-repo-url> rtmkt
cd rtmkt
```

Optionally create a `.env` to avoid long commands:

```bash
cat > .env << 'EOF'
# Live data
FINNHUB_API_TOKEN=<put-your-token-here>
SYMBOLS=AAPL,MSFT,SPY

# Spark / streaming
SPARK_SHUFFLE_PARTS=8
SPARK_LOG_LEVEL=WARN
TRIGGER_INTERVAL=5 seconds
SPARK_WATERMARK_SECONDS=30

# Signals
SIGNAL_PCT_CHANGE=0.01   # ±1% per 1m bar
EOF
```

> You can also export these variables in your shell instead of using `.env`.

---

## 2) Bring up the core stack

This starts Kafka (and creates topics), Cassandra (and applies schema), Spark (master/worker), API, and Grafana.

```bash
docker compose pull
docker compose up -d kafka kafka-setup cassandra cassandra-init spark-master spark-worker api grafana
```

Verify:
```bash
docker compose ps
curl -s http://localhost:8000/healthz
# → {"status":"ok","contact_points":["cassandra"],"port":9042,"keyspace":"market_ks"}
```

---

## 3) Start the Spark job

Runs fully **inside** the Spark master and tails logs. Writes to Cassandra and (optionally) emits signals.

```bash
bash services/spark/submit.sh
```

- Uses checkpoints under `/tmp/streaming-checkpoints` (inside the Spark master container).
- Default offsets: `latest` (for live). For backfill, export `KAFKA_STARTING_OFFSETS=earliest` before running.

Keep this terminal open to monitor progress.

---

## 4) Start the **Live Producer**

Stream live trades from Finnhub to Kafka:

```bash
docker compose run --rm \
  -e PRODUCER_MODE=live \
  -e SYMBOLS="${SYMBOLS:-AAPL,MSFT}" \
  -e FINNHUB_API_TOKEN="${FINNHUB_API_TOKEN}" \
  producer
```

Tips:
- To reduce rate‑limits (HTTP 429), limit `SYMBOLS` (e.g., `AAPL,MSFT`).
- For replay demos, use `-e PRODUCER_MODE=replay` (and optionally a date in your Grafana variable).

---

## 5) Quick checks

**Kafka (optional):**
```bash
docker compose exec kafka bash -lc '/kafka-scripts/console-consumer.sh market.trades.raw 5'
```

**Cassandra (1s sample):**
```bash
docker compose exec cassandra cqlsh -e "
SELECT symbol, window_start, open, high, low, close, volume
FROM market_ks.ohlcv_1s
WHERE symbol='AAPL' AND bucket_date=dateof(toTimestamp(now()))
ORDER BY window_start DESC LIMIT 5;"
```

**API:**
```bash
curl "http://localhost:8000/ohlcv/1m?symbol=AAPL&limit=50"
curl "http://localhost:8000/indicators/1m?symbol=AAPL&limit=20"
curl "http://localhost:8000/signals/recent?symbol=AAPL&limit=5"
```

- Leave `date` empty for **live** (API defaults to **today**).
- For a replay day, pass `&date=2023-11-14`.

---

## 6) Grafana

Open **http://localhost:3000** (admin / admin).

### Datasource (Infinity)
1. **Connections → Data sources → Add data source → Infinity**  
2. Set:
   - **Name:** e.g., `Infinity (API)`
   - **Source:** URL
   - **Base URL:** `http://api:8000` (Docker network hostname)
3. **Save & test** → should be green.

### Dashboard
Use the working dashboard JSON you already have (the one with `ohlcv` & `indicators` panels).  
Each panel should:

- **Parser = Simple** (aka Frontend / Simple) in Infinity.
- **Columns mapping**:  
  - `window_start` → **time (timestamp)**
  - metric (e.g., `close`, `vwap`, `volume`, `trade_count`) → **number**
- **URLs** (examples):
  - Close (1s):  `http://api:8000/ohlcv/1s?symbol=${symbol}&limit=2000`
  - Close (1m):  `http://api:8000/ohlcv/1m?symbol=${symbol}&date=${date}&limit=1000`
  - Volume (1m): `http://api:8000/ohlcv/1m?symbol=${symbol}&date=${date}&limit=1000`
  - Indicators:  `http://api:8000/indicators/1m?symbol=${symbol}&date=${date}&limit=1000`

**Common fixes**
- If a time series shows **“No data”** but a Table shows rows: set **Parser = Simple**, ensure `window_start` is mapped to **timestamp** and your metric to **number**.
- If you see a URL error like `https:///…`: check the datasource **Base URL** is `http://api:8000` and your panel URL starts with `/…` or a full URL (not `https:///`).

---

## 7) Signals (optional; enabled by default)

- Topic: `market.signals` (Kafka)
- Table: `market_ks.signals_recent` (Cassandra)
- Threshold: `SIGNAL_PCT_CHANGE` (default `0.01` = ±1%/min)

Inspect:
```bash
docker compose exec cassandra cqlsh -e "
SELECT symbol, ts, type, pct_change, close, vwap, volume
FROM market_ks.signals_recent
WHERE symbol='AAPL' AND bucket_date=dateof(toTimestamp(now()))
ORDER BY ts DESC LIMIT 10;"
```

```bash
docker compose exec kafka bash -lc '/kafka-scripts/console-consumer.sh market.signals 5'
```

API:
```bash
curl "http://localhost:8000/signals/recent?symbol=AAPL&limit=5"
```

---

## 8) Reset / iterate

**Reset Spark checkpoints (after logic/schema changes):**
```bash
docker compose exec spark-master bash -lc 'rm -rf /tmp/streaming-checkpoints/*'
bash services/spark/submit.sh
```

**Recreate Kafka topics (idempotent):**
```bash
docker compose run --rm kafka-setup
```

**Rebuild API after edits:**
```bash
docker compose up -d --build api
```

---

## 9) Stop & clean up

```bash
# Stop containers (keep data)
docker compose stop

# Remove containers & network (keep volumes)
docker compose down

# Full cleanup (remove volumes/data)
docker compose down -v
```

---

## Layout (high-level)

```
services/
  api/         FastAPI read service (Cassandra → JSON)
  producer/    Finnhub → Kafka producer (live or replay)
  spark/       Structured Streaming job + submit.sh
cassandra/     schema.cql (+ init.cql optional)
kafka/         create-topics.sh (+ console scripts)
grafana/       provisioning (datasources/dashboards if used)
```

Notes:
- The legacy `rest` service is **unused**—`api` is the read service used by Grafana.
- Topics (including `market.signals`) are created by `kafka-setup` via `kafka/create-topics.sh`.
- Spark writes to: `ohlcv_1s`, `ohlcv_1m`, `indicators_1m`, and `signals_recent` (Cassandra).
- Checkpoints: `/tmp/streaming-checkpoints` (Spark master container).

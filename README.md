# RTMKT — Real-Time Financial Market Data Pipeline

Finnhub → Kafka → Spark Structured Streaming → Cassandra → FastAPI → Grafana

> This repository is built in steps. **You are at Step 1 (scaffold & tooling).**
> Next steps will introduce Docker Compose orchestration, producers, Spark jobs, Cassandra schema application, REST service, and Grafana provisioning.

## Quick Start (Step 1)

```bash
make help
make bootstrap         # create .venv and install dev deps
make install-all       # install service deps
make lint              # ruff + black check
make test              # run unit tests
```

## Step 2 — Docker Compose Orchestration

### Prereqs
- Docker Desktop (Windows/Mac) or Docker Engine 24+ (Linux)
- Ensure Git uses LF for shell scripts: `git config core.autocrlf input`

### Start the core stack
```bash
# from repo root
docker compose pull
docker compose up -d kafka cassandra
# wait until both are healthy, then:
docker compose up -d cassandra-init
docker compose up -d kafka-setup
docker compose up -d spark-master spark-worker rest grafana
```

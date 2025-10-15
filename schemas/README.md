# Schema Contracts (JSON)

## `market.trades.raw`
- **Schema:** `schemas/trade.schema.json`
- **Key notes**
  - `type` literal: `"trade"`
  - `source`: `"finnhub"` or `"replay"`
  - `ingest_ts`: ISO8601 string
  - `event_ts`: millisecond epoch (int)
  - `symbol`: uppercase ticker
  - `key`: deterministic `"SYMBOL|event_ts|price|volume"`

This schema is validated in the Producer and assumed by Spark.

## Aggregates (Cassandra)
- Shapes are defined in `schemas/ohlcv.schema.json` and in `cassandra/schema.cql`.
- Spark writes windowed OHLCV (1s/1m) and indicators (1m).

> We intentionally avoid a runtime Schema Registry for local dev. The JSON Schema files here are the **contract** for both producer and consumer code.

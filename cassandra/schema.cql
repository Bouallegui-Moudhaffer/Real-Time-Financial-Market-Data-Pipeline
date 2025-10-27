-- =============================================================================
-- RTMKT Cassandra schema (idempotent)
-- Time-series friendly: date bucketing, TWCS compaction, default TTLs.
-- =============================================================================

-- Keyspace --------------------------------------------------------------------
CREATE KEYSPACE IF NOT EXISTS market_ks
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;

-- Switch context
USE market_ks;

-- Helper notes:
--  - All timestamps are UTC.
--  - bucket_date = toDate(window_start/event_time) computed on write.
--  - TWCS window size aligns with TTL cadence for compaction efficiency.
--  - gc_grace_seconds reduced for single-DC dev; increase for multi-DC.

-- =========================
-- Optional RAW trades table
-- =========================
CREATE TABLE IF NOT EXISTS trades_raw (
  symbol text,
  bucket_date date,
  event_time timestamp,
  price double,
  volume double,
  key text,
  PRIMARY KEY ((symbol, bucket_date), event_time, key)
) WITH CLUSTERING ORDER BY (event_time ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '24'
  }
  AND default_time_to_live = 86400   -- 24h
  AND gc_grace_seconds = 86400;      -- 24h; safe for single-DC dev

-- ==============
-- 1-second bars
-- ==============
CREATE TABLE IF NOT EXISTS ohlcv_1s (
  symbol text,
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  trade_count bigint,
  vwap double,
  last_event_ts timestamp,
  src text,
  PRIMARY KEY ((symbol, bucket_date), window_start)
) WITH CLUSTERING ORDER BY (window_start ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '24'
  }
  AND default_time_to_live = 604800  -- 7 days
  AND gc_grace_seconds = 86400;

-- ============
-- 1-minute bars
-- ============
CREATE TABLE IF NOT EXISTS ohlcv_1m (
  symbol text,
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  trade_count bigint,
  vwap double,
  pct_change_1m double,
  last_event_ts timestamp,
  src text,
  PRIMARY KEY ((symbol, bucket_date), window_start)
) WITH CLUSTERING ORDER BY (window_start ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '24'
  }
  AND default_time_to_live = 2592000 -- 30 days
  AND gc_grace_seconds = 86400;

-- =================
-- 1-minute indicators
-- =================
CREATE TABLE IF NOT EXISTS indicators_1m (
  symbol text,
  bucket_date date,
  window_start timestamp,
  window_end timestamp,
  vwap_1m double,
  vol_1m double,
  PRIMARY KEY ((symbol, bucket_date), window_start)
) WITH CLUSTERING ORDER BY (window_start ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '24'
  }
  AND default_time_to_live = 2592000 -- 30 days
  AND gc_grace_seconds = 86400;

-- Helpful comments (DESCRIBE will show):
COMMENT ON TABLE trades_raw IS 'Optional raw trades with 24h TTL; not required for E2E.';
COMMENT ON TABLE ohlcv_1s  IS 'Per-second OHLCV; TTL 7d; keyed by (symbol, date).';
COMMENT ON TABLE ohlcv_1m  IS 'Per-minute OHLCV; TTL 30d; used by REST/Grafana.';
COMMENT ON TABLE indicators_1m IS 'Per-minute indicators (VWAP, volatility).';


-- recent signal events (partition by symbol + bucket date for easy querying)
CREATE TABLE IF NOT EXISTS market_ks.signals_recent (
  symbol      text,
  bucket_date date,
  ts          timestamp,
  type        text,
  pct_change  double,
  close       double,
  vwap        double,
  volume      double,
  src         text,
  PRIMARY KEY ((symbol, bucket_date), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

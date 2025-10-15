-- Sample queries for manual validation
USE market_ks;

-- 1) Check tables & options
DESCRIBE TABLES;
DESCRIBE TABLE market_ks.ohlcv_1m;

-- 2) Insert a quick test row into ohlcv_1m (optional)
-- Adjust timestamps to current UTC day to match bucket_date
-- BEGIN BATCH
-- INSERT INTO ohlcv_1m (symbol, bucket_date, window_start, window_end, open, high, low, close, volume, trade_count, vwap, pct_change_1m, last_event_ts, src)
-- VALUES ('AAPL', toDate('2025-10-13T00:00:00Z'), '2025-10-13T10:00:00Z', '2025-10-13T10:01:00Z',
--   170.0, 171.0, 169.5, 170.8, 12345.0, 250, 170.45, 0.0047, '2025-10-13T10:00:59Z', 'manual-smoke');
-- APPLY BATCH;

-- 3) Query recent 1m bars for a symbol
-- Replace the date with today's UTC date if different
SELECT symbol, window_start, open, high, low, close, volume, vwap
FROM ohlcv_1m
WHERE symbol='AAPL'
  AND bucket_date = toDate('2025-10-13T00:00:00Z')
  AND window_start >= '2025-10-13T10:00:00Z'
  AND window_start <  '2025-10-13T11:00:00Z'
ORDER BY window_start ASC
LIMIT 10;

-- 4) Latest N windows for a symbol (descending):
SELECT symbol, window_start, close, vwap
FROM ohlcv_1m
WHERE symbol='AAPL'
  AND bucket_date = toDate('2025-10-13T00:00:00Z')
ORDER BY window_start DESC
LIMIT 5;

-- 5) Indicators
SELECT symbol, window_start, vwap_1m, vol_1m
FROM indicators_1m
WHERE symbol='AAPL'
  AND bucket_date = toDate('2025-10-13T00:00:00Z')
ORDER BY window_start DESC
LIMIT 5;

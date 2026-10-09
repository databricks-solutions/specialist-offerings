-- Profiler observation window — used to annualize measured DBU costs
-- (DAYS_PER_YEAR / window_days). distinct_days is preferred over wall-clock span.
-- @param catalog STRING = profiler
-- @param database STRING = demo
SELECT
  COUNT(DISTINCT to_date(from_unixtime(started_time / 1000))) AS distinct_days,
  (MAX(finished_time) - MIN(started_time)) / 1000.0 / 86400 AS span_days,
  COUNT(*) AS apps
FROM IDENTIFIER(:catalog || '.' || :database || '.yarn_applications')
WHERE started_time IS NOT NULL AND started_time > 0;

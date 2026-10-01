-- Peak + average hourly resource usage from the profiler, for right-sizing.
-- @param catalog STRING = profiler
-- @param database STRING = demo
SELECT
  ROUND(MAX(max_memory_mb) / 1024.0, 1) AS peak_memory_gb,
  MAX(max_cores)                        AS peak_vcores,
  ROUND(AVG(avg_memory_mb) / 1024.0, 1) AS avg_memory_gb,
  ROUND(AVG(avg_cores), 0)              AS avg_vcores
FROM IDENTIFIER(:catalog || '.' || :database || '.hourly_yarn_view');

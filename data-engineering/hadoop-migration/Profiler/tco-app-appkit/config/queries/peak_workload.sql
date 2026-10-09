-- Peak + average YARN-allocated memory/vcores across the profiler window, from the
-- CM timeseries (cm_ts_yarn_memory_allocation): peak = MAX over time, avg = AVG.
-- Empty for Ambari/HDP clusters (no Cloudera Manager timeseries) — the UI tolerates
-- nulls there and treats node count / sizing as a manual TCO input.
-- @param catalog STRING = profiler
-- @param database STRING = visa_dpi_mar
SELECT
  ROUND(MAX(total_allocated_memory_mb_across_yarn_pools_mean) / 1024.0, 1) AS peak_memory_gb,
  ROUND(MAX(total_allocated_vcores_across_yarn_pools_mean), 0)             AS peak_vcores,
  ROUND(AVG(total_allocated_memory_mb_across_yarn_pools_mean) / 1024.0, 1) AS avg_memory_gb,
  ROUND(AVG(total_allocated_vcores_across_yarn_pools_mean), 0)             AS avg_vcores
FROM IDENTIFIER(:catalog || '.' || :database || '.cm_ts_yarn_memory_allocation');

-- Profiler workload aggregated by job type — the basis for DBU cost allocation.
-- SKU mapping lives in Lakebase (tco.workload_sku_mapping); the engine joins it in TS.
-- @param catalog STRING = profiler
-- @param database STRING = demo
SELECT
  job_type,
  total_jobs,
  total_memory_gb_hours
FROM IDENTIFIER(:catalog || '.' || :database || '.workload_summary_by_type')
ORDER BY total_memory_gb_hours DESC;

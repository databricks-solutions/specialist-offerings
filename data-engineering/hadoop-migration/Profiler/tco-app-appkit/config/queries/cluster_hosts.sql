-- Source-cluster host summary (CDH/CM clusters) for the workload-profile fingerprint.
-- Ambari (HDP) clusters have an empty cm_hosts; node count is a manual TCO input there.
-- @param catalog STRING = profiler
-- @param database STRING = demo
SELECT
  COUNT(*)                        AS node_count,
  SUM(num_cores)                  AS total_vcores,
  ROUND(AVG(num_cores), 1)        AS avg_vcores_per_node,
  ROUND(SUM(total_phys_mem_gb), 1) AS total_mem_gb
FROM IDENTIFIER(:catalog || '.' || :database || '.cm_hosts');

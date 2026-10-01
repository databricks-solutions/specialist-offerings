-- TCO Calculator App - Seed Data
-- Run after schema.sql to populate default mappings and baseline assumptions.
-- Uses INSERT INTO SELECT to avoid inline table limitations with uuid()/current_timestamp().

-- 1. Default workload-to-SKU mapping
INSERT INTO tco_workload_sku_mapping (job_type, target_sku, target_sku_alt, compute_category, notes, updated_at)
SELECT 'Spark (Oozie)',  'PREMIUM_JOBS_COMPUTE',        'PREMIUM_JOBS_SERVERLESS_COMPUTE',  'jobs',        'Oozie-orchestrated Spark to Workflows jobs compute', current_timestamp()
UNION ALL SELECT 'Spark',          'PREMIUM_ALL_PURPOSE_COMPUTE',  'PREMIUM_JOBS_COMPUTE',             'all_purpose', 'Interactive/ad-hoc Spark to all-purpose or jobs', current_timestamp()
UNION ALL SELECT 'Hive (Oozie)',   'PREMIUM_SQL_COMPUTE',          'SERVERLESS_SQL_COMPUTE',           'sql',         'Oozie-orchestrated Hive to SQL warehouse', current_timestamp()
UNION ALL SELECT 'Hive',           'PREMIUM_SQL_COMPUTE',          'SERVERLESS_SQL_COMPUTE',           'sql',         'Interactive Hive to SQL warehouse', current_timestamp()
UNION ALL SELECT 'Sqoop (Oozie)',  'PREMIUM_JOBS_COMPUTE',          NULL,                              'jobs',        'Sqoop ingest to Lakeflow Connect or jobs compute', current_timestamp()
UNION ALL SELECT 'Sqoop',          'PREMIUM_JOBS_COMPUTE',          NULL,                              'jobs',        'Sqoop ingest to Lakeflow Connect or jobs compute', current_timestamp()
UNION ALL SELECT 'MapReduce',      'PREMIUM_JOBS_COMPUTE',          NULL,                              'jobs',        'Legacy MR to refactor to Spark on jobs compute', current_timestamp()
UNION ALL SELECT 'Oozie Launcher', 'PREMIUM_JOBS_COMPUTE',          NULL,                              'jobs',        'Launcher overhead to minimal jobs compute', current_timestamp()
UNION ALL SELECT 'Other',          'PREMIUM_ALL_PURPOSE_COMPUTE',   NULL,                              'all_purpose', 'Unclassified workloads to all-purpose', current_timestamp()
UNION ALL SELECT 'Impala',         'PREMIUM_SQL_COMPUTE',           'SERVERLESS_SQL_COMPUTE',           'sql',         'Impala analytical queries to SQL warehouse', current_timestamp();

-- 2. Baseline assumption sets
INSERT INTO tco_assumptions (
    assumption_id, name, target_cloud, databricks_tier, use_serverless, photon_enabled,
    utilization_factor, overhead_factor, discount_pct,
    vm_mem_gb, hdfs_repl_factor, delta_compression, storage_cost_per_gb_month,
    created_by, created_at
)
SELECT uuid(), 'AWS Premium - Baseline',
    'AWS', 'PREMIUM', false, true,
    0.9, 1.1, 0.0,
    64.0, 3, 0.5, 0.023,
    'seed', current_timestamp();

INSERT INTO tco_assumptions (
    assumption_id, name, target_cloud, databricks_tier, use_serverless, photon_enabled,
    utilization_factor, overhead_factor, discount_pct,
    vm_mem_gb, hdfs_repl_factor, delta_compression, storage_cost_per_gb_month,
    created_by, created_at
)
SELECT uuid(), 'AWS Premium - Serverless',
    'AWS', 'PREMIUM', true, true,
    0.85, 1.05, 0.0,
    64.0, 3, 0.5, 0.023,
    'seed', current_timestamp();

INSERT INTO tco_assumptions (
    assumption_id, name, target_cloud, databricks_tier, use_serverless, photon_enabled,
    utilization_factor, overhead_factor, discount_pct,
    vm_mem_gb, hdfs_repl_factor, delta_compression, storage_cost_per_gb_month,
    created_by, created_at
)
SELECT uuid(), 'Azure Premium - Baseline',
    'AZURE', 'PREMIUM', false, true,
    0.9, 1.1, 0.0,
    64.0, 3, 0.5, 0.018,
    'seed', current_timestamp();

INSERT INTO tco_assumptions (
    assumption_id, name, target_cloud, databricks_tier, use_serverless, photon_enabled,
    utilization_factor, overhead_factor, discount_pct,
    vm_mem_gb, hdfs_repl_factor, delta_compression, storage_cost_per_gb_month,
    created_by, created_at
)
SELECT uuid(), 'Conservative Estimate',
    'AWS', 'PREMIUM', false, true,
    0.7, 1.3, 0.0,
    64.0, 3, 0.6, 0.023,
    'seed', current_timestamp();

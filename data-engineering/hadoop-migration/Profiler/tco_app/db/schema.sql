-- TCO Calculator App — Table Schemas
-- Run against Unity Catalog with USE CATALOG and USE SCHEMA before executing.
-- These tables store configurable mappings, assumptions, pricing snapshots,
-- and calculation results for reproducible TCO runs.
-- Note: DEFAULT clauses removed for Delta compatibility. Defaults handled in app code.

-- 1. Workload-to-SKU mapping (editable via App UI)
CREATE TABLE IF NOT EXISTS tco_workload_sku_mapping (
    job_type          STRING       COMMENT 'From yarn_analysis_vw: Spark (Oozie), Hive, Impala, etc.',
    target_sku        STRING       COMMENT 'Primary Databricks SKU, e.g. PREMIUM_JOBS_COMPUTE',
    target_sku_alt    STRING       COMMENT 'Alternative SKU, e.g. PREMIUM_JOBS_SERVERLESS_COMPUTE',
    compute_category  STRING       COMMENT 'jobs | sql | all_purpose | serverless_sql',
    notes             STRING,
    updated_at        TIMESTAMP
)
COMMENT 'Maps Hadoop workload types to Databricks compute SKUs for TCO calculation';

-- 2. Assumption sets (saved from App UI or preloaded)
CREATE TABLE IF NOT EXISTS tco_assumptions (
    assumption_id           STRING    COMMENT 'Unique ID',
    name                    STRING    COMMENT 'Human label, e.g. BNY baseline Q1',
    target_cloud            STRING    COMMENT 'AWS | AZURE | GCP',
    databricks_tier         STRING    COMMENT 'STANDARD | PREMIUM | ENTERPRISE',
    use_serverless          BOOLEAN,
    photon_enabled          BOOLEAN,
    utilization_factor      DOUBLE    COMMENT 'Fraction of raw GB-hours that become DBUs',
    overhead_factor         DOUBLE    COMMENT 'Multiplier for orchestration/driver overhead',
    discount_pct            DOUBLE    COMMENT 'Contract discount percentage 0-100',
    vm_mem_gb               DOUBLE    COMMENT 'Reference VM memory for instance calc',
    hdfs_repl_factor        INT       COMMENT 'Source HDFS replication factor',
    delta_compression       DOUBLE    COMMENT 'Delta compression ratio vs raw HDFS',
    storage_cost_per_gb_month DOUBLE  COMMENT 'Cloud object storage rate (S3/ADLS/GCS)',
    created_by              STRING,
    created_at              TIMESTAMP
)
COMMENT 'Named assumption sets for TCO calculations';

-- 3. Pricing snapshot (point-in-time freeze from system.billing.list_prices)
CREATE TABLE IF NOT EXISTS tco_pricing_snapshot (
    snapshot_id       STRING    COMMENT 'Groups all prices for a single snapshot',
    snapshot_at       TIMESTAMP COMMENT 'When the snapshot was taken',
    sku_name          STRING    COMMENT 'SKU name from system.billing.list_prices',
    cloud             STRING    COMMENT 'AWS | AZURE | GCP',
    list_price        DOUBLE    COMMENT 'pricing.default from system table',
    effective_price   DOUBLE    COMMENT 'pricing.effective_list from system table',
    price_start_time  TIMESTAMP COMMENT 'Original effective date of this price',
    currency_code     STRING
)
COMMENT 'Point-in-time pricing snapshots from system.billing.list_prices for reproducibility';

-- 4. TCO runs (each calculation execution)
CREATE TABLE IF NOT EXISTS tco_runs (
    run_id                       STRING,
    run_name                     STRING    COMMENT 'Label, e.g. BNY Hadoop TCO - Apr 2026',
    assumption_id                STRING    COMMENT 'FK to tco_assumptions',
    snapshot_id                  STRING    COMMENT 'FK to tco_pricing_snapshot',
    profiler_catalog             STRING    COMMENT 'Source profiler data catalog',
    profiler_schema              STRING    COMMENT 'Source profiler data schema',
    total_hadoop_cost_annual     DOUBLE    COMMENT 'Current Hadoop estate cost if known',
    total_databricks_cost_annual DOUBLE    COMMENT 'Estimated Databricks compute cost',
    total_storage_cost_annual    DOUBLE    COMMENT 'Estimated storage cost on cloud',
    total_cost_annual            DOUBLE    COMMENT 'Compute + storage total',
    savings_pct                  DOUBLE    COMMENT 'Savings vs Hadoop if known',
    created_by                   STRING,
    created_at                   TIMESTAMP
)
COMMENT 'Each TCO calculation run with summary results';

-- 5. TCO run details (per-workload breakdown within a run)
CREATE TABLE IF NOT EXISTS tco_run_details (
    run_id                  STRING    COMMENT 'FK to tco_runs',
    job_type                STRING    COMMENT 'From profiler: Spark (Oozie), Hive, etc.',
    target_sku              STRING    COMMENT 'Mapped Databricks SKU',
    total_apps              BIGINT    COMMENT 'Number of applications in this workload type',
    total_memory_gb_hours   DOUBLE    COMMENT 'Sum of memory_gb_hours from profiler',
    total_vcore_hours       DOUBLE    COMMENT 'Sum of vcore_hours from profiler',
    recommended_node_type   STRING    COMMENT 'Best-fit node type from system.compute.node_types',
    recommended_min_workers INT       COMMENT 'Minimum autoscale workers',
    recommended_max_workers INT       COMMENT 'Maximum autoscale workers',
    estimated_dbu_hours     DOUBLE    COMMENT 'Estimated DBU-hours after utilization/overhead',
    dbu_list_price          DOUBLE    COMMENT 'List price per DBU from snapshot',
    dbu_effective_price     DOUBLE    COMMENT 'After contract discount',
    estimated_cost          DOUBLE    COMMENT 'Annual estimated cost for this workload',
    hadoop_equivalent_cost  DOUBLE    COMMENT 'Current Hadoop cost for comparison'
)
COMMENT 'Per-workload-type cost breakdown within a TCO run';

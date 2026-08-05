-- TCO Calculator App — Schema V2 Migration
-- Adds Hadoop on-prem costs, VM pricing, tiered storage, migration timeline,
-- and expanded assumptions/run columns for full spreadsheet parity.
-- All ALTER TABLE uses IF NOT EXISTS pattern safe for re-runs.

-- ============================================================
-- 1. New lookup tables
-- ============================================================

CREATE TABLE IF NOT EXISTS tco_lookup_vm_instances (
    cloud             STRING    COMMENT 'AWS | AZURE | GCP',
    instance_type     STRING    COMMENT 'e.g. m6id.2xlarge, Standard_E8ds_v4',
    vcpus             INT       COMMENT 'Number of vCPUs',
    memory_gb         DOUBLE    COMMENT 'Memory in GB',
    on_demand_price   DOUBLE    COMMENT '$/hr on-demand',
    reserved_price    DOUBLE    COMMENT '$/hr 1-year reserved',
    spot_price        DOUBLE    COMMENT '$/hr spot/preemptible',
    region            STRING    COMMENT 'Cloud region',
    category          STRING    COMMENT 'worker | driver | dbsql',
    last_refreshed    TIMESTAMP COMMENT 'When prices were last fetched'
)
COMMENT 'Cloud VM instance pricing for TCO compute cost calculations';

CREATE TABLE IF NOT EXISTS tco_lookup_dbsql_sizes (
    size_name         STRING    COMMENT '2X-Small through 4X-Large',
    worker_count      INT       COMMENT 'Number of worker nodes',
    dbu_per_hour      DOUBLE    COMMENT 'DBUs consumed per hour',
    vcpus             INT       COMMENT 'Total vCPUs for the warehouse',
    cloud             STRING    COMMENT 'AWS | AZURE | GCP',
    vm_cost_per_hour  DOUBLE    COMMENT 'Total VM cost for workers'
)
COMMENT 'DBSQL warehouse T-shirt sizes with DBU and VM costs';

CREATE TABLE IF NOT EXISTS tco_vm_price_history (
    fetch_id          STRING    COMMENT 'UUID grouping prices from one API call',
    fetch_time        TIMESTAMP COMMENT 'When the API call was made',
    cloud             STRING    COMMENT 'AWS | AZURE | GCP',
    region            STRING    COMMENT 'Cloud region',
    instance_type     STRING    COMMENT 'VM instance type',
    price_type        STRING    COMMENT 'on_demand | reserved | spot',
    price_per_hour    DOUBLE    COMMENT 'Price in $/hr',
    currency          STRING    COMMENT 'Currency code',
    api_source        STRING    COMMENT 'azure_retail | aws_pricing | gcp_billing | manual_seed',
    raw_response_hash STRING    COMMENT 'SHA256 of API response for reproducibility'
)
COMMENT 'Immutable audit log of every VM price fetch for TCO audit trail';

CREATE TABLE IF NOT EXISTS tco_lookup_storage_tiers (
    cloud             STRING    COMMENT 'AWS | AZURE | GCP',
    tier_name         STRING    COMMENT 'hot | cold | archive',
    volume_min_tb     DOUBLE    COMMENT 'Min volume for this price tier',
    volume_max_tb     DOUBLE    COMMENT 'Max volume for this price tier',
    price_per_gb      DOUBLE    COMMENT '$/GB/month'
)
COMMENT 'Tiered cloud storage pricing for TCO storage calculations';

CREATE TABLE IF NOT EXISTS tco_migration_timeline (
    run_id            STRING    COMMENT 'FK to tco_runs',
    quarter           INT       COMMENT 'Quarter number 1-12',
    quarter_label     STRING    COMMENT 'e.g. Q1 Y1, Q2 Y1',
    migration_pct     DOUBLE    COMMENT 'Fraction migrated 0-1',
    hadoop_cost       DOUBLE    COMMENT 'Hadoop cost this quarter',
    databricks_cost   DOUBLE    COMMENT 'Databricks cost this quarter',
    migration_cost    DOUBLE    COMMENT 'Migration services cost this quarter',
    total_cost        DOUBLE    COMMENT 'Sum of all costs this quarter',
    can_turn_off_hadoop BOOLEAN COMMENT 'True when migration % allows Hadoop shutdown'
)
COMMENT 'Quarterly migration timeline for 3-year TCO comparison';

-- ============================================================
-- 2. ALTER tco_assumptions — one column at a time (no IF NOT EXISTS for ADD COLUMN)
--    db_connector.py catches "already exists" errors and skips.
-- ============================================================

ALTER TABLE tco_assumptions ADD COLUMN hadoop_vendor_type STRING COMMENT 'Licensed | Open Source';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_node_count INT COMMENT 'Number of Hadoop nodes';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_vcores_per_node INT COMMENT 'vCores per node';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_utilization_pct DOUBLE COMMENT 'Avg cluster utilization 0-100';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_license_per_node DOUBLE COMMENT 'Annual license cost per node';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_license_discount DOUBLE COMMENT 'License discount pct 0-100';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_support_pct DOUBLE COMMENT 'Support cost as pct of license';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_hardware_per_node DOUBLE COMMENT 'Annual hardware cost per node';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_datacenter_per_node DOUBLE COMMENT 'Annual datacenter cost per node';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_admin_count INT COMMENT 'Number of Hadoop admins';
ALTER TABLE tco_assumptions ADD COLUMN hadoop_admin_salary DOUBLE COMMENT 'Average admin salary';
ALTER TABLE tco_assumptions ADD COLUMN dev_test_uplift DOUBLE COMMENT 'Dev/test overhead multiplier 0-1';
ALTER TABLE tco_assumptions ADD COLUMN hyperthreading_factor DOUBLE COMMENT 'Hyperthreading adjustment factor';
ALTER TABLE tco_assumptions ADD COLUMN photon_perf_gain DOUBLE COMMENT 'Photon performance gain 0-1';
ALTER TABLE tco_assumptions ADD COLUMN etl_pct DOUBLE COMMENT 'ETL workload percentage 0-100';
ALTER TABLE tco_assumptions ADD COLUMN interactive_pct DOUBLE COMMENT 'Interactive workload percentage 0-100';
ALTER TABLE tco_assumptions ADD COLUMN bisql_pct DOUBLE COMMENT 'BI/SQL workload percentage 0-100';
ALTER TABLE tco_assumptions ADD COLUMN vm_discount_type STRING COMMENT 'on_demand | reserved | spot';
ALTER TABLE tco_assumptions ADD COLUMN worker_instance_type STRING COMMENT 'Worker VM instance type';
ALTER TABLE tco_assumptions ADD COLUMN driver_instance_type STRING COMMENT 'Driver VM instance type';
ALTER TABLE tco_assumptions ADD COLUMN dbsql_warehouse_size STRING COMMENT 'DBSQL warehouse T-shirt size';
ALTER TABLE tco_assumptions ADD COLUMN dbsql_type STRING COMMENT 'classic | pro | serverless';
ALTER TABLE tco_assumptions ADD COLUMN dbsql_utilization DOUBLE COMMENT 'DBSQL utilization factor 0-1';
ALTER TABLE tco_assumptions ADD COLUMN storage_discount_pct DOUBLE COMMENT 'Storage discount percentage 0-100';
ALTER TABLE tco_assumptions ADD COLUMN hot_storage_pct DOUBLE COMMENT 'Pct of data in hot tier 0-100';
ALTER TABLE tco_assumptions ADD COLUMN cold_storage_pct DOUBLE COMMENT 'Pct of data in cold tier 0-100';
ALTER TABLE tco_assumptions ADD COLUMN archive_storage_pct DOUBLE COMMENT 'Pct of data in archive tier 0-100';
ALTER TABLE tco_assumptions ADD COLUMN dbx_support_pct DOUBLE COMMENT 'DBX support cost as pct of DBU spend';
ALTER TABLE tco_assumptions ADD COLUMN dbx_admin_overhead_pct DOUBLE COMMENT 'DBX admin overhead as pct of current admin';
ALTER TABLE tco_assumptions ADD COLUMN migration_tshirt STRING COMMENT 'small | medium | large | custom';
ALTER TABLE tco_assumptions ADD COLUMN migration_custom_cost DOUBLE COMMENT 'Custom migration cost if tshirt=custom';
ALTER TABLE tco_assumptions ADD COLUMN ecif_credit DOUBLE COMMENT 'ECIF credit amount';
ALTER TABLE tco_assumptions ADD COLUMN migration_duration_quarters INT COMMENT 'Number of quarters for migration 1-12';

-- ============================================================
-- 3. ALTER tco_runs — add full cost breakdown columns
-- ============================================================

ALTER TABLE tco_runs ADD COLUMN hadoop_license_cost DOUBLE COMMENT 'Hadoop license cost annual';
ALTER TABLE tco_runs ADD COLUMN hadoop_support_cost DOUBLE COMMENT 'Hadoop support cost annual';
ALTER TABLE tco_runs ADD COLUMN hadoop_hardware_cost DOUBLE COMMENT 'Hadoop hardware cost annual';
ALTER TABLE tco_runs ADD COLUMN hadoop_datacenter_cost DOUBLE COMMENT 'Hadoop datacenter cost annual';
ALTER TABLE tco_runs ADD COLUMN hadoop_admin_cost DOUBLE COMMENT 'Hadoop admin cost annual';
ALTER TABLE tco_runs ADD COLUMN dbx_etl_dbu_cost DOUBLE COMMENT 'Databricks ETL DBU cost annual';
ALTER TABLE tco_runs ADD COLUMN dbx_interactive_dbu_cost DOUBLE COMMENT 'Databricks Interactive DBU cost annual';
ALTER TABLE tco_runs ADD COLUMN dbx_bisql_dbu_cost DOUBLE COMMENT 'Databricks BI/SQL DBU cost annual';
ALTER TABLE tco_runs ADD COLUMN dbx_vm_cost DOUBLE COMMENT 'Databricks VM compute cost annual';
ALTER TABLE tco_runs ADD COLUMN dbx_support_cost DOUBLE COMMENT 'Databricks support cost annual';
ALTER TABLE tco_runs ADD COLUMN dbx_admin_cost DOUBLE COMMENT 'Databricks admin cost annual';
ALTER TABLE tco_runs ADD COLUMN migration_cost_total DOUBLE COMMENT 'Total migration services cost';
ALTER TABLE tco_runs ADD COLUMN three_year_hadoop_total DOUBLE COMMENT '3-year Hadoop total cost';
ALTER TABLE tco_runs ADD COLUMN three_year_databricks_total DOUBLE COMMENT '3-year Databricks total cost';
ALTER TABLE tco_runs ADD COLUMN three_year_savings DOUBLE COMMENT '3-year net savings';
ALTER TABLE tco_runs ADD COLUMN vm_price_fetch_id STRING COMMENT 'FK to tco_vm_price_history for audit';

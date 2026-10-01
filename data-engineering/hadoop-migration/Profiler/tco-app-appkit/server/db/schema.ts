// TCO Postgres schema (Lakebase) — ported from the Dash app's db/schema.sql +
// schema_v2.sql + seed_data.sql + seed_lookups.sql.
//
// Databricks SQL → Postgres type mapping applied: STRING→TEXT, INT→INTEGER,
// BIGINT→BIGINT, DOUBLE→DOUBLE PRECISION, TIMESTAMP→TIMESTAMPTZ, BOOLEAN→BOOLEAN.
// The base + V2 "ALTER TABLE ADD COLUMN" migrations are folded into single
// greenfield CREATE statements (no migration history needed on a fresh DB).
//
// All DDL is idempotent (CREATE ... IF NOT EXISTS). Lookups and baseline
// assumptions are seeded only when their table is empty. Running this on
// startup (onPluginsReady) is what replaces the old manual "Initialize TCO
// Tables" button — the schema provisions itself.

export type LakebaseQuery = (
  text: string,
  params?: unknown[],
) => Promise<{ rows: Record<string, unknown>[] }>;

export const SCHEMA = 'tco';

// ── DDL (ordered; idempotent) ────────────────────────────────────────────────

const DDL: string[] = [
  `CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`,

  // Workload → Databricks SKU mapping (editable in the UI)
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.workload_sku_mapping (
     job_type         TEXT PRIMARY KEY,
     target_sku       TEXT,
     target_sku_alt   TEXT,
     compute_category TEXT,
     notes            TEXT,
     updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,

  // Named assumption sets (base + V2 columns combined)
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.assumptions (
     assumption_id             TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
     name                      TEXT,
     target_cloud              TEXT,
     databricks_tier           TEXT,
     use_serverless            BOOLEAN,
     photon_enabled            BOOLEAN,
     utilization_factor        DOUBLE PRECISION,
     overhead_factor           DOUBLE PRECISION,
     discount_pct              DOUBLE PRECISION,
     vm_mem_gb                 DOUBLE PRECISION,
     hdfs_repl_factor          INTEGER,
     delta_compression         DOUBLE PRECISION,
     storage_cost_per_gb_month DOUBLE PRECISION,
     hadoop_vendor_type        TEXT,
     hadoop_node_count         INTEGER,
     hadoop_vcores_per_node    INTEGER,
     hadoop_utilization_pct    DOUBLE PRECISION,
     hadoop_license_per_node   DOUBLE PRECISION,
     hadoop_license_discount   DOUBLE PRECISION,
     hadoop_support_pct        DOUBLE PRECISION,
     hadoop_hardware_per_node  DOUBLE PRECISION,
     hadoop_datacenter_per_node DOUBLE PRECISION,
     hadoop_admin_count        INTEGER,
     hadoop_admin_salary       DOUBLE PRECISION,
     dev_test_uplift           DOUBLE PRECISION,
     hyperthreading_factor     DOUBLE PRECISION,
     photon_perf_gain          DOUBLE PRECISION,
     etl_pct                   DOUBLE PRECISION,
     interactive_pct           DOUBLE PRECISION,
     bisql_pct                 DOUBLE PRECISION,
     vm_discount_type          TEXT,
     worker_instance_type      TEXT,
     driver_instance_type      TEXT,
     dbsql_warehouse_size      TEXT,
     dbsql_type                TEXT,
     dbsql_utilization         DOUBLE PRECISION,
     storage_discount_pct      DOUBLE PRECISION,
     hot_storage_pct           DOUBLE PRECISION,
     cold_storage_pct          DOUBLE PRECISION,
     archive_storage_pct       DOUBLE PRECISION,
     storage_total_tb          DOUBLE PRECISION,
     dbx_support_pct           DOUBLE PRECISION,
     dbx_admin_overhead_pct    DOUBLE PRECISION,
     migration_tshirt          TEXT,
     migration_custom_cost     DOUBLE PRECISION,
     ecif_credit               DOUBLE PRECISION,
     migration_duration_quarters INTEGER,
     dbu_method                TEXT,
     created_by                TEXT,
     created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,

  // Point-in-time pricing snapshot
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.pricing_snapshot (
     snapshot_id      TEXT,
     snapshot_at      TIMESTAMPTZ,
     sku_name         TEXT,
     cloud            TEXT,
     list_price       DOUBLE PRECISION,
     effective_price  DOUBLE PRECISION,
     price_start_time TIMESTAMPTZ,
     currency_code    TEXT
   )`,

  // TCO runs (summary + full cost breakdown, base + V2 combined)
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.runs (
     run_id                       TEXT PRIMARY KEY,
     run_name                     TEXT,
     assumption_id                TEXT,
     snapshot_id                  TEXT,
     profiler_catalog             TEXT,
     profiler_schema              TEXT,
     total_hadoop_cost_annual     DOUBLE PRECISION,
     total_databricks_cost_annual DOUBLE PRECISION,
     total_storage_cost_annual    DOUBLE PRECISION,
     total_cost_annual            DOUBLE PRECISION,
     savings_pct                  DOUBLE PRECISION,
     hadoop_license_cost          DOUBLE PRECISION,
     hadoop_support_cost          DOUBLE PRECISION,
     hadoop_hardware_cost         DOUBLE PRECISION,
     hadoop_datacenter_cost       DOUBLE PRECISION,
     hadoop_admin_cost            DOUBLE PRECISION,
     dbx_etl_dbu_cost             DOUBLE PRECISION,
     dbx_interactive_dbu_cost     DOUBLE PRECISION,
     dbx_bisql_dbu_cost           DOUBLE PRECISION,
     dbx_vm_cost                  DOUBLE PRECISION,
     dbx_support_cost             DOUBLE PRECISION,
     dbx_admin_cost               DOUBLE PRECISION,
     migration_cost_total         DOUBLE PRECISION,
     three_year_hadoop_total      DOUBLE PRECISION,
     three_year_databricks_total  DOUBLE PRECISION,
     three_year_savings           DOUBLE PRECISION,
     vm_price_fetch_id            TEXT,
     observation_window_days      DOUBLE PRECISION,
     annualization_factor         DOUBLE PRECISION,
     window_floored               BOOLEAN,
     created_by                   TEXT,
     created_at                   TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,

  // Per-workload breakdown within a run
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.run_details (
     run_id                  TEXT,
     job_type                TEXT,
     target_sku              TEXT,
     total_apps              BIGINT,
     total_memory_gb_hours   DOUBLE PRECISION,
     total_vcore_hours       DOUBLE PRECISION,
     recommended_node_type   TEXT,
     recommended_min_workers INTEGER,
     recommended_max_workers INTEGER,
     estimated_dbu_hours     DOUBLE PRECISION,
     window_dbu_hours        DOUBLE PRECISION,
     dbu_list_price          DOUBLE PRECISION,
     dbu_effective_price     DOUBLE PRECISION,
     estimated_cost          DOUBLE PRECISION,
     hadoop_equivalent_cost  DOUBLE PRECISION
   )`,

  // Quarterly 3-year migration timeline
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.migration_timeline (
     run_id              TEXT,
     quarter             INTEGER,
     quarter_label       TEXT,
     migration_pct       DOUBLE PRECISION,
     hadoop_cost         DOUBLE PRECISION,
     databricks_cost     DOUBLE PRECISION,
     migration_cost      DOUBLE PRECISION,
     total_cost          DOUBLE PRECISION,
     can_turn_off_hadoop BOOLEAN
   )`,

  // Lookup: cloud VM instance pricing
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.lookup_vm_instances (
     cloud           TEXT,
     instance_type   TEXT,
     vcpus           INTEGER,
     memory_gb       DOUBLE PRECISION,
     on_demand_price DOUBLE PRECISION,
     reserved_price  DOUBLE PRECISION,
     spot_price      DOUBLE PRECISION,
     region          TEXT,
     category        TEXT,
     last_refreshed  TIMESTAMPTZ
   )`,

  // Lookup: DBSQL warehouse sizes
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.lookup_dbsql_sizes (
     size_name        TEXT,
     worker_count     INTEGER,
     dbu_per_hour     DOUBLE PRECISION,
     vcpus            INTEGER,
     cloud            TEXT,
     vm_cost_per_hour DOUBLE PRECISION
   )`,

  // Lookup: tiered storage pricing
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.lookup_storage_tiers (
     cloud         TEXT,
     tier_name     TEXT,
     volume_min_tb DOUBLE PRECISION,
     volume_max_tb DOUBLE PRECISION,
     price_per_gb  DOUBLE PRECISION
   )`,

  // Immutable VM price fetch audit log
  `CREATE TABLE IF NOT EXISTS ${SCHEMA}.vm_price_history (
     fetch_id          TEXT,
     fetch_time        TIMESTAMPTZ,
     cloud             TEXT,
     region            TEXT,
     instance_type     TEXT,
     price_type        TEXT,
     price_per_hour    DOUBLE PRECISION,
     currency          TEXT,
     api_source        TEXT,
     raw_response_hash TEXT
   )`,

  // Additive migrations (idempotent) for columns added after a table first shipped.
  `ALTER TABLE ${SCHEMA}.assumptions ADD COLUMN IF NOT EXISTS dbu_method TEXT`,
  `ALTER TABLE ${SCHEMA}.assumptions ADD COLUMN IF NOT EXISTS storage_total_tb DOUBLE PRECISION`,
];

// ── Seed data (ported from seed_data.sql / seed_lookups.sql) ──────────────────

const SKU_MAPPING: Array<[string, string, string | null, string, string]> = [
  ['Spark (Oozie)', 'PREMIUM_JOBS_COMPUTE', 'PREMIUM_JOBS_SERVERLESS_COMPUTE', 'jobs', 'Oozie-orchestrated Spark to Workflows jobs compute'],
  ['Spark', 'PREMIUM_ALL_PURPOSE_COMPUTE', 'PREMIUM_JOBS_COMPUTE', 'all_purpose', 'Interactive/ad-hoc Spark to all-purpose or jobs'],
  ['Hive (Oozie)', 'PREMIUM_SQL_COMPUTE', 'SERVERLESS_SQL_COMPUTE', 'sql', 'Oozie-orchestrated Hive to SQL warehouse'],
  ['Hive', 'PREMIUM_SQL_COMPUTE', 'SERVERLESS_SQL_COMPUTE', 'sql', 'Interactive Hive to SQL warehouse'],
  ['Sqoop (Oozie)', 'PREMIUM_JOBS_COMPUTE', null, 'jobs', 'Sqoop ingest to Lakeflow Connect or jobs compute'],
  ['Sqoop', 'PREMIUM_JOBS_COMPUTE', null, 'jobs', 'Sqoop ingest to Lakeflow Connect or jobs compute'],
  ['MapReduce', 'PREMIUM_JOBS_COMPUTE', null, 'jobs', 'Legacy MR to refactor to Spark on jobs compute'],
  ['Oozie Launcher', 'PREMIUM_JOBS_COMPUTE', null, 'jobs', 'Launcher overhead to minimal jobs compute'],
  ['Other', 'PREMIUM_ALL_PURPOSE_COMPUTE', null, 'all_purpose', 'Unclassified workloads to all-purpose'],
  ['Impala', 'PREMIUM_SQL_COMPUTE', 'SERVERLESS_SQL_COMPUTE', 'sql', 'Impala analytical queries to SQL warehouse'],
];

// name, target_cloud, tier, use_serverless, photon, util, overhead, discount, vm_mem_gb, repl, compression, storage_rate
const ASSUMPTION_BASELINES: Array<[string, string, boolean, number, number, number, number]> = [
  // name, cloud, serverless, util, overhead, compression, storage_rate
  ['AWS Premium - Baseline', 'AWS', false, 0.9, 1.1, 0.5, 0.023],
  ['AWS Premium - Serverless', 'AWS', true, 0.85, 1.05, 0.5, 0.023],
  ['Azure Premium - Baseline', 'AZURE', false, 0.9, 1.1, 0.5, 0.018],
  ['Conservative Estimate', 'AWS', false, 0.7, 1.3, 0.6, 0.023],
];

// cloud, instance_type, vcpus, memory_gb, on_demand, reserved, spot, region, category
const VM_INSTANCES: Array<[string, string, number, number, number, number, number, string, string]> = [
  ['AWS', 'm6id.2xlarge', 8, 32.0, 0.5016, 0.3311, 0.3010, 'us-east-1', 'worker'],
  ['AWS', 'm6id.4xlarge', 16, 64.0, 1.0032, 0.6621, 0.6019, 'us-east-1', 'worker'],
  ['AWS', 'm6id.8xlarge', 32, 128.0, 2.0064, 1.3242, 1.2038, 'us-east-1', 'worker'],
  ['AWS', 'm6id.xlarge', 4, 16.0, 0.2508, 0.1656, 0.1505, 'us-east-1', 'driver'],
  ['AZURE', 'Standard_E8ds_v4', 8, 64.0, 0.576, 0.3802, 0.3456, 'eastus', 'worker'],
  ['AZURE', 'Standard_E16ds_v4', 16, 128.0, 1.152, 0.7603, 0.6912, 'eastus', 'worker'],
  ['AZURE', 'Standard_E4ds_v4', 4, 32.0, 0.288, 0.1901, 0.1728, 'eastus', 'driver'],
  ['GCP', 'n2-highmem-8', 8, 64.0, 0.5266, 0.3476, 0.1580, 'us-central1', 'worker'],
  ['GCP', 'n2-highmem-16', 16, 128.0, 1.0532, 0.6951, 0.3160, 'us-central1', 'worker'],
  ['GCP', 'n2-highmem-4', 4, 32.0, 0.2633, 0.1738, 0.0790, 'us-central1', 'driver'],
];

// size_name, worker_count, dbu_per_hour, vcpus, cloud, vm_cost_per_hour
const DBSQL_SIZES: Array<[string, number, number, number, string, number]> = [
  ['2X-Small', 1, 2.0, 8, 'AWS', 0.50], ['X-Small', 2, 4.0, 16, 'AWS', 1.00], ['Small', 4, 8.0, 32, 'AWS', 2.00],
  ['Medium', 8, 16.0, 64, 'AWS', 4.01], ['Large', 16, 32.0, 128, 'AWS', 8.02], ['X-Large', 32, 64.0, 256, 'AWS', 16.04],
  ['2X-Large', 64, 128.0, 512, 'AWS', 32.08], ['3X-Large', 128, 256.0, 1024, 'AWS', 64.16], ['4X-Large', 256, 512.0, 2048, 'AWS', 128.32],
  ['2X-Small', 1, 2.0, 8, 'AZURE', 0.58], ['X-Small', 2, 4.0, 16, 'AZURE', 1.15], ['Small', 4, 8.0, 32, 'AZURE', 2.30],
  ['Medium', 8, 16.0, 64, 'AZURE', 4.61], ['Large', 16, 32.0, 128, 'AZURE', 9.22], ['X-Large', 32, 64.0, 256, 'AZURE', 18.43],
  ['2X-Large', 64, 128.0, 512, 'AZURE', 36.86], ['3X-Large', 128, 256.0, 1024, 'AZURE', 73.73], ['4X-Large', 256, 512.0, 2048, 'AZURE', 147.46],
  ['2X-Small', 1, 2.0, 8, 'GCP', 0.53], ['X-Small', 2, 4.0, 16, 'GCP', 1.05], ['Small', 4, 8.0, 32, 'GCP', 2.11],
  ['Medium', 8, 16.0, 64, 'GCP', 4.21], ['Large', 16, 32.0, 128, 'GCP', 8.43], ['X-Large', 32, 64.0, 256, 'GCP', 16.85],
  ['2X-Large', 64, 128.0, 512, 'GCP', 33.70], ['3X-Large', 128, 256.0, 1024, 'GCP', 67.41], ['4X-Large', 256, 512.0, 2048, 'GCP', 134.82],
];

// cloud, tier_name, volume_min_tb, volume_max_tb, price_per_gb
const STORAGE_TIERS: Array<[string, string, number, number, number]> = [
  ['AWS', 'hot', 0.0, 50.0, 0.023], ['AWS', 'hot', 50.0, 500.0, 0.022], ['AWS', 'hot', 500.0, 999999.0, 0.021],
  ['AWS', 'cold', 0.0, 999999.0, 0.0125], ['AWS', 'archive', 0.0, 999999.0, 0.004],
  ['AZURE', 'hot', 0.0, 50.0, 0.018], ['AZURE', 'hot', 50.0, 500.0, 0.0173], ['AZURE', 'hot', 500.0, 999999.0, 0.0166],
  ['AZURE', 'cold', 0.0, 999999.0, 0.01], ['AZURE', 'archive', 0.0, 999999.0, 0.002],
  ['GCP', 'hot', 0.0, 999999.0, 0.020], ['GCP', 'cold', 0.0, 999999.0, 0.010], ['GCP', 'archive', 0.0, 999999.0, 0.004],
];

// ── Helpers ───────────────────────────────────────────────────────────────────

async function isEmpty(query: LakebaseQuery, table: string): Promise<boolean> {
  const { rows } = await query(`SELECT 1 FROM ${SCHEMA}.${table} LIMIT 1`);
  return rows.length === 0;
}

/** Build a multi-row INSERT with positional params ($1,$2,...). */
function insertMany(table: string, cols: string[], rows: unknown[][]): { text: string; params: unknown[] } {
  const params: unknown[] = [];
  const tuples = rows.map((row) => {
    const placeholders = row.map((v) => {
      params.push(v);
      return `$${params.length}`;
    });
    return `(${placeholders.join(', ')})`;
  });
  return {
    text: `INSERT INTO ${SCHEMA}.${table} (${cols.join(', ')}) VALUES ${tuples.join(', ')}`,
    params,
  };
}

// ── Setup (idempotent) ────────────────────────────────────────────────────────

/**
 * Create the TCO schema + tables (idempotent) and seed lookups + baseline
 * assumptions when empty. Safe to run on every startup. This is the
 * auto-provisioning that replaces the Dash app's manual "Initialize TCO Tables".
 */
export async function setupTcoSchema(query: LakebaseQuery): Promise<void> {
  for (const stmt of DDL) {
    await query(stmt);
  }

  if (await isEmpty(query, 'workload_sku_mapping')) {
    const { text, params } = insertMany(
      'workload_sku_mapping',
      ['job_type', 'target_sku', 'target_sku_alt', 'compute_category', 'notes'],
      SKU_MAPPING.map((r) => [...r]),
    );
    await query(text, params);
  }

  if (await isEmpty(query, 'assumptions')) {
    // Full baseline rows with the spreadsheet defaults the Dash seed used.
    const rows = ASSUMPTION_BASELINES.map(([name, cloud, serverless, util, overhead, compression, storageRate]) => [
      name, cloud, 'PREMIUM', serverless, true, util, overhead, 0.0, 64.0, 3, compression, storageRate, 'seed',
    ]);
    const { text, params } = insertMany(
      'assumptions',
      [
        'name', 'target_cloud', 'databricks_tier', 'use_serverless', 'photon_enabled',
        'utilization_factor', 'overhead_factor', 'discount_pct', 'vm_mem_gb',
        'hdfs_repl_factor', 'delta_compression', 'storage_cost_per_gb_month', 'created_by',
      ],
      rows,
    );
    await query(text, params);
  }

  if (await isEmpty(query, 'lookup_vm_instances')) {
    const now = new Date().toISOString();
    const { text, params } = insertMany(
      'lookup_vm_instances',
      ['cloud', 'instance_type', 'vcpus', 'memory_gb', 'on_demand_price', 'reserved_price', 'spot_price', 'region', 'category', 'last_refreshed'],
      VM_INSTANCES.map((r) => [...r, now]),
    );
    await query(text, params);
  }

  if (await isEmpty(query, 'lookup_dbsql_sizes')) {
    const { text, params } = insertMany(
      'lookup_dbsql_sizes',
      ['size_name', 'worker_count', 'dbu_per_hour', 'vcpus', 'cloud', 'vm_cost_per_hour'],
      DBSQL_SIZES.map((r) => [...r]),
    );
    await query(text, params);
  }

  if (await isEmpty(query, 'lookup_storage_tiers')) {
    const { text, params } = insertMany(
      'lookup_storage_tiers',
      ['cloud', 'tier_name', 'volume_min_tb', 'volume_max_tb', 'price_per_gb'],
      STORAGE_TIERS.map((r) => [...r]),
    );
    await query(text, params);
  }
}

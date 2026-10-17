// TCO engine constants — ported from the Dash app's models/constants.py
// (sourced from the "Lookups- Other" sheet). Fallback defaults; the app prefers
// the Lakebase lookup tables where available.

import type { Assumptions } from '../../shared/tco-types';

export const HOURS_PER_YEAR = 8760;
export const DAYS_PER_YEAR = 365;
/** Floor so a tiny capture can't imply a ~35,000x annualization multiplier. */
export const MIN_WINDOW_DAYS = 1;
export const RECOMMENDED_WINDOW_DAYS = 7;

// Performance gains by engine type (fraction of vCores saved)
export const PERFORMANCE_GAINS: Record<string, number> = {
  classic: 0.3,
  photon: 0.75,
  serverless: 0.75,
};

// DBSQL utilization by warehouse type
export const DBSQL_UTILIZATION: Record<string, number> = {
  classic: 0.7,
  pro: 0.7,
  serverless: 1.0,
};

// VM pricing discount by purchase type (fraction off on-demand)
export const VM_DISCOUNTS: Record<string, number> = {
  on_demand: 0.0,
  reserved: 0.34,
  spot: 0.4,
};

// ── Migration services T-shirt sizing ─────────────────────────────────────────
// Dash constants.py had only cost; the sheet's "Lookups- Migration & Admin
// T-Shirt Sizing" tab also drives timeline (months) + support FTEs + node-count
// bucketing. Full table ported here (TODO #7).
export interface TshirtSize {
  cost: number;
  timeline_months: number;
  ftes: number;
  node_min: number; // inclusive lower bound
  node_max: number; // exclusive upper bound (Infinity for the top bucket)
}

export const MIGRATION_TSHIRT: Record<'small' | 'medium' | 'large' | 'custom', TshirtSize> = {
  small: { cost: 500_000, timeline_months: 6, ftes: 3, node_min: 0, node_max: 50 },
  medium: { cost: 850_000, timeline_months: 8, ftes: 5, node_min: 50, node_max: 150 },
  large: { cost: 1_750_000, timeline_months: 11, ftes: 7, node_min: 150, node_max: 500 },
  // "custom" = 500+ nodes; cost 0 here means "use migration_custom_cost".
  custom: { cost: 0, timeline_months: 24, ftes: 9, node_min: 500, node_max: Infinity },
};

/** Auto-select the T-shirt size from node count (sheet's bucketing). */
export function tshirtForNodeCount(nodes: number): keyof typeof MIGRATION_TSHIRT {
  if (nodes < 50) return 'small';
  if (nodes < 150) return 'medium';
  if (nodes < 500) return 'large';
  return 'custom';
}

// DBU rates by cloud (jobs/all-purpose, classic/photon) — from spreadsheet.
export const DBU_RATES: Record<string, Record<string, number>> = {
  AWS: { jobs_classic: 0.15, jobs_photon: 0.2, all_purpose_classic: 0.4, all_purpose_photon: 0.55 },
  AZURE: { jobs_classic: 0.15, jobs_photon: 0.2, all_purpose_classic: 0.4, all_purpose_photon: 0.55 },
  GCP: { jobs_classic: 0.15, jobs_photon: 0.2, all_purpose_classic: 0.4, all_purpose_photon: 0.55 },
};

// Default assumption values (ported from DEFAULT_ASSUMPTIONS).
export const DEFAULT_ASSUMPTIONS: Required<
  Pick<
    Assumptions,
    | 'hadoop_vendor_type' | 'hadoop_node_count' | 'hadoop_vcores_per_node' | 'hadoop_utilization_pct'
    | 'hadoop_license_per_node' | 'hadoop_license_discount' | 'hadoop_support_pct'
    | 'hadoop_hardware_per_node' | 'hadoop_datacenter_per_node' | 'hadoop_admin_count' | 'hadoop_admin_salary'
    | 'dev_test_uplift' | 'hyperthreading_factor' | 'photon_perf_gain'
    | 'etl_pct' | 'interactive_pct' | 'bisql_pct'
    | 'vm_discount_type' | 'worker_instance_type' | 'driver_instance_type'
    | 'dbsql_warehouse_size' | 'dbsql_type' | 'dbsql_utilization'
    | 'storage_discount_pct' | 'hot_storage_pct' | 'cold_storage_pct' | 'archive_storage_pct'
    | 'dbx_support_pct' | 'dbx_admin_overhead_pct'
    | 'migration_tshirt' | 'migration_custom_cost' | 'ecif_credit' | 'migration_duration_quarters'
    | 'utilization_factor' | 'overhead_factor' | 'discount_pct'
  >
> = {
  hadoop_vendor_type: 'Licensed',
  hadoop_node_count: 100,
  hadoop_vcores_per_node: 32,
  hadoop_utilization_pct: 30.0,
  hadoop_license_per_node: 11_200.0,
  hadoop_license_discount: 0.0,
  hadoop_support_pct: 25.0,
  hadoop_hardware_per_node: 1_000.0,
  hadoop_datacenter_per_node: 5_000.0,
  hadoop_admin_count: 6,
  hadoop_admin_salary: 180_000.0,
  dev_test_uplift: 0.2,
  hyperthreading_factor: 2.0,
  photon_perf_gain: 0.75,
  etl_pct: 40.0,
  interactive_pct: 30.0,
  bisql_pct: 30.0,
  vm_discount_type: 'on_demand',
  worker_instance_type: 'm6id.2xlarge',
  driver_instance_type: 'm6id.xlarge',
  dbsql_warehouse_size: 'Medium',
  dbsql_type: 'pro',
  dbsql_utilization: 0.7,
  storage_discount_pct: 0.0,
  hot_storage_pct: 70.0,
  cold_storage_pct: 20.0,
  archive_storage_pct: 10.0,
  dbx_support_pct: 25.0,
  dbx_admin_overhead_pct: 30.0,
  migration_tshirt: 'medium',
  migration_custom_cost: 0.0,
  ecif_credit: 0.0,
  migration_duration_quarters: 8,
  utilization_factor: 0.9,
  overhead_factor: 1.1,
  discount_pct: 0.0,
};

/** Fill missing assumption fields with defaults (mirrors get_assumptions NULL-fill). */
export function withDefaults(a: Assumptions): Assumptions & typeof DEFAULT_ASSUMPTIONS {
  return { ...DEFAULT_ASSUMPTIONS, ...stripNullish(a) };
}

function stripNullish(a: Assumptions): Assumptions {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(a)) {
    if (v !== null && v !== undefined) out[k] = v;
  }
  return out as Assumptions;
}

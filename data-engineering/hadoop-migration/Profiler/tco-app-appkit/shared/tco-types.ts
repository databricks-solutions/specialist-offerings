// Shared TCO contract — imported by the server (cost engine + routes) and the
// client (UI). Mirrors the Dash app's assumption/run shapes so the ported engine
// stays faithful to the source spreadsheet.

/** Full assumption set (matches tco.assumptions columns). All optional on the
 *  wire; the engine fills gaps from DEFAULT_ASSUMPTIONS. */
export interface Assumptions {
  assumption_id?: string;
  name?: string;
  target_cloud?: 'AWS' | 'AZURE' | 'GCP';
  databricks_tier?: 'STANDARD' | 'PREMIUM' | 'ENTERPRISE';
  use_serverless?: boolean;
  photon_enabled?: boolean;
  utilization_factor?: number;
  overhead_factor?: number;
  discount_pct?: number;
  vm_mem_gb?: number;
  hdfs_repl_factor?: number;
  delta_compression?: number;
  storage_cost_per_gb_month?: number;
  // Hadoop on-prem
  hadoop_vendor_type?: 'Licensed' | 'Open Source';
  hadoop_node_count?: number;
  hadoop_vcores_per_node?: number;
  hadoop_utilization_pct?: number;
  hadoop_license_per_node?: number;
  hadoop_license_discount?: number;
  hadoop_support_pct?: number;
  hadoop_hardware_per_node?: number;
  hadoop_datacenter_per_node?: number;
  hadoop_admin_count?: number;
  hadoop_admin_salary?: number;
  // Databricks modifiers
  dev_test_uplift?: number;
  hyperthreading_factor?: number;
  photon_perf_gain?: number;
  etl_pct?: number;
  interactive_pct?: number;
  bisql_pct?: number;
  vm_discount_type?: 'on_demand' | 'reserved' | 'spot';
  worker_instance_type?: string;
  driver_instance_type?: string;
  dbsql_warehouse_size?: string;
  dbsql_type?: 'classic' | 'pro' | 'serverless';
  dbsql_utilization?: number;
  storage_discount_pct?: number;
  hot_storage_pct?: number;
  cold_storage_pct?: number;
  archive_storage_pct?: number;
  dbx_support_pct?: number;
  dbx_admin_overhead_pct?: number;
  migration_tshirt?: 'small' | 'medium' | 'large' | 'custom';
  migration_custom_cost?: number;
  ecif_credit?: number;
  migration_duration_quarters?: number;
  // Databricks DBU derivation method (AppKit addition — see TODO #6).
  // 'measured' = annualize profiler workload (Dash behavior);
  // 'capacity' = sheet's nodes × vCores × split × util formula.
  dbu_method?: 'measured' | 'capacity';
}

export interface HadoopCosts {
  license_cost: number;
  support_cost: number;
  hardware_cost: number;
  datacenter_cost: number;
  admin_cost: number;
  total: number;
  node_count: number;
  vendor_type: string;
}

export interface ObservationWindow {
  window_days: number;
  distinct_days: number;
  span_days: number;
  annualization_factor: number;
  floored: boolean;
}

export interface TimelineQuarter {
  quarter: number;
  quarter_label: string;
  migration_pct: number;
  hadoop_cost: number;
  databricks_cost: number;
  migration_cost: number;
  total_cost: number;
  can_turn_off_hadoop: boolean;
}

export interface TimelineSummary {
  three_year_total: number;
  three_year_hadoop_portion: number;
  three_year_databricks_portion: number;
  three_year_migration_portion: number;
  do_nothing_total: number;
  net_savings: number;
  savings_pct: number;
  payback_quarter: number | null;
}

/** Request to run a TCO calculation. */
export interface CalculateRequest {
  assumption_id: string;
  catalog: string;
  database: string;
  run_name?: string;
  /** Optional override of the Hadoop annual cost (else computed from assumptions). */
  hadoop_cost_annual?: number;
}

/** Full result of a TCO run (also persisted to tco.runs / run_details / timeline). */
export interface TcoResult {
  run_id: string;
  run_name: string;
  assumption_id: string;
  hadoop_costs: HadoopCosts;
  hadoop_annual: number;
  stream_dbu_costs: { etl: number; interactive: number; bisql: number };
  total_compute_cost_annual: number;
  vm_cost_annual: number;
  total_storage_cost_annual: number;
  dbx_support_cost: number;
  dbx_admin_cost: number;
  total_dbx_annual: number;
  savings_pct: number | null;
  dbu_method: 'measured' | 'capacity';
  observation_window: ObservationWindow;
  timeline: TimelineQuarter[];
  timeline_summary: TimelineSummary;
  details: RunDetail[];
}

export interface RunDetail {
  job_type: string;
  target_sku: string;
  total_apps: number;
  total_memory_gb_hours: number;
  estimated_dbu_hours: number;
  window_dbu_hours: number;
  dbu_effective_price: number;
  estimated_cost: number;
}

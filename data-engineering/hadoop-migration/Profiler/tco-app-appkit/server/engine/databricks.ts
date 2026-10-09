// Databricks-side cost model — ported from the Dash app's cost_engine.py (DBU loop),
// vm_costs.py, and storage.py. Pure functions: the orchestrator fetches workload
// rows (warehouse), lookups + SKU mapping (Lakebase) and feeds them in.

import type { Assumptions, RunDetail } from '../../shared/tco-types';
import { HOURS_PER_YEAR, VM_DISCOUNTS, withDefaults } from './constants';

const round2 = (n: number) => Math.round(n * 100) / 100;

// Observed $/DBU by SKU (from the sheet / Dash pricing snapshot). The orchestrator
// may override these from a live system.billing.list_prices snapshot (TODO).
export const SKU_DBU_RATE: Record<string, number> = {
  PREMIUM_JOBS_COMPUTE: 0.15,
  PREMIUM_JOBS_SERVERLESS_COMPUTE: 0.2,
  PREMIUM_ALL_PURPOSE_COMPUTE: 0.55,
  PREMIUM_SQL_COMPUTE: 0.22,
  SERVERLESS_SQL_COMPUTE: 0.7,
};
const DEFAULT_SKU = 'PREMIUM_ALL_PURPOSE_COMPUTE';

export interface WorkloadRow {
  job_type: string;
  total_jobs: number;
  total_memory_gb_hours: number;
}
export interface SkuMappingRow {
  job_type: string;
  target_sku: string;
  target_sku_alt: string | null;
  compute_category: string;
}

function categoryToStream(cat: string): 'etl' | 'interactive' | 'bisql' {
  if (cat === 'sql' || cat === 'serverless_sql') return 'bisql';
  if (cat === 'all_purpose') return 'interactive';
  return 'etl';
}

/**
 * MEASURED DBU — faithful port of cost_engine.py's per-workload loop.
 * window_dbus = mem_gb_hours × util × overhead × (1+dev_test) × (1−perf_gain)
 * estimated_dbus = window_dbus × annualizationFactor
 * annual_cost = estimated_dbus × effective_price
 */
export function computeMeasuredDbu(
  workloads: WorkloadRow[],
  skuMapping: SkuMappingRow[],
  assumptions: Assumptions,
  annualizationFactor: number,
  skuRate: Record<string, number> = SKU_DBU_RATE,
): { streamCosts: { etl: number; interactive: number; bisql: number }; totalCompute: number; details: RunDetail[] } {
  const a = withDefaults(assumptions);
  const utilFactor = a.utilization_factor;
  const overheadFactor = a.overhead_factor;
  const devTest = a.dev_test_uplift;
  const perfGain = a.photon_enabled === false ? 0.3 : (a.photon_perf_gain ?? 0.75);
  const discount = (a.discount_pct || 0) / 100;
  const mapByType = new Map(skuMapping.map((m) => [m.job_type, m]));

  const streamCosts = { etl: 0, interactive: 0, bisql: 0 };
  const details: RunDetail[] = [];
  let totalCompute = 0;

  for (const w of workloads) {
    const m = mapByType.get(w.job_type);
    let sku = m?.target_sku || DEFAULT_SKU;
    if (a.use_serverless && m?.target_sku_alt) sku = m.target_sku_alt;
    const cat = m?.compute_category || 'all_purpose';

    const mem = Number(w.total_memory_gb_hours || 0);
    const windowDbus = mem * utilFactor * overheadFactor * (1 + devTest) * (1 - perfGain);
    const estimatedDbus = windowDbus * annualizationFactor;
    const listPrice = skuRate[sku] ?? skuRate[DEFAULT_SKU];
    const effectivePrice = listPrice * (1 - discount);
    const annualCost = estimatedDbus * effectivePrice;

    streamCosts[categoryToStream(cat)] += annualCost;
    totalCompute += annualCost;

    details.push({
      job_type: w.job_type,
      target_sku: sku,
      total_apps: Math.trunc(Number(w.total_jobs || 0)),
      total_memory_gb_hours: mem,
      estimated_dbu_hours: round2(estimatedDbus),
      window_dbu_hours: Math.round(windowDbus * 10000) / 10000,
      dbu_effective_price: effectivePrice,
      estimated_cost: round2(annualCost),
    });
  }

  return {
    streamCosts: {
      etl: round2(streamCosts.etl),
      interactive: round2(streamCosts.interactive),
      bisql: round2(streamCosts.bisql),
    },
    totalCompute: round2(totalCompute),
    details,
  };
}

// Capacity-DBU constants extracted verbatim from the sheet's "Run-Rate
// Calculations" tab (m6id.2xlarge; 6 workers + 1 driver per cluster). The chain:
//   total_vcpus = nodes × vCores/node × split × util × (1+devtest)          [O17]
//   vcpus_req   = total_vcpus × vcpu_per_vcore × (1 − perf_gain)            [O15 = O17×O24]
//   clusters    = vcpus_req / (worker_nodes × vcpus/worker)                 [O29]
//   $DBU        = clusters × 8760 × (worker_nodes + driver_nodes) × $DBU/node/hr  [O74+O80]
const CAP = {
  workerNodesPerCluster: 6, // O32
  driverNodesPerCluster: 1, // O35
  vcpusPerWorker: 8, // O34 (m6id.2xlarge)
  // $DBU per node-hour = (DBU/hr/instance × $/DBU), by compute category, m6id.2xlarge.
  dbuDollarPerNodeHr: { jobs: 0.6612, all_purpose: 1.672, sql: 0.6612 } as Record<string, number>,
  biPerfGain: 0.36, // BI runtime perf gain (sheet)
};

// Serverless cost ratio applied to the Interactive + BI/SQL capacity streams when
// `use_serverless` is set (the Visa DPI profile runs those serverless). This is the
// documented GC-benchmark estimate (~0.53) and is OVERRIDABLE per assumption via
// `serverless_dbu_ratio`. NOTE: pending exact calibration against the sheet's
// serverless Run-Rate rows — treat as an estimate, not a reconciled figure. ETL
// (jobs) is unaffected; it does not run serverless in the profile.
export const SERVERLESS_DBU_RATIO = 0.53;

/**
 * CAPACITY DBU (TODO #6) — faithful port of the sheet's top-down cluster model.
 * Reproduces the sheet's ETL $DBU exactly ($141,594 for Visa) and the
 * non-serverless worker+driver chain for every stream.
 *
 * SERVERLESS: when `use_serverless` is set, the Interactive + BI/SQL streams run
 * serverless (as in the Visa profile) and their capacity cost is scaled by
 * SERVERLESS_DBU_RATIO (overridable via `serverless_dbu_ratio`). With serverless
 * off (the default) this returns the non-serverless worker+driver capacity cost,
 * so existing behavior is unchanged. ETL (jobs) is never scaled.
 */
export function computeCapacityDbu(
  assumptions: Assumptions,
): { streamCosts: { etl: number; interactive: number; bisql: number }; totalCompute: number } {
  const a = withDefaults(assumptions);
  const totalVCores = (a.hadoop_node_count || 0) * (a.hadoop_vcores_per_node || 0);
  const util = (a.hadoop_utilization_pct || 0) / 100;
  const devtest = a.dev_test_uplift; // O22
  const vcpuPerVcore = a.hyperthreading_factor || 1; // O25 (sheet C44 = 1)
  const photonPerf = a.photon_perf_gain ?? 0.75;

  // Serverless scaling for the interactive + BI/SQL streams (ETL unaffected).
  const serverlessRatio = a.use_serverless ? (a.serverless_dbu_ratio ?? SERVERLESS_DBU_RATIO) : 1;

  const streams = [
    { key: 'etl' as const, pct: (a.etl_pct || 0) / 100, perf: photonPerf, cat: 'jobs', serverless: 1 },
    { key: 'interactive' as const, pct: (a.interactive_pct || 0) / 100, perf: photonPerf, cat: 'all_purpose', serverless: serverlessRatio },
    { key: 'bisql' as const, pct: (a.bisql_pct || 0) / 100, perf: CAP.biPerfGain, cat: 'sql', serverless: serverlessRatio },
  ];
  const streamCosts = { etl: 0, interactive: 0, bisql: 0 };
  for (const s of streams) {
    const totalVcpus = totalVCores * s.pct * util * (1 + devtest);
    const vcpusReq = totalVcpus * vcpuPerVcore * (1 - s.perf);
    const clusters = vcpusReq / (CAP.workerNodesPerCluster * CAP.vcpusPerWorker);
    const dbuPerNodeHr = CAP.dbuDollarPerNodeHr[s.cat] ?? CAP.dbuDollarPerNodeHr.jobs;
    const cost = clusters * HOURS_PER_YEAR * (CAP.workerNodesPerCluster + CAP.driverNodesPerCluster) * dbuPerNodeHr * s.serverless;
    streamCosts[s.key] = round2(cost);
  }
  return {
    streamCosts,
    totalCompute: round2(streamCosts.etl + streamCosts.interactive + streamCosts.bisql),
  };
}

// ── VM compute (port of vm_costs.py arithmetic; cluster config + rates passed in) ──
export interface StreamClusterConfig {
  clusters: number;
  workers_per_cluster: number;
  hours_per_day: number;
}
export function computeVmCost(
  streamClusters: Record<string, StreamClusterConfig>,
  workerRate: number,
  driverRate: number,
): number {
  let total = 0;
  for (const cfg of Object.values(streamClusters)) {
    const annualHours = cfg.hours_per_day * 365;
    total += cfg.clusters * cfg.workers_per_cluster * workerRate * annualHours; // workers
    total += cfg.clusters * 1 * driverRate * annualHours; // 1 driver/cluster
  }
  return round2(total);
}

export function priceKeyForDiscount(t: string): 'on_demand_price' | 'reserved_price' | 'spot_price' {
  if (t === 'reserved') return 'reserved_price';
  if (t === 'spot') return 'spot_price';
  return 'on_demand_price';
}
export { VM_DISCOUNTS };

// ── Storage (port of storage.py tiered arithmetic) ───────────────────────────
// hdfsUsedGb from profiler cm_hdfs_usage (0 for Ambari clusters), OR manualTotalTb
// (the sheet treats storage as a manual input — e.g. Visa's 34,000 TB).
export function computeStorageCost(
  opts: {
    hdfsUsedGb?: number;
    manualTotalTb?: number;
    hdfsReplFactor: number;
    deltaCompression: number;
    hotPct: number;
    coldPct: number;
    archivePct: number;
    discountPct: number;
    tierPrice: { hot: number; cold: number; archive: number };
  },
): { delta_storage_gb: number; annual_cost: number } {
  // Manual total = the Delta storage to price directly (no replication/compression
  // adjustment); otherwise derive from HDFS used ÷ replication × compression.
  let deltaGb: number;
  if (opts.manualTotalTb && opts.manualTotalTb > 0) {
    deltaGb = opts.manualTotalTb * 1024;
  } else {
    const used = opts.hdfsUsedGb || 0;
    if (used === 0) return { delta_storage_gb: 0, annual_cost: 0 };
    deltaGb = (used / opts.hdfsReplFactor) * opts.deltaCompression;
  }
  const discount = opts.discountPct / 100;
  let monthly = 0;
  for (const [tier, pct] of [['hot', opts.hotPct], ['cold', opts.coldPct], ['archive', opts.archivePct]] as const) {
    const tierGb = deltaGb * (pct / 100);
    monthly += tierGb * opts.tierPrice[tier] * (1 - discount);
  }
  return { delta_storage_gb: round2(deltaGb), annual_cost: round2(monthly * 12) };
}

// ── Support + admin (trivial derived costs) ──────────────────────────────────
export function computeSupportCost(totalCompute: number, assumptions: Assumptions): number {
  const a = withDefaults(assumptions);
  return round2(totalCompute * ((a.dbx_support_pct || 25) / 100));
}
export function computeDbxAdminCost(hadoopAdminCost: number, assumptions: Assumptions): number {
  const a = withDefaults(assumptions);
  return round2(hadoopAdminCost * ((a.dbx_admin_overhead_pct || 30) / 100));
}

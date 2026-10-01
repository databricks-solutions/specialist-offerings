// TCO orchestrator — port of cost_engine.py's calculate_tco(). Ties together:
// Lakebase (assumptions, SKU mapping, lookups, run persistence) + warehouse
// (workload + observation window via the analytics plugin) + the pure cost modules.

import { randomUUID } from 'node:crypto';
import { sql } from '@databricks/appkit';
import type {
  Assumptions, CalculateRequest, TcoResult, ObservationWindow, HadoopCosts,
} from '../../shared/tco-types';
import { SCHEMA, type LakebaseQuery } from '../db/schema';
import { DAYS_PER_YEAR, MIN_WINDOW_DAYS, HOURS_PER_YEAR, withDefaults } from './constants';
import { calculateHadoopCosts } from './hadoop';
import {
  computeMeasuredDbu, computeCapacityDbu, computeVmCost, computeStorageCost,
  computeSupportCost, computeDbxAdminCost, priceKeyForDiscount,
  type WorkloadRow, type SkuMappingRow,
} from './databricks';
import { calculateMigrationTimeline, calculateDoNothing, summarizeTimeline } from './migration';

// Bound SQL parameter marker (what sql.string()/sql.int()/… return).
type QueryParam = ReturnType<typeof sql.string>;

// Minimal shape of the appkit context we use (lakebase + analytics server APIs).
export interface EngineAppKit {
  lakebase: { query: LakebaseQuery };
  analytics: { query: (q: string, params?: Record<string, QueryParam | null | undefined>) => Promise<unknown> };
}

const round2 = (n: number) => Math.round(n * 100) / 100;
const num = (v: unknown, d = 0): number => (v == null || Number.isNaN(Number(v)) ? d : Number(v));

/** Run a warehouse query (via the analytics plugin) and return rows as objects. */
async function warehouse(appkit: EngineAppKit, q: string, params?: Record<string, QueryParam | null | undefined>): Promise<Record<string, unknown>[]> {
  const res = await appkit.analytics.query(q, params);
  // AppKit analytics returns { chunk_index, row_offset, row_count, data: [...rows] }.
  // Tolerate a bare array or a {rows} wrapper as well. Numeric cells arrive as strings.
  if (res && typeof res === 'object' && Array.isArray((res as { data?: unknown[] }).data)) {
    return (res as { data: Record<string, unknown>[] }).data;
  }
  if (Array.isArray(res)) return res as Record<string, unknown>[];
  if (res && typeof res === 'object' && Array.isArray((res as { rows?: unknown[] }).rows)) {
    return (res as { rows: Record<string, unknown>[] }).rows;
  }
  return [];
}

function identifier(table: string): string {
  return `IDENTIFIER(:catalog || '.' || :database || '.${table}')`;
}

/** Measure the profiler observation window → annualization factor (port of get_observation_window_days). */
async function getObservationWindow(appkit: EngineAppKit, catalog: string, database: string): Promise<ObservationWindow> {
  const fallback: ObservationWindow = { window_days: 1, distinct_days: 0, span_days: 0, annualization_factor: DAYS_PER_YEAR, floored: true };
  try {
    const rows = await warehouse(
      appkit,
      `SELECT COUNT(DISTINCT to_date(from_unixtime(started_time/1000))) AS distinct_days,
              (MAX(finished_time)-MIN(started_time))/1000.0/86400 AS span_days,
              COUNT(*) AS apps
       FROM ${identifier('yarn_applications')}
       WHERE started_time IS NOT NULL AND started_time > 0`,
      { catalog: sql.string(catalog), database: sql.string(database) },
    );
    const r = rows[0];
    if (!r || r.distinct_days == null) return fallback;
    const distinctDays = Math.trunc(num(r.distinct_days));
    const spanDays = num(r.span_days);
    const measured = Math.max(distinctDays, spanDays);
    const windowDays = Math.max(measured, MIN_WINDOW_DAYS);
    return {
      window_days: Math.round(windowDays * 10000) / 10000,
      distinct_days: distinctDays,
      span_days: Math.round(spanDays * 10000) / 10000,
      annualization_factor: Math.round((DAYS_PER_YEAR / windowDays) * 10000) / 10000,
      floored: measured < MIN_WINDOW_DAYS,
    };
  } catch {
    return fallback;
  }
}

export async function calculateTco(appkit: EngineAppKit, req: CalculateRequest): Promise<TcoResult> {
  const { catalog, database } = req;

  // 1. Load the assumption set from Lakebase.
  const aRows = await appkit.lakebase.query(`SELECT * FROM ${SCHEMA}.assumptions WHERE assumption_id = $1`, [req.assumption_id]);
  if (aRows.rows.length === 0) throw new Error(`Assumption set not found: ${req.assumption_id}`);
  const assumptions = withDefaults(aRows.rows[0] as Assumptions);
  const dbuMethod = assumptions.dbu_method ?? 'measured';

  // 2. Load SKU mapping from Lakebase.
  const skuRows = await appkit.lakebase.query(`SELECT job_type, target_sku, target_sku_alt, compute_category FROM ${SCHEMA}.workload_sku_mapping`);
  const skuMapping: SkuMappingRow[] = skuRows.rows.map((r) => ({
    job_type: String(r.job_type),
    target_sku: String(r.target_sku),
    target_sku_alt: typeof r.target_sku_alt === 'string' ? r.target_sku_alt : null,
    compute_category: String(r.compute_category),
  }));

  // 3. Hadoop on-prem costs (assumption-driven).
  const hadoopCosts: HadoopCosts = calculateHadoopCosts(assumptions);
  const hadoopAnnual = req.hadoop_cost_annual && req.hadoop_cost_annual > 0 ? req.hadoop_cost_annual : hadoopCosts.total;

  // 4. Observation window + profiler workload (warehouse).
  const window = await getObservationWindow(appkit, catalog, database);
  const workloadRaw = await warehouse(
    appkit,
    `SELECT job_type, total_jobs, total_memory_gb_hours FROM ${identifier('workload_summary_by_type')} ORDER BY total_memory_gb_hours DESC`,
    { catalog: sql.string(catalog), database: sql.string(database) },
  );
  const workloadRows: WorkloadRow[] = workloadRaw.map((r) => ({
    job_type: String(r.job_type),
    total_jobs: num(r.total_jobs),
    total_memory_gb_hours: num(r.total_memory_gb_hours),
  }));

  // 5. Databricks DBU — measured (profiler-driven) or capacity (sheet-style).
  const dbu = dbuMethod === 'capacity'
    ? { ...computeCapacityDbu(assumptions), details: [] as TcoResult['details'] }
    : computeMeasuredDbu(workloadRows, skuMapping, assumptions, window.annualization_factor);
  const streamCosts = dbu.streamCosts;
  const totalCompute = dbu.totalCompute;
  const details = 'details' in dbu ? dbu.details : [];

  // 6. VM compute — size clusters from workload (port of _stream_clusters) × lookup rates.
  const vmCost = await computeVmForRun(appkit, assumptions, workloadRows, catalog, database);

  // 7. Storage — tiered, from HDFS metrics or manual TB override.
  const storageAnnual = await computeStorageForRun(appkit, assumptions, catalog, database);

  // 8. Support + admin.
  const supportCost = computeSupportCost(totalCompute, assumptions);
  const adminCost = computeDbxAdminCost(hadoopCosts.admin_cost, assumptions);

  const totalDbx = round2(totalCompute + vmCost + storageAnnual + supportCost + adminCost);
  const savingsPct = hadoopAnnual > 0 ? round2((1 - totalDbx / hadoopAnnual) * 100 * 10) / 10 : null;

  // 9. Migration timeline.
  const timeline = calculateMigrationTimeline(assumptions, hadoopAnnual, totalDbx);
  const doNothing = calculateDoNothing(hadoopAnnual);
  const timelineSummary = summarizeTimeline(timeline, doNothing);

  // 10. Persist the run.
  const runId = randomUUID();
  await persistRun(appkit, runId, req, {
    hadoopCosts, hadoopAnnual, streamCosts, vmCost, storageAnnual, supportCost, adminCost,
    totalDbx, savingsPct, window, timelineSummary,
  });
  await persistDetails(appkit, runId, details);
  await persistTimeline(appkit, runId, timeline);

  return {
    run_id: runId,
    run_name: req.run_name ?? '',
    assumption_id: req.assumption_id,
    hadoop_costs: hadoopCosts,
    hadoop_annual: round2(hadoopAnnual),
    stream_dbu_costs: streamCosts,
    total_compute_cost_annual: totalCompute,
    vm_cost_annual: vmCost,
    total_storage_cost_annual: storageAnnual,
    dbx_support_cost: supportCost,
    dbx_admin_cost: adminCost,
    total_dbx_annual: totalDbx,
    savings_pct: savingsPct,
    dbu_method: dbuMethod,
    observation_window: window,
    timeline,
    timeline_summary: timelineSummary,
    details,
  };
}

// ── VM sizing + rate lookup (port of cost_engine.py _stream_clusters + vm_costs.py) ──
async function computeVmForRun(
  appkit: EngineAppKit, a: ReturnType<typeof withDefaults>, workloads: WorkloadRow[], _catalog: string, _database: string,
): Promise<number> {
  const totalVcores = workloads.reduce((s, w) => s + num(w.total_memory_gb_hours), 0);
  const devTest = a.dev_test_uplift;
  const ht = a.hyperthreading_factor || 2;
  const perfGain = a.photon_perf_gain ?? 0.75;
  const cloud = a.target_cloud ?? 'AWS';

  // Worker/driver instance rates from the Lakebase lookup.
  const priceKey = priceKeyForDiscount(a.vm_discount_type || 'on_demand');
  const wRow = (await appkit.lakebase.query(
    `SELECT ${priceKey} AS rate, vcpus FROM ${SCHEMA}.lookup_vm_instances WHERE instance_type=$1 AND cloud=$2 LIMIT 1`,
    [a.worker_instance_type, cloud],
  )).rows[0];
  const dRow = (await appkit.lakebase.query(
    `SELECT ${priceKey} AS rate FROM ${SCHEMA}.lookup_vm_instances WHERE instance_type=$1 AND cloud=$2 LIMIT 1`,
    [a.driver_instance_type, cloud],
  )).rows[0];
  const workerRate = num(wRow?.rate);
  const driverRate = num(dRow?.rate);
  const vcpusPerWorker = Math.max(1, Math.trunc(num(wRow?.vcpus, 8)));

  const streamClusters = (pct: number, hoursPerDay: number) => {
    const streamVcores = (totalVcores * pct * (1 + devTest)) / ht * (1 - perfGain);
    const workers = Math.max(1, Math.trunc(streamVcores / vcpusPerWorker / hoursPerDay / 365));
    const clusters = Math.max(1, Math.trunc(workers / 4));
    return { clusters, workers_per_cluster: Math.min(workers, 4), hours_per_day: hoursPerDay };
  };
  const config = {
    etl: streamClusters((a.etl_pct || 40) / 100, 24),
    interactive: streamClusters((a.interactive_pct || 30) / 100, 12),
  };
  let vm = computeVmCost(config, workerRate, driverRate);

  // BI/SQL VM via DBSQL lookup (serverless → $0).
  if ((a.dbsql_type || 'pro') !== 'serverless') {
    const dbsql = (await appkit.lakebase.query(
      `SELECT vm_cost_per_hour FROM ${SCHEMA}.lookup_dbsql_sizes WHERE size_name=$1 AND cloud=$2 LIMIT 1`,
      [a.dbsql_warehouse_size, cloud],
    )).rows[0];
    vm += round2(num(dbsql?.vm_cost_per_hour) * HOURS_PER_YEAR);
  }
  return round2(vm);
}

// ── Storage (port of storage.py tiered calc) ─────────────────────────────────
async function computeStorageForRun(
  appkit: EngineAppKit, a: ReturnType<typeof withDefaults>, catalog: string, database: string,
): Promise<number> {
  const cloud = a.target_cloud ?? 'AWS';
  // HDFS used GB from profiler cm_hdfs_usage (0 for Ambari clusters).
  let hdfsUsedGb = 0;
  try {
    const rows = await warehouse(
      appkit,
      `SELECT MAX(CASE WHEN metric_name LIKE '%capacity_used%' THEN value END)/1e9 AS used_gb
       FROM ${identifier('cm_hdfs_usage')}`,
      { catalog: sql.string(catalog), database: sql.string(database) },
    );
    hdfsUsedGb = num(rows[0]?.used_gb);
  } catch {
    hdfsUsedGb = 0;
  }

  // Tier prices from the Lakebase lookup (fallback to defaults).
  const tierRows = (await appkit.lakebase.query(
    `SELECT tier_name, MIN(price_per_gb) AS price FROM ${SCHEMA}.lookup_storage_tiers WHERE cloud=$1 GROUP BY tier_name`,
    [cloud],
  )).rows;
  const tierPrice = { hot: 0.023, cold: 0.0125, archive: 0.004 };
  for (const t of tierRows) {
    const name = String(t.tier_name) as 'hot' | 'cold' | 'archive';
    if (name in tierPrice) tierPrice[name] = num(t.price, tierPrice[name]);
  }

  const res = computeStorageCost({
    hdfsUsedGb,
    manualTotalTb: num(a.storage_total_tb),
    hdfsReplFactor: a.hdfs_repl_factor || 3,
    deltaCompression: a.delta_compression || 0.5,
    hotPct: a.hot_storage_pct, coldPct: a.cold_storage_pct, archivePct: a.archive_storage_pct,
    discountPct: a.storage_discount_pct,
    tierPrice,
  });
  return res.annual_cost;
}

// ── Persistence ───────────────────────────────────────────────────────────────
async function persistRun(
  appkit: EngineAppKit, runId: string, req: CalculateRequest,
  r: {
    hadoopCosts: HadoopCosts; hadoopAnnual: number; streamCosts: { etl: number; interactive: number; bisql: number };
    vmCost: number; storageAnnual: number; supportCost: number; adminCost: number; totalDbx: number;
    savingsPct: number | null; window: ObservationWindow; timelineSummary: TcoResult['timeline_summary'];
  },
) {
  await appkit.lakebase.query(
    `INSERT INTO ${SCHEMA}.runs
      (run_id, run_name, assumption_id, profiler_catalog, profiler_schema,
       total_hadoop_cost_annual, total_databricks_cost_annual, total_storage_cost_annual,
       total_cost_annual, savings_pct, hadoop_license_cost, hadoop_support_cost,
       hadoop_hardware_cost, hadoop_datacenter_cost, hadoop_admin_cost,
       dbx_etl_dbu_cost, dbx_interactive_dbu_cost, dbx_bisql_dbu_cost, dbx_vm_cost,
       dbx_support_cost, dbx_admin_cost, migration_cost_total, three_year_hadoop_total,
       three_year_databricks_total, three_year_savings, observation_window_days,
       annualization_factor, window_floored, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,'app')`,
    [
      runId, req.run_name ?? '', req.assumption_id, req.catalog, req.database,
      r.hadoopAnnual, r.totalDbx, r.storageAnnual, r.totalDbx, r.savingsPct,
      r.hadoopCosts.license_cost, r.hadoopCosts.support_cost, r.hadoopCosts.hardware_cost,
      r.hadoopCosts.datacenter_cost, r.hadoopCosts.admin_cost,
      r.streamCosts.etl, r.streamCosts.interactive, r.streamCosts.bisql, r.vmCost,
      r.supportCost, r.adminCost, r.timelineSummary.three_year_migration_portion,
      r.timelineSummary.do_nothing_total, r.timelineSummary.three_year_total, r.timelineSummary.net_savings,
      r.window.window_days, r.window.annualization_factor, r.window.floored,
    ],
  );
}

async function persistDetails(appkit: EngineAppKit, runId: string, details: TcoResult['details']) {
  for (const d of details) {
    await appkit.lakebase.query(
      `INSERT INTO ${SCHEMA}.run_details
        (run_id, job_type, target_sku, total_apps, total_memory_gb_hours, total_vcore_hours,
         estimated_dbu_hours, window_dbu_hours, dbu_effective_price, estimated_cost)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [runId, d.job_type, d.target_sku, d.total_apps, d.total_memory_gb_hours, d.total_memory_gb_hours,
       d.estimated_dbu_hours, d.window_dbu_hours, d.dbu_effective_price, d.estimated_cost],
    );
  }
}

async function persistTimeline(appkit: EngineAppKit, runId: string, timeline: TcoResult['timeline']) {
  for (const q of timeline) {
    await appkit.lakebase.query(
      `INSERT INTO ${SCHEMA}.migration_timeline
        (run_id, quarter, quarter_label, migration_pct, hadoop_cost, databricks_cost,
         migration_cost, total_cost, can_turn_off_hadoop)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [runId, q.quarter, q.quarter_label, q.migration_pct, q.hadoop_cost, q.databricks_cost,
       q.migration_cost, q.total_cost, q.can_turn_off_hadoop],
    );
  }
}

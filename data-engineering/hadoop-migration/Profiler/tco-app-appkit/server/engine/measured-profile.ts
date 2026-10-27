// Measured-profile reference — reads what the profiler actually recorded for a
// catalog.schema so the Assumptions UI can show "measured: X [use]" hints beside the
// (manual) inputs. Every source query is individually guarded: CDH/CM clusters surface
// everything; Ambari/HDP clusters (empty cm_hosts / yarn_nodes / HDFS) simply omit the
// fields they can't support, leaving them manual. Nothing here auto-classifies the
// ETL/Interactive/BI split — `engine_mix` is reference-only (the split stays a human call).

import { sql } from '@databricks/appkit';
import { type EngineAppKit, warehouse, identifier, getObservationWindow } from './cost-engine';

type QueryParam = ReturnType<typeof sql.string>;

export interface EngineMixRow {
  application_type: string;
  apps: number;
  mem_gb_hours: number;
  pct: number;
}

export interface MeasuredProfile {
  /** From cm_hosts (CDH). Omitted when unavailable (e.g. Ambari). */
  node_count?: number;
  vcores_per_node?: number;
  /** HDFS used, TB, from cm_hdfs_usage. Omitted when unavailable (Ambari). */
  storage_tb?: number;
  /** Σ allocated vCore-hours over the window — lets the client compute a utilization
   *  hint against whatever node_count × vCores/node is in the form. */
  alloc_vcore_hours?: number;
  /** Observation window in days (wall-clock span, floored) for the utilization denominator. */
  window_days: number;
  /** Workload by YARN application_type (TEZ/SPARK/MAPREDUCE/…), % of memory-GB-hours. Reference only. */
  engine_mix: EngineMixRow[];
}

const num = (v: unknown, d = 0): number => (v == null || Number.isNaN(Number(v)) ? d : Number(v));

/**
 * Best-effort measured profile for `catalog.schema`. Never throws for a missing table —
 * each probe is guarded and simply omits its field.
 */
export async function getMeasuredProfile(
  appkit: EngineAppKit,
  catalog: string,
  database: string,
): Promise<MeasuredProfile> {
  const params: Record<string, QueryParam> = { catalog: sql.string(catalog), database: sql.string(database) };
  const out: MeasuredProfile = { window_days: 1, engine_mix: [] };

  // Observation window (already guarded internally → fallback window on failure).
  try {
    const w = await getObservationWindow(appkit, catalog, database);
    out.window_days = w.window_days;
  } catch { /* keep default */ }

  // Node count + vCores/node from Cloudera Manager host inventory (CDH only).
  try {
    const rows = await warehouse(
      appkit,
      `SELECT COUNT(*) AS n, ROUND(AVG(num_cores), 0) AS vc FROM ${identifier('cm_hosts')}`,
      params,
    );
    const n = num(rows[0]?.n);
    if (n > 0) {
      out.node_count = n;
      const vc = num(rows[0]?.vc);
      if (vc > 0) out.vcores_per_node = vc;
    }
  } catch { /* omit */ }

  // Σ allocated vCore-hours from YARN apps (works for Ambari too).
  try {
    const rows = await warehouse(
      appkit,
      `SELECT SUM(vcore_seconds) / 3600.0 AS h FROM ${identifier('yarn_applications')} WHERE started_time > 0`,
      params,
    );
    const h = num(rows[0]?.h);
    if (h > 0) out.alloc_vcore_hours = Math.round(h);
  } catch { /* omit */ }

  // HDFS used → TB from Cloudera Manager (CDH only).
  try {
    const rows = await warehouse(
      appkit,
      `SELECT MAX(CASE WHEN metric_name LIKE '%capacity_used%' THEN value END) / 1e12 AS tb FROM ${identifier('cm_hdfs_usage')}`,
      params,
    );
    const tb = num(rows[0]?.tb);
    if (tb > 0) out.storage_tb = Math.round(tb);
  } catch { /* omit */ }

  // Engine mix by YARN application_type (reference only — the real split signal; job_type
  // collapses to "Other"). % of memory-GB-hours.
  try {
    const rows = await warehouse(
      appkit,
      `SELECT application_type, COUNT(*) AS apps, SUM(memory_seconds) / 3600.0 / 1024.0 AS mem
         FROM ${identifier('yarn_applications')}
        GROUP BY application_type ORDER BY mem DESC`,
      params,
    );
    const total = rows.reduce((s, r) => s + num(r.mem), 0) || 1;
    out.engine_mix = rows.map((r) => {
      const mem = num(r.mem);
      return {
        application_type: typeof r.application_type === 'string' && r.application_type ? r.application_type : '—',
        apps: Math.trunc(num(r.apps)),
        mem_gb_hours: Math.round(mem),
        pct: Math.round((mem / total) * 1000) / 10,
      };
    });
  } catch { /* omit */ }

  return out;
}

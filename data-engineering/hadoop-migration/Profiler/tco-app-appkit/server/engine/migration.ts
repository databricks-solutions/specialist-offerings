// 3-year migration timeline — port of the Dash app's models/migration_timeline.py,
// extended with the full T-shirt sizing lookup (TODO #7): per-size migration cost,
// timeline, and FTEs, plus node-count → size auto-mapping.

import type { Assumptions, TimelineQuarter, TimelineSummary } from '../../shared/tco-types';
import { MIGRATION_TSHIRT, tshirtForNodeCount, type TshirtSize } from './constants';

const round2 = (n: number) => Math.round(n * 100) / 100;
const round4 = (n: number) => Math.round(n * 10000) / 10000;

/**
 * Resolve the effective T-shirt sizing for a run. If `migration_tshirt` is unset
 * ('auto') and a node count is known, bucket it; 'custom' uses migration_custom_cost.
 * Returns cost + the per-size timeline/FTE metadata (#7).
 */
export function resolveTshirt(a: Assumptions): { size: keyof typeof MIGRATION_TSHIRT; meta: TshirtSize; cost: number; ftes: number; timeline_months: number } {
  let size = a.migration_tshirt;
  if (!size && a.hadoop_node_count) size = tshirtForNodeCount(a.hadoop_node_count);
  if (!size) size = 'medium';
  const meta = MIGRATION_TSHIRT[size];
  const cost = size === 'custom' ? Number(a.migration_custom_cost || 0) : meta.cost;
  return { size, meta, cost, ftes: meta.ftes, timeline_months: meta.timeline_months };
}

export function calculateMigrationTimeline(
  assumptions: Assumptions,
  hadoopAnnual: number,
  databricksAnnual: number,
): TimelineQuarter[] {
  // Duration: explicit quarters override; else derive from the T-shirt timeline (#7).
  const tshirt = resolveTshirt(assumptions);
  let duration = Number(
    assumptions.migration_duration_quarters ?? Math.ceil(tshirt.timeline_months / 3),
  );
  duration = Math.max(1, Math.min(12, duration));

  const ecifCredit = Number(assumptions.ecif_credit || 0);
  const migrationTotal = Math.max(0, tshirt.cost - ecifCredit);
  const migrationPerQuarter = duration > 0 ? migrationTotal / duration : 0;
  const hadoopQuarterly = hadoopAnnual / 4;
  const databricksQuarterly = databricksAnnual / 4;

  const timeline: TimelineQuarter[] = [];
  for (let q = 1; q <= 12; q++) {
    const year = Math.floor((q - 1) / 4) + 1;
    const qtr = ((q - 1) % 4) + 1;
    const migrationPct = q <= duration ? q / duration : 1.0;
    const hCost = hadoopQuarterly * (1 - migrationPct);
    const dCost = databricksQuarterly * migrationPct;
    const mCost = q <= duration ? migrationPerQuarter : 0;
    timeline.push({
      quarter: q,
      quarter_label: `Q${qtr} Y${year}`,
      migration_pct: round4(migrationPct),
      hadoop_cost: round2(hCost),
      databricks_cost: round2(dCost),
      migration_cost: round2(mCost),
      total_cost: round2(hCost + dCost + mCost),
      can_turn_off_hadoop: migrationPct >= 1.0,
    });
  }
  return timeline;
}

export function calculateDoNothing(hadoopAnnual: number): { annual_cost: number; quarterly_cost: number; three_year_total: number } {
  return {
    annual_cost: round2(hadoopAnnual),
    quarterly_cost: round2(hadoopAnnual / 4),
    three_year_total: round2(hadoopAnnual * 3),
  };
}

export function summarizeTimeline(
  timeline: TimelineQuarter[],
  doNothing: ReturnType<typeof calculateDoNothing>,
): TimelineSummary {
  const threeYearTotal = timeline.reduce((s, q) => s + q.total_cost, 0);
  const threeYearHadoop = timeline.reduce((s, q) => s + q.hadoop_cost, 0);
  const threeYearDatabricks = timeline.reduce((s, q) => s + q.databricks_cost, 0);
  const threeYearMigration = timeline.reduce((s, q) => s + q.migration_cost, 0);
  const doNothingTotal = doNothing.three_year_total;
  const netSavings = doNothingTotal - threeYearTotal;

  let cumulative = 0;
  let doNothingCumulative = 0;
  let paybackQuarter: number | null = null;
  for (const q of timeline) {
    cumulative += q.total_cost;
    doNothingCumulative += doNothing.quarterly_cost;
    if (paybackQuarter === null && cumulative < doNothingCumulative) paybackQuarter = q.quarter;
  }

  return {
    three_year_total: round2(threeYearTotal),
    three_year_hadoop_portion: round2(threeYearHadoop),
    three_year_databricks_portion: round2(threeYearDatabricks),
    three_year_migration_portion: round2(threeYearMigration),
    do_nothing_total: round2(doNothingTotal),
    net_savings: round2(netSavings),
    savings_pct: doNothingTotal > 0 ? round2((netSavings / doNothingTotal) * 100 * 10) / 10 : 0,
    payback_quarter: paybackQuarter,
  };
}

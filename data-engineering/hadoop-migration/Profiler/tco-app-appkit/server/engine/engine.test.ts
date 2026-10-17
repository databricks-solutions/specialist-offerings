import { describe, it, expect } from 'vitest';
import { calculateHadoopCosts } from './hadoop';
import { tshirtForNodeCount, MIGRATION_TSHIRT } from './constants';
import { resolveTshirt, calculateMigrationTimeline, calculateDoNothing, summarizeTimeline } from './migration';
import type { Assumptions } from '../../shared/tco-types';

// Visa DPI sheet inputs (Inputs & Assumptions tab).
const visaDpi: Assumptions = {
  hadoop_vendor_type: 'Open Source',
  hadoop_node_count: 643,
  hadoop_vcores_per_node: 79,
  hadoop_datacenter_per_node: 5000, // sheet: 643 × $5k = $3,215,000
  hadoop_hardware_per_node: 2196, // back-solved from sheet HW $1,412,031
  hadoop_admin_count: 15,
  hadoop_admin_salary: 250_000, // 15 × $250k = $3,750,000
  migration_tshirt: 'custom',
  migration_custom_cost: 3_000_000,
  migration_duration_quarters: 8,
};

describe('Hadoop cost reconciliation vs the Visa sheet', () => {
  const h = calculateHadoopCosts(visaDpi);

  it('license + support are $0 for OSS', () => {
    expect(h.license_cost).toBe(0);
    expect(h.support_cost).toBe(0);
  });

  it('datacenter matches the sheet exactly ($3,215,000)', () => {
    expect(h.datacenter_cost).toBe(3_215_000);
  });

  it('admin matches the sheet exactly ($3,750,000)', () => {
    expect(h.admin_cost).toBe(3_750_000);
  });

  it('total reconciles to the sheet ($8,377,031 ± rounding)', () => {
    // Sheet total 8,377,031; HW per-node rounding gives 8,377,028.
    expect(Math.abs(h.total - 8_377_031)).toBeLessThanOrEqual(10);
  });
});

describe('T-shirt sizing (TODO #7)', () => {
  it('buckets node count to size per the sheet', () => {
    expect(tshirtForNodeCount(40)).toBe('small');
    expect(tshirtForNodeCount(100)).toBe('medium');
    expect(tshirtForNodeCount(300)).toBe('large');
    expect(tshirtForNodeCount(643)).toBe('custom');
  });

  it('carries per-size timeline + FTEs', () => {
    expect(MIGRATION_TSHIRT.large.timeline_months).toBe(11);
    expect(MIGRATION_TSHIRT.large.ftes).toBe(7);
    expect(MIGRATION_TSHIRT.custom.ftes).toBe(9);
  });

  it('auto-maps node count when tshirt unset', () => {
    const r = resolveTshirt({ hadoop_node_count: 300 });
    expect(r.size).toBe('large');
    expect(r.cost).toBe(1_750_000);
    expect(r.timeline_months).toBe(11);
  });

  it('custom uses migration_custom_cost', () => {
    const r = resolveTshirt(visaDpi);
    expect(r.size).toBe('custom');
    expect(r.cost).toBe(3_000_000);
  });
});

describe('Migration timeline', () => {
  const hadoopAnnual = 8_377_028;
  const dbxAnnual = 13_540_181;
  const timeline = calculateMigrationTimeline(visaDpi, hadoopAnnual, dbxAnnual);
  const doNothing = calculateDoNothing(hadoopAnnual);
  const summary = summarizeTimeline(timeline, doNothing);

  it('produces 12 quarters, fully migrated by the duration', () => {
    expect(timeline).toHaveLength(12);
    expect(timeline[7].migration_pct).toBe(1); // 8 quarters duration → Q8 = 100%
    expect(timeline[11].can_turn_off_hadoop).toBe(true);
  });

  it('do-nothing is 3× annual Hadoop', () => {
    expect(doNothing.three_year_total).toBe(round2(hadoopAnnual * 3));
  });

  it('summary net savings = do-nothing − 3yr total', () => {
    expect(summary.net_savings).toBe(round2(summary.do_nothing_total - summary.three_year_total));
  });
});

const round2 = (n: number) => Math.round(n * 100) / 100;

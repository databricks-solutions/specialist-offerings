import { describe, it, expect } from 'vitest';
import {
  computeMeasuredDbu,
  computeCapacityDbu,
  computeStorageCost,
  computeSupportCost,
  computeDbxAdminCost,
  type WorkloadRow,
  type SkuMappingRow,
} from './databricks';
import type { Assumptions } from '../../shared/tco-types';

const skuMap: SkuMappingRow[] = [
  { job_type: 'Other', target_sku: 'PREMIUM_ALL_PURPOSE_COMPUTE', target_sku_alt: null, compute_category: 'all_purpose' },
  { job_type: 'MapReduce', target_sku: 'PREMIUM_JOBS_COMPUTE', target_sku_alt: null, compute_category: 'jobs' },
];

describe('Measured DBU (port of cost_engine.py loop)', () => {
  const wl: WorkloadRow[] = [{ job_type: 'Other', total_jobs: 10, total_memory_gb_hours: 100 }];
  // defaults: util 0.9, overhead 1.1, dev_test 0.2, perf_gain 0.75 → factor 0.297
  it('applies util×overhead×(1+dev)×(1−perf)×annualization×price', () => {
    const r = computeMeasuredDbu(wl, skuMap, {}, 1);
    // 100 × 0.9 × 1.1 × 1.2 × 0.25 = 29.7 window DBUs; × $0.55 = 16.335
    expect(r.details[0].window_dbu_hours).toBeCloseTo(29.7, 4);
    expect(r.details[0].estimated_cost).toBeCloseTo(16.34, 2);
    expect(r.streamCosts.interactive).toBeCloseTo(16.34, 2);
  });

  it('annualizes by the window factor', () => {
    const r = computeMeasuredDbu(wl, skuMap, {}, 10);
    expect(r.details[0].estimated_cost).toBeCloseTo(163.35, 1);
  });

  it('applies contract discount', () => {
    const r = computeMeasuredDbu(wl, skuMap, { discount_pct: 10 }, 1);
    expect(r.details[0].estimated_cost).toBeCloseTo(14.7, 2); // 29.7 × 0.495
  });

  it('routes jobs-category to the ETL stream at $0.15', () => {
    const r = computeMeasuredDbu([{ job_type: 'MapReduce', total_jobs: 5, total_memory_gb_hours: 100 }], skuMap, {}, 1);
    expect(r.streamCosts.etl).toBeCloseTo(29.7 * 0.15, 2);
  });
});

describe('Capacity DBU (#6) — reconciles to the sheet Run-Rate chain', () => {
  const visa: Assumptions = {
    hadoop_node_count: 643, hadoop_vcores_per_node: 79,
    hadoop_utilization_pct: 6, etl_pct: 20, interactive_pct: 40, bisql_pct: 40,
    photon_perf_gain: 0.75, hyperthreading_factor: 1, dev_test_uplift: 0.1,
  };
  const r = computeCapacityDbu(visa);

  it('ETL $DBU matches the sheet exactly ($141,594)', () => {
    // 643×79×0.2×0.06×1.1 ×0.25 /(6×8) clusters × 8760 × 7 × $0.6612
    expect(Math.abs(r.streamCosts.etl - 141_594)).toBeLessThanOrEqual(10);
  });

  it('Interactive (non-serverless) matches the sheet chain (S74+S80 ≈ $716,109)', () => {
    expect(Math.abs(r.streamCosts.interactive - 716_109)).toBeLessThanOrEqual(50);
  });
});

describe('Storage arithmetic (port of storage.py tiered calc)', () => {
  it('manual total TB (no compression, all hot) = TB×1024×rate×12', () => {
    const r = computeStorageCost({
      manualTotalTb: 34000, hdfsReplFactor: 3, deltaCompression: 1.0,
      hotPct: 100, coldPct: 0, archivePct: 0, discountPct: 0,
      tierPrice: { hot: 0.026, cold: 0.0125, archive: 0.004 },
    });
    // 34000 × 1024 × 0.026 × 12 = 10,862,592
    expect(r.annual_cost).toBeCloseTo(10_862_592, 0);
  });

  it('returns $0 when no HDFS data and no manual total (Ambari case)', () => {
    const r = computeStorageCost({
      hdfsUsedGb: 0, hdfsReplFactor: 3, deltaCompression: 0.5,
      hotPct: 70, coldPct: 20, archivePct: 10, discountPct: 0,
      tierPrice: { hot: 0.023, cold: 0.0125, archive: 0.004 },
    });
    expect(r.annual_cost).toBe(0);
  });
});

describe('Support + admin derived costs', () => {
  it('support = compute × dbx_support_pct', () => {
    expect(computeSupportCost(1_000_000, { dbx_support_pct: 25 })).toBe(250_000);
  });
  it('dbx admin = hadoop admin × overhead — matches the sheet ($1,125,000)', () => {
    expect(computeDbxAdminCost(3_750_000, { dbx_admin_overhead_pct: 30 })).toBe(1_125_000);
  });
});

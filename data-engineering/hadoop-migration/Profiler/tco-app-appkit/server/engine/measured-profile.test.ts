import { describe, it, expect } from 'vitest';
import { getMeasuredProfile } from './measured-profile';
import type { EngineAppKit } from './cost-engine';

// Mock appkit whose analytics.query routes by SQL substring; returning an Error
// simulates a missing table (the guarded probe should omit that field).
function mockAppkit(route: (q: string) => Record<string, unknown>[] | Error): EngineAppKit {
  return {
    lakebase: { query: (() => Promise.resolve({ rows: [] })) as EngineAppKit['lakebase']['query'] },
    analytics: {
      query: (q: string) => {
        const r = route(q);
        return r instanceof Error ? Promise.reject(r) : Promise.resolve({ data: r });
      },
    },
  };
}

describe('getMeasuredProfile', () => {
  it('CDH: surfaces node/vCores/storage/alloc-hours/window + engine mix', async () => {
    const route = (q: string): Record<string, unknown>[] => {
      if (q.includes('distinct_days')) return [{ distinct_days: 11, span_days: 26.8, apps: 5178 }];
      if (q.includes('cm_hosts')) return [{ n: 643, vc: 79 }];
      if (q.includes('vcore_seconds')) return [{ h: 456574 }];
      if (q.includes('cm_hdfs_usage')) return [{ tb: 34000 }];
      if (q.includes('application_type')) return [
        { application_type: 'TEZ', apps: 3860, mem: 8059675 },
        { application_type: 'SPARK', apps: 213, mem: 236 },
      ];
      return [];
    };
    const m = await getMeasuredProfile(mockAppkit(route), 'profiler', 'cdh');
    expect(m.node_count).toBe(643);
    expect(m.vcores_per_node).toBe(79);
    expect(m.storage_tb).toBe(34000);
    expect(m.alloc_vcore_hours).toBe(456574);
    expect(m.window_days).toBeCloseTo(26.8, 1);
    expect(m.engine_mix).toHaveLength(2);
    expect(m.engine_mix[0].application_type).toBe('TEZ');
    expect(m.engine_mix[0].pct).toBeGreaterThan(99); // TEZ dominates
    expect(m.engine_mix.reduce((s, e) => s + e.pct, 0)).toBeCloseTo(100, 0);
  });

  it('Ambari: omits fields whose source tables are missing, without throwing', async () => {
    const route = (q: string): Record<string, unknown>[] | Error => {
      if (q.includes('distinct_days')) return [{ distinct_days: 11, span_days: 26.8, apps: 5178 }];
      if (q.includes('cm_hosts')) return new Error('[DELTA_TABLE_NOT_FOUND] cm_hosts');
      if (q.includes('vcore_seconds')) return [{ h: 456574 }];
      if (q.includes('cm_hdfs_usage')) return new Error('[DELTA_TABLE_NOT_FOUND] cm_hdfs_usage');
      if (q.includes('application_type')) return [{ application_type: 'TEZ', apps: 3860, mem: 8059675 }];
      return [];
    };
    const m = await getMeasuredProfile(mockAppkit(route), 'profiler', 'visa_dpi_mar');
    expect(m.node_count).toBeUndefined();
    expect(m.vcores_per_node).toBeUndefined();
    expect(m.storage_tb).toBeUndefined();
    expect(m.alloc_vcore_hours).toBe(456574); // YARN-derived, available for Ambari
    expect(m.engine_mix).toHaveLength(1);
    expect(m.window_days).toBeGreaterThan(0);
  });

  it('empty schema: returns a valid shape (window default, empty mix), no throw', async () => {
    const m = await getMeasuredProfile(mockAppkit(() => []), 'profiler', 'empty');
    expect(m.node_count).toBeUndefined();
    expect(m.alloc_vcore_hours).toBeUndefined();
    expect(m.engine_mix).toEqual([]);
    expect(m.window_days).toBeGreaterThanOrEqual(1);
  });
});

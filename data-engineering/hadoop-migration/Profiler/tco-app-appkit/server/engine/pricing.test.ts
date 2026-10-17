import { describe, it, expect, beforeEach } from 'vitest';
import { getPricingSnapshot, _resetPricingCache } from './pricing';
import { SKU_DBU_RATE } from './databricks';

beforeEach(() => _resetPricingCache());

describe('Pricing snapshot (system.billing.list_prices)', () => {
  it('overrides matched SKUs with live prices (exact + region-prefix), lowest wins', async () => {
    const rows = [
      { sku_name: 'PREMIUM_ALL_PURPOSE_COMPUTE', usd: 0.6 },
      { sku_name: 'PREMIUM_ALL_PURPOSE_COMPUTE_US_EAST_1', usd: 0.58 }, // prefix, lower → wins
      { sku_name: 'PREMIUM_JOBS_COMPUTE', usd: 0.3 },
    ];
    const snap = await getPricingSnapshot(() => Promise.resolve(rows));
    expect(snap.source).toBe('live');
    expect(snap.rates.PREMIUM_ALL_PURPOSE_COMPUTE).toBeCloseTo(0.58, 4);
    expect(snap.rates.PREMIUM_JOBS_COMPUTE).toBeCloseTo(0.3, 4);
    expect(snap.refreshed).toContain('PREMIUM_ALL_PURPOSE_COMPUTE');
    // Unmatched keys keep their static rate.
    expect(snap.rates.SERVERLESS_SQL_COMPUTE).toBe(SKU_DBU_RATE.SERVERLESS_SQL_COMPUTE);
  });

  it('does not cross-match sibling SKUs (jobs vs jobs-serverless)', async () => {
    const rows = [{ sku_name: 'PREMIUM_JOBS_SERVERLESS_COMPUTE', usd: 0.9 }];
    const snap = await getPricingSnapshot(() => Promise.resolve(rows));
    expect(snap.rates.PREMIUM_JOBS_SERVERLESS_COMPUTE).toBeCloseTo(0.9, 4);
    expect(snap.rates.PREMIUM_JOBS_COMPUTE).toBe(SKU_DBU_RATE.PREMIUM_JOBS_COMPUTE); // untouched
  });

  it('falls back to static rates when the query throws (no grant / offline)', async () => {
    const snap = await getPricingSnapshot(() => Promise.reject(new Error('no grant')));
    expect(snap.source).toBe('static');
    expect(snap.rates).toEqual(SKU_DBU_RATE);
    expect(snap.refreshed).toHaveLength(0);
  });

  it('falls back to static when no rows are returned', async () => {
    const snap = await getPricingSnapshot(() => Promise.resolve([]));
    expect(snap.source).toBe('static');
  });

  it('ignores non-positive / non-numeric prices', async () => {
    const rows = [
      { sku_name: 'PREMIUM_SQL_COMPUTE', usd: 0 },
      { sku_name: 'PREMIUM_SQL_COMPUTE_US_WEST_2', usd: 'n/a' },
    ];
    const snap = await getPricingSnapshot(() => Promise.resolve(rows));
    expect(snap.source).toBe('static');
    expect(snap.rates.PREMIUM_SQL_COMPUTE).toBe(SKU_DBU_RATE.PREMIUM_SQL_COMPUTE);
  });
});

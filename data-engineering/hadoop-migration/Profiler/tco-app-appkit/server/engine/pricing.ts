// Live pricing snapshot — reads current $/DBU per SKU from the UC system table
// `system.billing.list_prices` and overrides the static SKU_DBU_RATE used by the
// measured-DBU path. Falls back to the static rates if the table is unreadable
// (the app SP may lack SELECT on `system.billing`, or we may be offline).
//
// Scope: this feeds the MEASURED DBU path (per-SKU $/DBU). Capacity mode prices
// by $DBU/node-hour (DBU-per-instance-hour × $/DBU) and is not re-derived here.

import { SKU_DBU_RATE } from './databricks';

export interface PricingSnapshot {
  /** $/DBU keyed by our internal SKU names (same keys as SKU_DBU_RATE). */
  rates: Record<string, number>;
  source: 'live' | 'static';
  /** SKU keys that were refreshed from the live table (empty when static). */
  refreshed: string[];
  fetched_at: string;
}

/** A no-arg warehouse runner (the orchestrator passes a bound `warehouse(appkit, q)`). */
export type PriceQuery = (sql: string) => Promise<Record<string, unknown>[]>;

const TTL_MS = 60 * 60 * 1000; // 1h — list_prices changes rarely; avoid per-run hits.
let cache: { snapshot: PricingSnapshot; at: number } | null = null;

/** Reset the memo (tests). */
export function _resetPricingCache(): void {
  cache = null;
}

const num = (v: unknown): number | null => {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

/**
 * Match a list_prices `sku_name` to one of our internal SKU keys. list_prices
 * names are canonical (e.g. `PREMIUM_ALL_PURPOSE_COMPUTE`) but may carry a region
 * suffix (`..._US_EAST_1`) or a `(PHOTON)` qualifier — accept exact or prefix.
 */
function matchesKey(skuName: string, key: string): boolean {
  if (skuName === key) return true;
  return skuName.startsWith(`${key}_`) || skuName.startsWith(`${key}(`);
}

/**
 * Fetch current $/DBU per SKU from system.billing.list_prices. Returns a snapshot
 * whose `rates` is a complete map (live where matched, static otherwise). Never
 * throws — on any failure it returns the static rates with source:'static'.
 */
export async function getPricingSnapshot(query: PriceQuery): Promise<PricingSnapshot> {
  if (cache && Date.now() - cache.at < TTL_MS) return cache.snapshot;

  const staticSnapshot = (): PricingSnapshot => ({
    rates: { ...SKU_DBU_RATE },
    source: 'static',
    refreshed: [],
    fetched_at: new Date().toISOString(),
  });

  try {
    // Current list prices only (price_end_time IS NULL), DBU usage unit. `pricing`
    // is a struct; `pricing.default` is the undiscounted list $/DBU.
    const rows = await query(
      `SELECT sku_name, CAST(pricing.default AS DOUBLE) AS usd
         FROM system.billing.list_prices
        WHERE usage_unit = 'DBU' AND price_end_time IS NULL`,
    );
    if (!rows.length) {
      cache = { snapshot: staticSnapshot(), at: Date.now() };
      return cache.snapshot;
    }

    const rates: Record<string, number> = { ...SKU_DBU_RATE };
    const refreshed: string[] = [];
    for (const key of Object.keys(SKU_DBU_RATE)) {
      // Among matching rows, take the lowest current price (conservative; also
      // collapses multi-region duplicates deterministically).
      let best: number | null = null;
      for (const r of rows) {
        const sku = typeof r.sku_name === 'string' ? r.sku_name : '';
        if (!matchesKey(sku, key)) continue;
        const usd = num(r.usd);
        if (usd == null || usd <= 0) continue;
        best = best == null ? usd : Math.min(best, usd);
      }
      if (best != null) {
        rates[key] = best;
        refreshed.push(key);
      }
    }

    const snapshot: PricingSnapshot = {
      rates,
      source: refreshed.length ? 'live' : 'static',
      refreshed,
      fetched_at: new Date().toISOString(),
    };
    cache = { snapshot, at: Date.now() };
    return snapshot;
  } catch {
    // Table unreadable (no grant / offline) — keep static rates.
    const snapshot = staticSnapshot();
    cache = { snapshot, at: Date.now() };
    return snapshot;
  }
}

// Read routes for TCO reference data (lookups, SKU mapping) + run history.
// Run *creation* lands in Phase 2 with the TS cost engine; these are the
// read/edit endpoints the UI needs for the Pricing & SKU Mapping and results pages.

import { z } from 'zod';
import { Application } from 'express';
import { SCHEMA, type LakebaseQuery } from '../../db/schema';

interface AppKitWithLakebase {
  lakebase: { query: LakebaseQuery };
  server: { extend(fn: (app: Application) => void): void };
}

const SkuMappingBody = z.object({
  target_sku: z.string().min(1),
  target_sku_alt: z.string().nullable().optional(),
  compute_category: z.enum(['jobs', 'sql', 'all_purpose', 'serverless_sql']),
  notes: z.string().optional(),
});

function cloudFilter(req: { query: Record<string, unknown> }): string | null {
  const c = req.query.cloud;
  return typeof c === 'string' && c ? c.toUpperCase() : null;
}

export function registerReferenceRoutes(appkit: AppKitWithLakebase) {
  const { query } = appkit.lakebase;

  appkit.server.extend((app) => {
    // ── Lookups ──────────────────────────────────────────────────────────────
    app.get('/api/tco/lookups/vm-instances', async (req, res) => {
      try {
        const cloud = cloudFilter(req);
        const { rows } = cloud
          ? await query(`SELECT * FROM ${SCHEMA}.lookup_vm_instances WHERE cloud = $1 ORDER BY vcpus`, [cloud])
          : await query(`SELECT * FROM ${SCHEMA}.lookup_vm_instances ORDER BY cloud, vcpus`);
        res.json(rows);
      } catch (err) {
        console.error('Failed to list VM instances:', err);
        res.status(500).json({ error: 'Failed to list VM instances' });
      }
    });

    app.get('/api/tco/lookups/dbsql-sizes', async (req, res) => {
      try {
        const cloud = cloudFilter(req);
        const { rows } = cloud
          ? await query(`SELECT * FROM ${SCHEMA}.lookup_dbsql_sizes WHERE cloud = $1 ORDER BY dbu_per_hour`, [cloud])
          : await query(`SELECT * FROM ${SCHEMA}.lookup_dbsql_sizes ORDER BY cloud, dbu_per_hour`);
        res.json(rows);
      } catch (err) {
        console.error('Failed to list DBSQL sizes:', err);
        res.status(500).json({ error: 'Failed to list DBSQL sizes' });
      }
    });

    app.get('/api/tco/lookups/storage-tiers', async (req, res) => {
      try {
        const cloud = cloudFilter(req);
        const { rows } = cloud
          ? await query(`SELECT * FROM ${SCHEMA}.lookup_storage_tiers WHERE cloud = $1 ORDER BY tier_name, volume_min_tb`, [cloud])
          : await query(`SELECT * FROM ${SCHEMA}.lookup_storage_tiers ORDER BY cloud, tier_name, volume_min_tb`);
        res.json(rows);
      } catch (err) {
        console.error('Failed to list storage tiers:', err);
        res.status(500).json({ error: 'Failed to list storage tiers' });
      }
    });

    // ── Workload → SKU mapping ─────────────────────────────────────────────────
    app.get('/api/tco/sku-mapping', async (_req, res) => {
      try {
        const { rows } = await query(
          `SELECT job_type, target_sku, target_sku_alt, compute_category, notes, updated_at
             FROM ${SCHEMA}.workload_sku_mapping ORDER BY job_type`,
        );
        res.json(rows);
      } catch (err) {
        console.error('Failed to list SKU mapping:', err);
        res.status(500).json({ error: 'Failed to list SKU mapping' });
      }
    });

    app.put('/api/tco/sku-mapping/:jobType', async (req, res) => {
      const parsed = SkuMappingBody.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'Invalid body' });
        return;
      }
      try {
        // Upsert so the UI can edit existing or add new job-type mappings.
        const { target_sku, target_sku_alt = null, compute_category, notes = null } = parsed.data;
        const { rows } = await query(
          `INSERT INTO ${SCHEMA}.workload_sku_mapping (job_type, target_sku, target_sku_alt, compute_category, notes, updated_at)
           VALUES ($1, $2, $3, $4, $5, now())
           ON CONFLICT (job_type) DO UPDATE SET
             target_sku = EXCLUDED.target_sku,
             target_sku_alt = EXCLUDED.target_sku_alt,
             compute_category = EXCLUDED.compute_category,
             notes = EXCLUDED.notes,
             updated_at = now()
           RETURNING *`,
          [req.params.jobType, target_sku, target_sku_alt, compute_category, notes],
        );
        res.json(rows[0]);
      } catch (err) {
        console.error('Failed to upsert SKU mapping:', err);
        res.status(500).json({ error: 'Failed to update SKU mapping' });
      }
    });

    // ── Runs (read) ────────────────────────────────────────────────────────────
    app.get('/api/tco/runs', async (_req, res) => {
      try {
        const { rows } = await query(
          `SELECT run_id, run_name, total_hadoop_cost_annual, total_databricks_cost_annual,
                  total_cost_annual, savings_pct, created_at
             FROM ${SCHEMA}.runs ORDER BY created_at DESC`,
        );
        res.json(rows);
      } catch (err) {
        console.error('Failed to list runs:', err);
        res.status(500).json({ error: 'Failed to list runs' });
      }
    });

    app.get('/api/tco/runs/:id', async (req, res) => {
      try {
        const run = await query(`SELECT * FROM ${SCHEMA}.runs WHERE run_id = $1`, [req.params.id]);
        if (run.rows.length === 0) {
          res.status(404).json({ error: 'Run not found' });
          return;
        }
        const details = await query(
          `SELECT * FROM ${SCHEMA}.run_details WHERE run_id = $1 ORDER BY estimated_cost DESC`,
          [req.params.id],
        );
        const timeline = await query(
          `SELECT * FROM ${SCHEMA}.migration_timeline WHERE run_id = $1 ORDER BY quarter`,
          [req.params.id],
        );
        res.json({ ...run.rows[0], details: details.rows, timeline: timeline.rows });
      } catch (err) {
        console.error('Failed to get run:', err);
        res.status(500).json({ error: 'Failed to get run' });
      }
    });
  });
}

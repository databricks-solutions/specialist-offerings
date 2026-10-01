// CRUD routes for TCO assumption sets (tco.assumptions).
// Modeled on the scaffold's sample lakebase todo-routes pattern:
// parameterized appkit.lakebase.query + zod validation + Express routes.

import { z } from 'zod';
import { Application } from 'express';
import { SCHEMA, type LakebaseQuery } from '../../db/schema';

interface AppKitWithLakebase {
  lakebase: { query: LakebaseQuery };
  server: { extend(fn: (app: Application) => void): void };
}

// Writable assumption fields (everything except the server-managed id/created_at).
// All optional so the UI can save partial sets; defaults live in the cost engine.
const AssumptionFields = z
  .object({
    name: z.string().min(1),
    target_cloud: z.enum(['AWS', 'AZURE', 'GCP']),
    databricks_tier: z.enum(['STANDARD', 'PREMIUM', 'ENTERPRISE']),
    use_serverless: z.boolean(),
    photon_enabled: z.boolean(),
    utilization_factor: z.number(),
    overhead_factor: z.number(),
    discount_pct: z.number(),
    vm_mem_gb: z.number(),
    hdfs_repl_factor: z.number().int(),
    delta_compression: z.number(),
    storage_cost_per_gb_month: z.number(),
    hadoop_vendor_type: z.enum(['Licensed', 'Open Source']),
    hadoop_node_count: z.number().int(),
    hadoop_vcores_per_node: z.number().int(),
    hadoop_utilization_pct: z.number(),
    hadoop_license_per_node: z.number(),
    hadoop_license_discount: z.number(),
    hadoop_support_pct: z.number(),
    hadoop_hardware_per_node: z.number(),
    hadoop_datacenter_per_node: z.number(),
    hadoop_admin_count: z.number().int(),
    hadoop_admin_salary: z.number(),
    dev_test_uplift: z.number(),
    hyperthreading_factor: z.number(),
    photon_perf_gain: z.number(),
    etl_pct: z.number(),
    interactive_pct: z.number(),
    bisql_pct: z.number(),
    vm_discount_type: z.enum(['on_demand', 'reserved', 'spot']),
    worker_instance_type: z.string(),
    driver_instance_type: z.string(),
    dbsql_warehouse_size: z.string(),
    dbsql_type: z.enum(['classic', 'pro', 'serverless']),
    dbsql_utilization: z.number(),
    storage_discount_pct: z.number(),
    hot_storage_pct: z.number(),
    cold_storage_pct: z.number(),
    archive_storage_pct: z.number(),
    storage_total_tb: z.number(),
    dbx_support_pct: z.number(),
    dbx_admin_overhead_pct: z.number(),
    migration_tshirt: z.enum(['small', 'medium', 'large', 'custom']),
    migration_custom_cost: z.number(),
    ecif_credit: z.number(),
    migration_duration_quarters: z.number().int(),
    dbu_method: z.enum(['measured', 'capacity']),
  })
  .partial();

// Create requires a name; update is fully partial.
const AssumptionBody = AssumptionFields.refine((b) => b.name !== undefined, {
  message: 'name is required',
});

const WRITABLE_FIELDS = Object.keys(AssumptionFields.shape);

export function registerAssumptionsRoutes(appkit: AppKitWithLakebase) {
  const { query } = appkit.lakebase;

  appkit.server.extend((app) => {
    // List (summary columns for the picker/panel)
    app.get('/api/tco/assumptions', async (_req, res) => {
      try {
        const { rows } = await query(
          `SELECT assumption_id, name, target_cloud, databricks_tier, use_serverless,
                  hadoop_node_count, hadoop_vcores_per_node, discount_pct, created_at
             FROM ${SCHEMA}.assumptions ORDER BY created_at DESC`,
        );
        res.json(rows);
      } catch (err) {
        console.error('Failed to list assumptions:', err);
        res.status(500).json({ error: 'Failed to list assumptions' });
      }
    });

    // Get one (full row)
    app.get('/api/tco/assumptions/:id', async (req, res) => {
      try {
        const { rows } = await query(
          `SELECT * FROM ${SCHEMA}.assumptions WHERE assumption_id = $1`,
          [req.params.id],
        );
        if (rows.length === 0) {
          res.status(404).json({ error: 'Assumption set not found' });
          return;
        }
        res.json(rows[0]);
      } catch (err) {
        console.error('Failed to get assumption:', err);
        res.status(500).json({ error: 'Failed to get assumption set' });
      }
    });

    // Create
    app.post('/api/tco/assumptions', async (req, res) => {
      const parsed = AssumptionBody.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'Invalid body' });
        return;
      }
      try {
        const entries = Object.entries(parsed.data);
        const cols = entries.map(([k]) => k).concat('created_by');
        const vals = entries.map(([, v]) => v).concat('app');
        const placeholders = cols.map((_, i) => `$${i + 1}`);
        const { rows } = await query(
          `INSERT INTO ${SCHEMA}.assumptions (${cols.join(', ')})
           VALUES (${placeholders.join(', ')}) RETURNING *`,
          vals,
        );
        res.status(201).json(rows[0]);
      } catch (err) {
        console.error('Failed to create assumption:', err);
        res.status(500).json({ error: 'Failed to create assumption set' });
      }
    });

    // Update (partial)
    app.put('/api/tco/assumptions/:id', async (req, res) => {
      const parsed = AssumptionFields.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'Invalid body' });
        return;
      }
      const entries = Object.entries(parsed.data).filter(([k]) => WRITABLE_FIELDS.includes(k));
      if (entries.length === 0) {
        res.status(400).json({ error: 'No updatable fields provided' });
        return;
      }
      try {
        const setClause = entries.map(([k], i) => `${k} = $${i + 2}`).join(', ');
        const vals = entries.map(([, v]) => v);
        const { rows } = await query(
          `UPDATE ${SCHEMA}.assumptions SET ${setClause}
           WHERE assumption_id = $1 RETURNING *`,
          [req.params.id, ...vals],
        );
        if (rows.length === 0) {
          res.status(404).json({ error: 'Assumption set not found' });
          return;
        }
        res.json(rows[0]);
      } catch (err) {
        console.error('Failed to update assumption:', err);
        res.status(500).json({ error: 'Failed to update assumption set' });
      }
    });

    // Delete
    app.delete('/api/tco/assumptions/:id', async (req, res) => {
      try {
        const { rows } = await query(
          `DELETE FROM ${SCHEMA}.assumptions WHERE assumption_id = $1 RETURNING assumption_id`,
          [req.params.id],
        );
        if (rows.length === 0) {
          res.status(404).json({ error: 'Assumption set not found' });
          return;
        }
        res.status(204).end();
      } catch (err) {
        console.error('Failed to delete assumption:', err);
        res.status(500).json({ error: 'Failed to delete assumption set' });
      }
    });
  });
}

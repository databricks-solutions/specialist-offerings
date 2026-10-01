// POST /api/tco/calculate — runs a full TCO calculation via the cost engine and
// persists the run to tco.runs / run_details / migration_timeline.

import { z } from 'zod';
import { Application } from 'express';
import { calculateTco, type EngineAppKit } from '../../engine/cost-engine';

interface AppKitForCalc extends EngineAppKit {
  server: { extend(fn: (app: Application) => void): void };
}

const CalcBody = z.object({
  assumption_id: z.string().min(1),
  catalog: z.string().min(1),
  database: z.string().min(1),
  run_name: z.string().optional(),
  hadoop_cost_annual: z.number().optional(),
});

export function registerCalculateRoutes(appkit: AppKitForCalc) {
  appkit.server.extend((app) => {
    app.post('/api/tco/calculate', async (req, res) => {
      const parsed = CalcBody.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'Invalid body' });
        return;
      }
      try {
        const result = await calculateTco(appkit, parsed.data);
        res.status(201).json(result);
      } catch (err) {
        console.error('TCO calculation failed:', err);
        res.status(500).json({ error: (err as Error).message });
      }
    });
  });
}

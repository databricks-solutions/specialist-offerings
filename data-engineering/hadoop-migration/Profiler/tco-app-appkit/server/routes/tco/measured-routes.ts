// GET /api/tco/measured?catalog=&schema= — best-effort measured values from the
// profiler for a catalog.schema, used to show "measured: X [use]" hints on the
// Assumptions page. Never 500s on a missing table (the engine omits unavailable fields).

import { Application } from 'express';
import { getMeasuredProfile } from '../../engine/measured-profile';
import { type EngineAppKit } from '../../engine/cost-engine';

interface AppKitForMeasured extends EngineAppKit {
  server: { extend(fn: (app: Application) => void): void };
}

export function registerMeasuredRoutes(appkit: AppKitForMeasured) {
  appkit.server.extend((app) => {
    app.get('/api/tco/measured', async (req, res) => {
      const catalog = typeof req.query.catalog === 'string' ? req.query.catalog : '';
      const schema = typeof req.query.schema === 'string' ? req.query.schema : '';
      if (!catalog || !schema) {
        res.status(400).json({ error: 'catalog and schema are required' });
        return;
      }
      try {
        res.json(await getMeasuredProfile(appkit, catalog, schema));
      } catch (err) {
        console.error('Failed to read measured profile:', err);
        res.status(500).json({ error: 'Failed to read measured profile' });
      }
    });
  });
}

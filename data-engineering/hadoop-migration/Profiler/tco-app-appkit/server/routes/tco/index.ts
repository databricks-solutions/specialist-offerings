// TCO state layer entry point: provision the Lakebase schema (idempotent) and
// register the assumptions + reference/run routes. Called from onPluginsReady.

import { Application } from 'express';
import { setupTcoSchema, type LakebaseQuery } from '../../db/schema';
import { registerAssumptionsRoutes } from './assumptions-routes';
import { registerReferenceRoutes } from './reference-routes';

interface AppKitWithLakebase {
  lakebase: { query: LakebaseQuery };
  server: { extend(fn: (app: Application) => void): void };
}

export async function setupTco(appkit: AppKitWithLakebase): Promise<void> {
  try {
    await setupTcoSchema(appkit.lakebase.query);
    console.log(`[tco] schema ready (${'tco'}.*) — tables provisioned + lookups seeded`);
  } catch (err) {
    console.warn('[tco] schema setup failed:', (err as Error).message);
    console.warn('[tco] routes still registered; DB calls may error until Lakebase is reachable.');
    console.warn('[tco] See https://developers.databricks.com/docs/appkit/v0/plugins/lakebase#database-permissions');
  }
  registerAssumptionsRoutes(appkit);
  registerReferenceRoutes(appkit);
}

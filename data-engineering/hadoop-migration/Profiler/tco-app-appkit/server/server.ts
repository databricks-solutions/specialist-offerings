import { createApp, analytics, lakebase, server } from '@databricks/appkit';
import { setupTco } from './routes/tco';

createApp({
  plugins: [
    analytics(),
    lakebase(),
    server(),
  ],
  async onPluginsReady(appkit) {
    // Provision the TCO Postgres schema (idempotent) + register state routes.
    // This auto-setup replaces the Dash app's manual "Initialize TCO Tables".
    await setupTco(appkit);
  },
}).catch(console.error);

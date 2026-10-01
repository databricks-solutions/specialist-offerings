import { createBrowserRouter, RouterProvider, NavLink, Outlet } from 'react-router';
import { CatalogProvider, useCatalogSchema } from '@/lib/catalog';
import { CalculatorPage } from '@/pages/tco/CalculatorPage';
import { AssumptionsPage } from '@/pages/tco/AssumptionsPage';
import { WorkloadProfilePage } from '@/pages/tco/WorkloadProfilePage';
import { PricingSkuPage } from '@/pages/tco/PricingSkuPage';
import { MigrationTimelinePage } from '@/pages/tco/MigrationTimelinePage';
import { ScenarioComparisonPage } from '@/pages/tco/ScenarioComparisonPage';

const navLinkClass = ({ isActive }: { isActive: boolean }) =>
  `px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
    isActive ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
  }`;

function CatalogSelector() {
  const { catalog, database, setCatalog, setDatabase } = useCatalogSchema();
  return (
    <div className="ml-auto flex items-center gap-2 text-sm">
      <span className="text-muted-foreground hidden sm:inline">Source</span>
      <input aria-label="Catalog" className="h-8 w-28 rounded-md border px-2 bg-background font-mono text-xs"
        value={catalog} onChange={(e) => setCatalog(e.target.value)} />
      <span className="text-muted-foreground">.</span>
      <input aria-label="Schema" className="h-8 w-32 rounded-md border px-2 bg-background font-mono text-xs"
        value={database} onChange={(e) => setDatabase(e.target.value)} />
    </div>
  );
}

function Layout() {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="border-b px-4 md:px-6 py-3 flex items-center gap-4 flex-wrap">
        <h1 className="text-lg font-semibold text-foreground">Hadoop → Databricks TCO</h1>
        <nav className="flex gap-1 flex-wrap">
          <NavLink to="/" end className={navLinkClass}>Calculator</NavLink>
          <NavLink to="/workload" className={navLinkClass}>Workload</NavLink>
          <NavLink to="/pricing" className={navLinkClass}>Pricing &amp; SKU</NavLink>
          <NavLink to="/migration" className={navLinkClass}>Migration</NavLink>
          <NavLink to="/scenarios" className={navLinkClass}>Scenarios</NavLink>
          <NavLink to="/assumptions" className={navLinkClass}>Assumptions</NavLink>
        </nav>
        <CatalogSelector />
      </header>
      <main className="flex-1 p-4 md:p-6">
        <Outlet />
      </main>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <CalculatorPage /> },
      { path: '/workload', element: <WorkloadProfilePage /> },
      { path: '/pricing', element: <PricingSkuPage /> },
      { path: '/migration', element: <MigrationTimelinePage /> },
      { path: '/scenarios', element: <ScenarioComparisonPage /> },
      { path: '/assumptions', element: <AssumptionsPage /> },
    ],
  },
]);

export default function App() {
  return (
    <CatalogProvider>
      <RouterProvider router={router} />
    </CatalogProvider>
  );
}

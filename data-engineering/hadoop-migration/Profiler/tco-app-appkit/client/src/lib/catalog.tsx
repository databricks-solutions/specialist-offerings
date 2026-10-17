// Selected profiler catalog/schema, shared across pages (the sidebar selector
// in the Dash app). Persisted to localStorage so it survives reloads.

import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';

interface CatalogState {
  catalog: string;
  database: string;
  setCatalog: (c: string) => void;
  setDatabase: (d: string) => void;
}

const DEFAULTS = { catalog: 'profiler', database: 'visa_dpi_mar' };
const CatalogContext = createContext<CatalogState | null>(null);

function load(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key) || fallback;
  } catch {
    return fallback;
  }
}

export function CatalogProvider({ children }: { children: ReactNode }) {
  const [catalog, setCatalogState] = useState(() => load('tco.catalog', DEFAULTS.catalog));
  const [database, setDatabaseState] = useState(() => load('tco.database', DEFAULTS.database));

  const setCatalog = useCallback((c: string) => {
    setCatalogState(c);
    try { localStorage.setItem('tco.catalog', c); } catch { /* ignore */ }
  }, []);
  const setDatabase = useCallback((d: string) => {
    setDatabaseState(d);
    try { localStorage.setItem('tco.database', d); } catch { /* ignore */ }
  }, []);

  return (
    <CatalogContext.Provider value={{ catalog, database, setCatalog, setDatabase }}>
      {children}
    </CatalogContext.Provider>
  );
}

export function useCatalogSchema(): CatalogState {
  const ctx = useContext(CatalogContext);
  if (!ctx) throw new Error('useCatalogSchema must be used within CatalogProvider');
  return ctx;
}

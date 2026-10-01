// Typed client for the TCO backend routes (/api/tco/*).

import type { Assumptions, TcoResult, CalculateRequest } from '../../../shared/tco-types';

export interface AssumptionSummary {
  assumption_id: string;
  name: string;
  target_cloud: string;
  databricks_tier: string;
  use_serverless: boolean;
  hadoop_node_count: number | null;
  hadoop_vcores_per_node: number | null;
  discount_pct: number | null;
  created_at: string;
}

export interface RunSummary {
  run_id: string;
  run_name: string;
  total_hadoop_cost_annual: number;
  total_databricks_cost_annual: number;
  total_cost_annual: number;
  savings_pct: number | null;
  created_at: string;
}

export interface SkuMappingRow {
  job_type: string;
  target_sku: string;
  target_sku_alt: string | null;
  compute_category: string;
  notes: string | null;
}

async function unwrap<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string };
    throw new Error(body.error || `${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<T>;
}

const jsonInit = (method: string, body: unknown): RequestInit => ({
  method,
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
});

export interface VmInstanceRow {
  cloud: string; instance_type: string; vcpus: number; memory_gb: number;
  on_demand_price: number; reserved_price: number; spot_price: number; region: string; category: string;
}
export interface DbsqlSizeRow {
  size_name: string; worker_count: number; dbu_per_hour: number; vcpus: number; cloud: string; vm_cost_per_hour: number;
}
export interface StorageTierRow {
  cloud: string; tier_name: string; volume_min_tb: number; volume_max_tb: number; price_per_gb: number;
}

export const api = {
  listAssumptions: () => fetch('/api/tco/assumptions').then(unwrap<AssumptionSummary[]>),
  updateSkuMapping: (jobType: string, body: Partial<SkuMappingRow>) =>
    fetch(`/api/tco/sku-mapping/${encodeURIComponent(jobType)}`, jsonInit('PUT', body)).then(unwrap<SkuMappingRow>),
  vmInstances: (cloud: string) => fetch(`/api/tco/lookups/vm-instances?cloud=${cloud}`).then(unwrap<VmInstanceRow[]>),
  dbsqlSizes: (cloud: string) => fetch(`/api/tco/lookups/dbsql-sizes?cloud=${cloud}`).then(unwrap<DbsqlSizeRow[]>),
  storageTiers: (cloud: string) => fetch(`/api/tco/lookups/storage-tiers?cloud=${cloud}`).then(unwrap<StorageTierRow[]>),
  getAssumption: (id: string) => fetch(`/api/tco/assumptions/${id}`).then(unwrap<Assumptions>),
  createAssumption: (body: Assumptions) =>
    fetch('/api/tco/assumptions', jsonInit('POST', body)).then(unwrap<Assumptions>),
  updateAssumption: (id: string, body: Partial<Assumptions>) =>
    fetch(`/api/tco/assumptions/${id}`, jsonInit('PUT', body)).then(unwrap<Assumptions>),
  deleteAssumption: async (id: string) => {
    const res = await fetch(`/api/tco/assumptions/${id}`, { method: 'DELETE' });
    if (!res.ok && res.status !== 204) throw new Error(`${res.status} ${res.statusText}`);
  },
  calculate: (req: CalculateRequest) =>
    fetch('/api/tco/calculate', jsonInit('POST', req)).then(unwrap<TcoResult>),
  listRuns: () => fetch('/api/tco/runs').then(unwrap<RunSummary[]>),
  skuMapping: () => fetch('/api/tco/sku-mapping').then(unwrap<SkuMappingRow[]>),
};

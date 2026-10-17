import { useState, useEffect, useCallback } from 'react';
import {
  Button, Card, CardContent, CardHeader, CardTitle, Label,
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@databricks/appkit-ui/react';
import { api, type SkuMappingRow, type VmInstanceRow, type DbsqlSizeRow } from '@/lib/api';

const CATEGORIES = ['jobs', 'sql', 'all_purpose', 'serverless_sql'];

export function PricingSkuPage() {
  const [cloud, setCloud] = useState('AWS');
  const [mapping, setMapping] = useState<SkuMappingRow[]>([]);
  const [vm, setVm] = useState<VmInstanceRow[]>([]);
  const [dbsql, setDbsql] = useState<DbsqlSizeRow[]>([]);
  const [status, setStatus] = useState<string | null>(null);

  const refresh = useCallback(() => {
    api.skuMapping().then(setMapping).catch((e) => setStatus(`Error: ${(e as Error).message}`));
    api.vmInstances(cloud).then(setVm).catch(() => {});
    api.dbsqlSizes(cloud).then(setDbsql).catch(() => {});
  }, [cloud]);
  useEffect(() => { refresh(); }, [refresh]);

  const setRow = (jt: string, patch: Partial<SkuMappingRow>) =>
    setMapping((m) => m.map((r) => (r.job_type === jt ? { ...r, ...patch } : r)));

  async function save(r: SkuMappingRow) {
    try {
      await api.updateSkuMapping(r.job_type, {
        target_sku: r.target_sku, target_sku_alt: r.target_sku_alt,
        compute_category: r.compute_category, notes: r.notes ?? undefined,
      });
      setStatus(`Saved "${r.job_type}" at ${new Date().toLocaleTimeString()}`);
    } catch (e) {
      setStatus(`Error: ${(e as Error).message}`);
    }
  }

  return (
    <div className="space-y-6 w-full max-w-6xl mx-auto">
      <div className="flex items-center gap-4">
        <h2 className="text-2xl font-bold">Pricing &amp; SKU Mapping</h2>
        <div className="ml-auto flex items-center gap-2">
          <Label>Cloud</Label>
          <Select value={cloud} onValueChange={setCloud}>
            <SelectTrigger className="w-28"><SelectValue /></SelectTrigger>
            <SelectContent>{['AWS', 'AZURE', 'GCP'].map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}</SelectContent>
          </Select>
        </div>
      </div>
      {status && <div className="text-sm text-green-600">{status}</div>}

      <Card>
        <CardHeader><CardTitle>Workload → SKU mapping (edit to retune DBU allocation)</CardTitle></CardHeader>
        <CardContent>
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-muted-foreground border-b">
                <th className="py-2">Job type</th><th>Target SKU</th><th>Category</th><th></th>
              </tr>
            </thead>
            <tbody>
              {mapping.map((r) => (
                <tr key={r.job_type} className="border-b last:border-0">
                  <td className="py-2 font-medium">{r.job_type}</td>
                  <td>
                    <input className="w-72 h-8 rounded-md border px-2 text-xs font-mono bg-background"
                      value={r.target_sku} onChange={(e) => setRow(r.job_type, { target_sku: e.target.value })} />
                  </td>
                  <td>
                    <select className="h-8 rounded-md border px-2 text-xs bg-background"
                      value={r.compute_category} onChange={(e) => setRow(r.job_type, { compute_category: e.target.value })}>
                      {CATEGORIES.map((c) => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </td>
                  <td className="text-right"><Button variant="ghost" size="sm" onClick={() => void save(r)}>Save</Button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card>
          <CardHeader><CardTitle>VM Instances ({cloud})</CardTitle></CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead><tr className="text-left text-muted-foreground border-b"><th className="py-2">Type</th><th>vCPUs</th><th>Mem GB</th><th className="text-right">$/hr</th></tr></thead>
              <tbody>
                {vm.map((v) => (
                  <tr key={v.instance_type} className="border-b last:border-0">
                    <td className="py-1.5 font-mono text-xs">{v.instance_type}</td><td>{v.vcpus}</td><td>{v.memory_gb}</td><td className="text-right">${v.on_demand_price}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle>DBSQL Sizes ({cloud})</CardTitle></CardHeader>
          <CardContent>
            <table className="w-full text-sm">
              <thead><tr className="text-left text-muted-foreground border-b"><th className="py-2">Size</th><th>DBU/hr</th><th className="text-right">VM $/hr</th></tr></thead>
              <tbody>
                {dbsql.map((d) => (
                  <tr key={d.size_name} className="border-b last:border-0">
                    <td className="py-1.5">{d.size_name}</td><td>{d.dbu_per_hour}</td><td className="text-right">${d.vm_cost_per_hour}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

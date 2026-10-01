import { useState, useEffect } from 'react';
import {
  Button, Card, CardContent, CardHeader, CardTitle, Label,
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue, Skeleton,
} from '@databricks/appkit-ui/react';
import { api, type AssumptionSummary } from '@/lib/api';
import { useCatalogSchema } from '@/lib/catalog';
import type { TcoResult } from '../../../../shared/tco-types';

const usd = (n: number | null | undefined) =>
  n == null ? '—' : n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

function Stat({ label, value, accent }: { label: string; value: string; accent?: 'red' | 'green' | 'blue' }) {
  const color = accent === 'red' ? 'text-red-600' : accent === 'green' ? 'text-green-600' : accent === 'blue' ? 'text-blue-600' : 'text-foreground';
  return (
    <div className="rounded-lg border p-4">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={`text-2xl font-semibold ${color}`}>{value}</div>
    </div>
  );
}

export function CalculatorPage() {
  const { catalog, database } = useCatalogSchema();
  const [assumptions, setAssumptions] = useState<AssumptionSummary[]>([]);
  const [assumptionId, setAssumptionId] = useState<string>('');
  const [runName, setRunName] = useState('run 1');
  const [result, setResult] = useState<TcoResult | null>(null);
  const [running, setRunning] = useState(false);
  const [status, setStatus] = useState<{ kind: 'ok' | 'err'; msg: string } | null>(null);

  useEffect(() => {
    api.listAssumptions().then((a) => {
      setAssumptions(a);
      if (a.length && !assumptionId) setAssumptionId(a[0].assumption_id);
    }).catch((e) => setStatus({ kind: 'err', msg: String(e.message || e) }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function calculate() {
    if (!assumptionId) { setStatus({ kind: 'err', msg: 'Pick an assumption set first.' }); return; }
    setRunning(true);
    setStatus(null);
    try {
      const r = await api.calculate({ assumption_id: assumptionId, catalog, database, run_name: runName });
      setResult(r);
      setStatus({ kind: 'ok', msg: `Calculated at ${new Date().toLocaleTimeString()} on ${catalog}.${database} (run ${r.run_id.slice(0, 8)})` });
    } catch (e) {
      setStatus({ kind: 'err', msg: String((e as Error).message || e) });
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="space-y-6 w-full max-w-7xl mx-auto">
      <h2 className="text-2xl font-bold">TCO Calculator</h2>

      {/* Controls */}
      <Card>
        <CardContent className="pt-6 grid grid-cols-1 md:grid-cols-4 gap-4 items-end">
          <div className="md:col-span-2">
            <Label>Assumption set</Label>
            <Select value={assumptionId} onValueChange={setAssumptionId}>
              <SelectTrigger><SelectValue placeholder="Select an assumption set" /></SelectTrigger>
              <SelectContent>
                {assumptions.map((a) => (
                  <SelectItem key={a.assumption_id} value={a.assumption_id}>
                    {a.name} ({a.target_cloud}{a.hadoop_node_count ? `, ${a.hadoop_node_count} nodes` : ''})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label>Run name</Label>
            <input className="w-full h-9 rounded-md border px-3 text-sm bg-background"
              value={runName} onChange={(e) => setRunName(e.target.value)} />
          </div>
          <Button onClick={calculate} disabled={running}>
            {running ? 'Calculating…' : 'Calculate TCO'}
          </Button>
          <div className="md:col-span-4 text-xs text-muted-foreground">
            Source data: <span className="font-mono">{catalog}.{database}</span> (change in the header)
          </div>
          {status && (
            <div className={`md:col-span-4 text-sm ${status.kind === 'ok' ? 'text-green-600' : 'text-red-600'}`}>
              {status.msg}
            </div>
          )}
        </CardContent>
      </Card>

      {running && <Skeleton className="h-40 w-full" />}

      {result && !running && (
        <>
          <Card>
            <CardHeader><CardTitle>Hadoop On-Prem — Annual</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 md:grid-cols-6 gap-3">
              <Stat label="License" value={usd(result.hadoop_costs.license_cost)} />
              <Stat label="Support" value={usd(result.hadoop_costs.support_cost)} />
              <Stat label="Hardware" value={usd(result.hadoop_costs.hardware_cost)} />
              <Stat label="Datacenter" value={usd(result.hadoop_costs.datacenter_cost)} />
              <Stat label="Admin" value={usd(result.hadoop_costs.admin_cost)} />
              <Stat label="TOTAL" value={usd(result.hadoop_annual)} accent="red" />
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Databricks — Annual ({result.dbu_method} DBU)</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 md:grid-cols-7 gap-3">
              <Stat label="ETL DBU" value={usd(result.stream_dbu_costs.etl)} />
              <Stat label="Interactive DBU" value={usd(result.stream_dbu_costs.interactive)} />
              <Stat label="BI/SQL DBU" value={usd(result.stream_dbu_costs.bisql)} />
              <Stat label="VM Compute" value={usd(result.vm_cost_annual)} accent="blue" />
              <Stat label="Storage" value={usd(result.total_storage_cost_annual)} accent="blue" />
              <Stat label="Support" value={usd(result.dbx_support_cost)} />
              <Stat label="Admin" value={usd(result.dbx_admin_cost)} />
            </CardContent>
            <CardContent className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <Stat label="Hadoop Annual" value={usd(result.hadoop_annual)} accent="red" />
              <Stat label="Databricks Annual" value={usd(result.total_dbx_annual)} accent="green" />
              <Stat label="3-yr Net Savings" value={usd(result.timeline_summary.net_savings)} accent={result.timeline_summary.net_savings >= 0 ? 'green' : 'red'} />
              <Stat label="Savings vs Hadoop" value={result.savings_pct == null ? '—' : `${result.savings_pct}%`} accent={(result.savings_pct ?? 0) >= 0 ? 'green' : 'red'} />
            </CardContent>
          </Card>

          {result.observation_window.floored && (
            <div className="text-sm text-amber-600">
              ⚠ Low confidence: the profiler window is only {result.observation_window.window_days} day(s)
              (annualized ×{result.observation_window.annualization_factor}). Use a multi-day extract for a reliable measured-DBU figure.
            </div>
          )}

          {result.details.length > 0 && (
            <Card>
              <CardHeader><CardTitle>Per-Workload Breakdown</CardTitle></CardHeader>
              <CardContent>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-muted-foreground border-b">
                      <th className="py-2">Job type</th><th>Apps</th><th>GB-hours</th><th>SKU</th><th>DBU-hours</th><th className="text-right">Cost</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.details.map((d) => (
                      <tr key={d.job_type} className="border-b last:border-0">
                        <td className="py-2">{d.job_type}</td>
                        <td>{d.total_apps.toLocaleString()}</td>
                        <td>{Math.round(d.total_memory_gb_hours).toLocaleString()}</td>
                        <td className="font-mono text-xs">{d.target_sku}</td>
                        <td>{Math.round(d.estimated_dbu_hours).toLocaleString()}</td>
                        <td className="text-right">{usd(d.estimated_cost)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  );
}

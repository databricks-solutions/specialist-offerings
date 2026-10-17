import { useState, useEffect } from 'react';
import {
  Card, CardContent, CardHeader, CardTitle, Label,
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@databricks/appkit-ui/react';
import { api, type RunSummary, type RunDetailFull } from '@/lib/api';

const usd = (n: number | null | undefined) =>
  n == null ? '—' : n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

export function MigrationTimelinePage() {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [runId, setRunId] = useState('');
  const [run, setRun] = useState<RunDetailFull | null>(null);

  useEffect(() => {
    api.listRuns().then((r) => { setRuns(r); if (r.length && !runId) setRunId(r[0].run_id); }).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  useEffect(() => { if (runId) api.getRun(runId).then(setRun).catch(() => setRun(null)); }, [runId]);

  return (
    <div className="space-y-6 w-full max-w-5xl mx-auto">
      <div className="flex items-center gap-4 flex-wrap">
        <h2 className="text-2xl font-bold">Migration Timeline</h2>
        <div className="ml-auto flex items-center gap-2">
          <Label>Run</Label>
          <Select value={runId} onValueChange={setRunId}>
            <SelectTrigger className="w-72"><SelectValue placeholder="Select a run" /></SelectTrigger>
            <SelectContent>
              {runs.map((r) => <SelectItem key={r.run_id} value={r.run_id}>{r.run_name || r.run_id.slice(0, 8)} — {usd(r.total_cost_annual)}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
      </div>

      {run && (
        <>
          <Card>
            <CardHeader><CardTitle>3-Year Summary</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div className="rounded-lg border p-4"><div className="text-xs text-muted-foreground">Hadoop (do nothing)</div><div className="text-xl font-semibold text-red-600">{usd(run.total_hadoop_cost_annual * 3)}</div></div>
              <div className="rounded-lg border p-4"><div className="text-xs text-muted-foreground">Databricks annual</div><div className="text-xl font-semibold text-green-600">{usd(run.total_databricks_cost_annual)}</div></div>
              <div className="rounded-lg border p-4"><div className="text-xs text-muted-foreground">3-yr net savings</div><div className={`text-xl font-semibold ${run.three_year_savings >= 0 ? 'text-green-600' : 'text-red-600'}`}>{usd(run.three_year_savings)}</div></div>
              <div className="rounded-lg border p-4"><div className="text-xs text-muted-foreground">Savings vs Hadoop</div><div className="text-xl font-semibold">{run.savings_pct == null ? '—' : `${run.savings_pct}%`}</div></div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Quarterly Ramp</CardTitle></CardHeader>
            <CardContent>
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-muted-foreground border-b">
                    <th className="py-2">Quarter</th><th>Migrated %</th><th className="text-right">Hadoop</th><th className="text-right">Databricks</th><th className="text-right">Migration</th><th className="text-right">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {run.timeline.map((q) => (
                    <tr key={q.quarter} className="border-b last:border-0">
                      <td className="py-2">{q.quarter_label}</td>
                      <td>{Math.round(q.migration_pct * 100)}%</td>
                      <td className="text-right">{usd(q.hadoop_cost)}</td>
                      <td className="text-right">{usd(q.databricks_cost)}</td>
                      <td className="text-right">{usd(q.migration_cost)}</td>
                      <td className="text-right font-medium">{usd(q.total_cost)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </>
      )}
      {!run && <p className="text-sm text-muted-foreground">Run a calculation first, then pick it here to see the 3-year migration ramp.</p>}
    </div>
  );
}

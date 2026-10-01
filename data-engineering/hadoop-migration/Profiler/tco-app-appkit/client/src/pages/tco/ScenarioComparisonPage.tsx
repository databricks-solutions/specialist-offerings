import { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, Button } from '@databricks/appkit-ui/react';
import { api, type RunSummary } from '@/lib/api';

const usd = (n: number | null | undefined) =>
  n == null ? '—' : n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

export function ScenarioComparisonPage() {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const refresh = () => { void api.listRuns().then(setRuns).catch(() => {}); };
  useEffect(() => { refresh(); }, []);

  return (
    <div className="space-y-6 w-full max-w-5xl mx-auto">
      <div className="flex items-center gap-4">
        <h2 className="text-2xl font-bold">Scenario Comparison</h2>
        <Button variant="ghost" size="sm" className="ml-auto" onClick={refresh}>Refresh</Button>
      </div>

      <Card>
        <CardHeader><CardTitle>All runs ({runs.length})</CardTitle></CardHeader>
        <CardContent>
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-muted-foreground border-b">
                <th className="py-2">Run</th>
                <th className="text-right">Hadoop / yr</th>
                <th className="text-right">Databricks / yr</th>
                <th className="text-right">Savings</th>
                <th className="text-right">Created</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <tr key={r.run_id} className="border-b last:border-0">
                  <td className="py-2 font-medium">{r.run_name || r.run_id.slice(0, 8)}</td>
                  <td className="text-right text-red-600">{usd(r.total_hadoop_cost_annual)}</td>
                  <td className="text-right text-green-600">{usd(r.total_databricks_cost_annual)}</td>
                  <td className={`text-right ${(r.savings_pct ?? 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {r.savings_pct == null ? '—' : `${r.savings_pct}%`}
                  </td>
                  <td className="text-right text-xs text-muted-foreground">{new Date(r.created_at).toLocaleString()}</td>
                </tr>
              ))}
              {runs.length === 0 && <tr><td colSpan={5} className="py-3 text-muted-foreground">No runs yet — use the Calculator.</td></tr>}
            </tbody>
          </table>
        </CardContent>
      </Card>
    </div>
  );
}

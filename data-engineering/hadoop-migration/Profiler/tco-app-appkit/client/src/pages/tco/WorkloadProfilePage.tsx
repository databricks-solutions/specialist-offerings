import { useMemo } from 'react';
import { useAnalyticsQuery, Card, CardContent, CardHeader, CardTitle, Skeleton } from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { useCatalogSchema } from '@/lib/catalog';

type Row = Record<string, unknown>;
const n = (v: unknown) => (v == null ? 0 : Number(v));
const fmt = (v: unknown) => Math.round(n(v)).toLocaleString();

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border p-4">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-2xl font-semibold">{value}</div>
    </div>
  );
}

export function WorkloadProfilePage() {
  const { catalog, database } = useCatalogSchema();
  const params = useMemo(() => ({ catalog: sql.string(catalog), database: sql.string(database) }), [catalog, database]);

  const window = useAnalyticsQuery('observation_window', params);
  const workload = useAnalyticsQuery('workload_by_type', params);
  const peak = useAnalyticsQuery('peak_workload', params);
  const hosts = useAnalyticsQuery('cluster_hosts', params);

  const loading = window.loading || workload.loading || peak.loading || hosts.loading;
  const err = window.error || workload.error || peak.error || hosts.error;

  const w = ((window.data ?? []) as Row[])[0] ?? {};
  const h = ((hosts.data ?? []) as Row[])[0] ?? {};
  const p = ((peak.data ?? []) as Row[])[0] ?? {};
  const wl = (workload.data ?? []) as Row[];

  return (
    <div className="space-y-6 w-full max-w-6xl mx-auto">
      <h2 className="text-2xl font-bold">Workload Profile</h2>
      <p className="text-sm text-muted-foreground">
        Source: <span className="font-mono">{catalog}.{database}</span>
      </p>

      {err && <div className="text-sm text-red-600">{String(err)}</div>}
      {loading && <Skeleton className="h-32 w-full" />}

      {!loading && !err && (
        <>
          <Card>
            <CardHeader><CardTitle>Cluster &amp; Observation Window</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 md:grid-cols-5 gap-3">
              <Stat label="Nodes (cm_hosts)" value={fmt(h.node_count)} />
              <Stat label="Total vCores" value={fmt(h.total_vcores)} />
              <Stat label="YARN apps" value={fmt(w.apps)} />
              <Stat label="Distinct days" value={fmt(w.distinct_days)} />
              <Stat label="Peak vCores" value={fmt(p.peak_vcores)} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader><CardTitle>Workload by Type</CardTitle></CardHeader>
            <CardContent>
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-muted-foreground border-b">
                    <th className="py-2">Job type</th><th>Apps</th><th className="text-right">Memory GB-hours</th>
                  </tr>
                </thead>
                <tbody>
                  {wl.map((r) => (
                    <tr key={String(r.job_type)} className="border-b last:border-0">
                      <td className="py-2">{String(r.job_type)}</td>
                      <td>{fmt(r.total_jobs)}</td>
                      <td className="text-right">{fmt(r.total_memory_gb_hours)}</td>
                    </tr>
                  ))}
                  {wl.length === 0 && <tr><td colSpan={3} className="py-3 text-muted-foreground">No workload rows for this schema.</td></tr>}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}

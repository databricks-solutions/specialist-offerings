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
  // Only the observation window + workload are core. Host (cm_hosts) and peak
  // (CM timeseries) are supplementary and are legitimately empty/absent for
  // Ambari/HDP clusters, so their errors must not blank the whole page.
  const coreErr = window.error || workload.error;
  const cmUnavailable = Boolean(hosts.error || peak.error) || n(((hosts.data ?? []) as Row[])[0]?.node_count) === 0;

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

      {coreErr && <div className="text-sm text-red-600">{String(coreErr)}</div>}
      {loading && <Skeleton className="h-32 w-full" />}

      {!loading && !coreErr && (
        <>
          <Card>
            <CardHeader><CardTitle>Cluster &amp; Observation Window</CardTitle></CardHeader>
            <CardContent className="grid grid-cols-2 md:grid-cols-5 gap-3">
              <Stat label="Nodes (cm_hosts)" value={fmt(h.node_count)} />
              <Stat label="Total vCores" value={fmt(h.total_vcores)} />
              <Stat label="YARN apps" value={fmt(w.apps)} />
              <Stat label="Distinct days" value={fmt(w.distinct_days)} />
              <Stat label="Peak vCores" value={fmt(p.peak_vcores)} />
              {cmUnavailable && (
                <p className="text-xs text-muted-foreground md:col-span-5">
                  No Cloudera Manager host/timeseries data for this schema (Ambari/HDP cluster) — node count &amp; sizing are manual TCO inputs.
                </p>
              )}
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

import { useState, useEffect } from 'react';
import { Button, Card, CardContent, CardHeader, CardTitle, Label } from '@databricks/appkit-ui/react';
import { api, type AssumptionSummary } from '@/lib/api';
import type { Assumptions } from '../../../../shared/tco-types';

// The fields the form edits (others fall back to engine defaults). Grouped for layout.
const NUM_FIELDS: Array<[keyof Assumptions, string]> = [
  ['hadoop_node_count', '# Nodes'],
  ['hadoop_vcores_per_node', 'vCores/node'],
  ['hadoop_utilization_pct', 'Utilization %'],
  ['hadoop_datacenter_per_node', 'Datacenter $/node'],
  ['hadoop_hardware_per_node', 'Hardware $/node'],
  ['hadoop_admin_count', '# Admins'],
  ['hadoop_admin_salary', 'Admin salary $'],
  ['etl_pct', 'ETL %'],
  ['interactive_pct', 'Interactive %'],
  ['bisql_pct', 'BI/SQL %'],
  ['storage_total_tb', 'Storage (TB)'],
  ['migration_custom_cost', 'Migration cost $ (custom)'],
];

const blank = (): Record<string, string> => ({ name: '', target_cloud: 'AWS', databricks_tier: 'PREMIUM', hadoop_vendor_type: 'Open Source', migration_tshirt: 'custom', dbu_method: 'measured' });

export function AssumptionsPage() {
  const [list, setList] = useState<AssumptionSummary[]>([]);
  const [form, setForm] = useState<Record<string, string>>(blank());
  const [editId, setEditId] = useState<string | null>(null);
  const [status, setStatus] = useState<{ kind: 'ok' | 'err'; msg: string } | null>(null);

  const refresh = () => { void api.listAssumptions().then(setList).catch((e) => setStatus({ kind: 'err', msg: String((e as Error).message || e) })); };
  useEffect(() => { refresh(); }, []);

  const set = (k: string, v: string) => setForm((f) => ({ ...f, [k]: v }));

  function toBody(): Assumptions {
    const b: Record<string, unknown> = {
      name: form.name, target_cloud: form.target_cloud, databricks_tier: form.databricks_tier,
      hadoop_vendor_type: form.hadoop_vendor_type, migration_tshirt: form.migration_tshirt,
      dbu_method: form.dbu_method,
    };
    for (const [k] of NUM_FIELDS) {
      const v = form[k as string];
      if (v !== undefined && v !== '') b[k as string] = Number(v);
    }
    return b as Assumptions;
  }

  async function save() {
    if (!form.name) { setStatus({ kind: 'err', msg: 'Name is required.' }); return; }
    try {
      if (editId) {
        await api.updateAssumption(editId, toBody());
        setStatus({ kind: 'ok', msg: `Updated "${form.name}" at ${new Date().toLocaleTimeString()}` });
      } else {
        const created = await api.createAssumption(toBody());
        setStatus({ kind: 'ok', msg: `Saved "${form.name}" (${created.assumption_id?.slice(0, 8)}) at ${new Date().toLocaleTimeString()}` });
      }
      setForm(blank()); setEditId(null);
      refresh();
    } catch (e) {
      setStatus({ kind: 'err', msg: String((e as Error).message || e) });
    }
  }

  async function edit(id: string) {
    const full = await api.getAssumption(id);
    const f: Record<string, string> = blank();
    for (const [k, v] of Object.entries(full)) if (v != null) f[k] = String(v);
    setForm(f); setEditId(id);
    setStatus({ kind: 'ok', msg: `Editing "${full.name}" — change fields and Save.` });
  }

  async function remove(id: string, name: string) {
    await api.deleteAssumption(id);
    setStatus({ kind: 'ok', msg: `Deleted "${name}".` });
    if (editId === id) { setForm(blank()); setEditId(null); }
    refresh();
  }

  return (
    <div className="space-y-6 w-full max-w-6xl mx-auto">
      <h2 className="text-2xl font-bold">Assumption Sets</h2>

      {/* Live panel — fills as you add (backlog #9) */}
      <Card>
        <CardHeader><CardTitle>Saved sets ({list.length})</CardTitle></CardHeader>
        <CardContent>
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-muted-foreground border-b">
                <th className="py-2">Name</th><th>Cloud</th><th>Tier</th><th>Nodes</th><th>vCores</th><th></th>
              </tr>
            </thead>
            <tbody>
              {list.map((a) => (
                <tr key={a.assumption_id} className="border-b last:border-0">
                  <td className="py-2 font-medium">{a.name}</td>
                  <td>{a.target_cloud}</td>
                  <td>{a.databricks_tier}</td>
                  <td>{a.hadoop_node_count ?? '—'}</td>
                  <td>{a.hadoop_vcores_per_node ?? '—'}</td>
                  <td className="text-right space-x-2">
                    <Button variant="ghost" size="sm" onClick={() => void edit(a.assumption_id)}>Edit</Button>
                    <Button variant="ghost" size="sm" onClick={() => void remove(a.assumption_id, a.name)}>Delete</Button>
                  </td>
                </tr>
              ))}
              {list.length === 0 && <tr><td colSpan={6} className="py-3 text-muted-foreground">No sets yet — create one below.</td></tr>}
            </tbody>
          </table>
        </CardContent>
      </Card>

      {/* Create / edit form */}
      <Card>
        <CardHeader><CardTitle>{editId ? 'Edit set' : 'New set'}</CardTitle></CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <div className="md:col-span-2">
              <Label>Name</Label>
              <input className="w-full h-9 rounded-md border px-3 text-sm bg-background" value={form.name} onChange={(e) => set('name', e.target.value)} />
            </div>
            {([['target_cloud', 'Cloud', ['AWS', 'AZURE', 'GCP']], ['databricks_tier', 'Tier', ['STANDARD', 'PREMIUM', 'ENTERPRISE']], ['hadoop_vendor_type', 'Hadoop vendor', ['Open Source', 'Licensed']], ['migration_tshirt', 'Migration size', ['small', 'medium', 'large', 'custom']], ['dbu_method', 'DBU method', ['measured', 'capacity']]] as const).map(([k, label, opts]) => (
              <div key={k}>
                <Label>{label}</Label>
                <select className="w-full h-9 rounded-md border px-2 text-sm bg-background" value={form[k]} onChange={(e) => set(k, e.target.value)}>
                  {opts.map((o) => <option key={o} value={o}>{o}</option>)}
                </select>
              </div>
            ))}
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {NUM_FIELDS.map(([k, label]) => (
              <div key={k as string}>
                <Label>{label}</Label>
                <input type="number" className="w-full h-9 rounded-md border px-3 text-sm bg-background"
                  value={form[k as string] ?? ''} onChange={(e) => set(k as string, e.target.value)} />
              </div>
            ))}
          </div>
          <div className="flex items-center gap-3">
            <Button onClick={() => void save()}>{editId ? 'Update' : 'Save'} assumption</Button>
            {editId && <Button variant="ghost" onClick={() => { setForm(blank()); setEditId(null); }}>Cancel</Button>}
            {status && <span className={`text-sm ${status.kind === 'ok' ? 'text-green-600' : 'text-red-600'}`}>{status.msg}</span>}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

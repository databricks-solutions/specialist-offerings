"""Page 3: Assumptions — Full TCO model inputs with 8-section accordion.

Save/load named assumption sets covering all 7 cost categories:
Hadoop on-prem, workload split, compute modifiers, VM/instance,
DBSQL warehouse, storage tiers, support/admin, and migration.
"""

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
from models.constants import DEFAULT_ASSUMPTIONS

dash.register_page(__name__, path="/assumptions", name="Assumptions", order=2)

D = DEFAULT_ASSUMPTIONS  # shorthand


def _num(id, val, **kw):
    return dbc.Input(id=id, type="number", value=val, size="sm", **kw)


def _section(title, children):
    return dbc.AccordionItem(children, title=title)


layout = dbc.Container([
    html.H2("Assumptions", className="mt-3 mb-3"),
    html.P("Configure all TCO model inputs. Save as a named set for calculations.",
           className="text-muted"),

    # Load existing
    dbc.Row([
        dbc.Col([
            dbc.Label("Load Saved Assumptions"),
            dcc.Dropdown(id="load-assumption-select", placeholder="Select..."),
        ], width=4),
        dbc.Col([
            dbc.Button("Load", id="btn-load-assumption", color="secondary", className="mt-4"),
        ], width=2),
    ], className="mb-3"),

    html.Hr(),

    dbc.Accordion([
        # 1. General
        _section("General", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Assumption Set Name"),
                    dbc.Input(id="input-name", type="text", placeholder="e.g. BNY baseline Q1"),
                ], width=4),
                dbc.Col([
                    dbc.Label("Target Cloud"),
                    dcc.Dropdown(id="input-cloud", options=[
                        {"label": "AWS", "value": "AWS"},
                        {"label": "Azure", "value": "AZURE"},
                        {"label": "GCP", "value": "GCP"},
                    ], value="AWS"),
                ], width=3),
                dbc.Col([
                    dbc.Label("Databricks Tier"),
                    dcc.Dropdown(id="input-tier", options=[
                        {"label": "Standard", "value": "STANDARD"},
                        {"label": "Premium", "value": "PREMIUM"},
                        {"label": "Enterprise", "value": "ENTERPRISE"},
                    ], value="PREMIUM"),
                ], width=3),
            ]),
        ]),

        # 2. Hadoop On-Prem
        _section("Hadoop On-Prem Costs", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Vendor Type"),
                    dcc.Dropdown(id="input-hadoop-vendor", options=[
                        {"label": "Licensed (Cloudera/HDP)", "value": "Licensed"},
                        {"label": "Open Source", "value": "Open Source"},
                    ], value=D["hadoop_vendor_type"]),
                ], width=3),
                dbc.Col([
                    dbc.Label("Node Count"),
                    _num("input-hadoop-nodes", D["hadoop_node_count"], min=1, max=10000),
                ], width=2),
                dbc.Col([
                    dbc.Label("vCores per Node"),
                    _num("input-hadoop-vcores", D["hadoop_vcores_per_node"], min=4, max=128),
                ], width=2),
                dbc.Col([
                    dbc.Label("Utilization %"),
                    _num("input-hadoop-util", D["hadoop_utilization_pct"], min=1, max=100),
                ], width=2),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("License $/Node/Year"),
                    _num("input-hadoop-license", D["hadoop_license_per_node"], min=0),
                ], width=3),
                dbc.Col([
                    dbc.Label("License Discount %"),
                    _num("input-hadoop-lic-disc", D["hadoop_license_discount"], min=0, max=100),
                ], width=2),
                dbc.Col([
                    dbc.Label("Support % of License"),
                    _num("input-hadoop-support", D["hadoop_support_pct"], min=0, max=100),
                ], width=2),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Hardware $/Node/Year"),
                    _num("input-hadoop-hw", D["hadoop_hardware_per_node"], min=0),
                ], width=3),
                dbc.Col([
                    dbc.Label("Datacenter $/Node/Year"),
                    _num("input-hadoop-dc", D["hadoop_datacenter_per_node"], min=0),
                ], width=3),
                dbc.Col([
                    dbc.Label("Admin Count"),
                    _num("input-hadoop-admins", D["hadoop_admin_count"], min=0),
                ], width=2),
                dbc.Col([
                    dbc.Label("Admin Salary $"),
                    _num("input-hadoop-salary", D["hadoop_admin_salary"], min=0),
                ], width=3),
            ]),
        ]),

        # 3. Workload Split
        _section("Workload Split", [
            html.P("Must sum to 100%.", className="text-muted small"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("ETL %"),
                    _num("input-etl-pct", D["etl_pct"], min=0, max=100),
                ], width=3),
                dbc.Col([
                    dbc.Label("Interactive %"),
                    _num("input-interactive-pct", D["interactive_pct"], min=0, max=100),
                ], width=3),
                dbc.Col([
                    dbc.Label("BI/SQL %"),
                    _num("input-bisql-pct", D["bisql_pct"], min=0, max=100),
                ], width=3),
            ]),
        ]),

        # 4. Databricks Compute
        _section("Databricks Compute", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Use Serverless"),
                    dbc.Switch(id="input-serverless", value=False),
                ], width=2),
                dbc.Col([
                    dbc.Label("Photon Enabled"),
                    dbc.Switch(id="input-photon", value=True),
                ], width=2),
                dbc.Col([
                    dbc.Label("Utilization Factor"),
                    _num("input-utilization", 0.9, min=0.1, max=1.0, step=0.05),
                ], width=2),
                dbc.Col([
                    dbc.Label("Overhead Factor"),
                    _num("input-overhead", 1.1, min=1.0, max=2.0, step=0.05),
                ], width=2),
                dbc.Col([
                    dbc.Label("Discount %"),
                    _num("input-discount", 0, min=0, max=100),
                ], width=2),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Dev/Test Uplift"),
                    _num("input-dev-test", D["dev_test_uplift"], min=0, max=1, step=0.05),
                ], width=2),
                dbc.Col([
                    dbc.Label("Hyperthreading Factor"),
                    _num("input-ht-factor", D["hyperthreading_factor"], min=1, max=4, step=0.5),
                ], width=2),
                dbc.Col([
                    dbc.Label("Photon Perf Gain"),
                    _num("input-photon-gain", D["photon_perf_gain"], min=0, max=1, step=0.05),
                ], width=2),
            ]),
        ]),

        # 5. VM & Instance
        _section("VM & Instance Types", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Worker Instance Type"),
                    dcc.Dropdown(id="input-worker-inst",
                                 value=D["worker_instance_type"],
                                 placeholder="Select instance...",
                                 searchable=True, clearable=False),
                ], width=3),
                dbc.Col([
                    dbc.Label("Driver Instance Type"),
                    dcc.Dropdown(id="input-driver-inst",
                                 value=D["driver_instance_type"],
                                 placeholder="Select instance...",
                                 searchable=True, clearable=False),
                ], width=3),
                dbc.Col([
                    dbc.Label("VM Discount Type"),
                    dcc.Dropdown(id="input-vm-discount", options=[
                        {"label": "On-Demand", "value": "on_demand"},
                        {"label": "1-Year Reserved", "value": "reserved"},
                        {"label": "Spot/Preemptible", "value": "spot"},
                    ], value=D["vm_discount_type"]),
                ], width=3),
                dbc.Col([
                    dbc.Label("Ref VM Memory (GB)"),
                    _num("input-vm-mem", 64, min=16, max=512),
                ], width=2),
            ]),
        ]),

        # 6. DBSQL Warehouse
        _section("DBSQL Warehouse", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Warehouse Size"),
                    dcc.Dropdown(id="input-dbsql-size", options=[
                        {"label": s, "value": s}
                        for s in ["2X-Small", "X-Small", "Small", "Medium",
                                  "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]
                    ], value=D["dbsql_warehouse_size"]),
                ], width=3),
                dbc.Col([
                    dbc.Label("Warehouse Type"),
                    dcc.Dropdown(id="input-dbsql-type", options=[
                        {"label": "Classic", "value": "classic"},
                        {"label": "Pro", "value": "pro"},
                        {"label": "Serverless", "value": "serverless"},
                    ], value=D["dbsql_type"]),
                ], width=3),
                dbc.Col([
                    dbc.Label("DBSQL Utilization"),
                    _num("input-dbsql-util", D["dbsql_utilization"], min=0.1, max=1.0, step=0.05),
                ], width=2),
            ]),
        ]),

        # 7. Storage
        _section("Storage", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("HDFS Replication Factor"),
                    _num("input-repl", 3, min=1, max=5),
                ], width=2),
                dbc.Col([
                    dbc.Label("Delta Compression Ratio"),
                    _num("input-compression", 0.5, min=0.1, max=1.0, step=0.05),
                ], width=2),
                dbc.Col([
                    dbc.Label("Storage Discount %"),
                    _num("input-storage-disc", D["storage_discount_pct"], min=0, max=100),
                ], width=2),
            ], className="mb-2"),
            html.P("Storage Tier Split (must sum to 100%):", className="small fw-bold mt-2"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Hot %"),
                    _num("input-hot-pct", D["hot_storage_pct"], min=0, max=100),
                ], width=2),
                dbc.Col([
                    dbc.Label("Cold %"),
                    _num("input-cold-pct", D["cold_storage_pct"], min=0, max=100),
                ], width=2),
                dbc.Col([
                    dbc.Label("Archive %"),
                    _num("input-archive-pct", D["archive_storage_pct"], min=0, max=100),
                ], width=2),
                dbc.Col([
                    dbc.Label("Flat Rate $/GB/mo (fallback)"),
                    _num("input-storage-cost", 0.023, min=0, max=1, step=0.001),
                ], width=3),
            ]),
        ]),

        # 8. Migration & Support
        _section("Migration & Support", [
            dbc.Row([
                dbc.Col([
                    dbc.Label("Migration T-Shirt"),
                    dcc.Dropdown(id="input-migration-tshirt", options=[
                        {"label": "Small ($500K)", "value": "small"},
                        {"label": "Medium ($850K)", "value": "medium"},
                        {"label": "Large ($1.75M)", "value": "large"},
                        {"label": "Custom", "value": "custom"},
                    ], value=D["migration_tshirt"]),
                ], width=3),
                dbc.Col([
                    dbc.Label("Custom Cost ($)"),
                    _num("input-migration-custom", D["migration_custom_cost"], min=0),
                ], width=3),
                dbc.Col([
                    dbc.Label("ECIF Credit ($)"),
                    _num("input-ecif", D["ecif_credit"], min=0),
                ], width=2),
                dbc.Col([
                    dbc.Label("Duration (Quarters)"),
                    _num("input-migration-quarters", D["migration_duration_quarters"],
                         min=1, max=12),
                ], width=2),
            ], className="mb-2"),
            dbc.Row([
                dbc.Col([
                    dbc.Label("Databricks Support % of DBU"),
                    _num("input-dbx-support", D["dbx_support_pct"], min=0, max=100),
                ], width=3),
                dbc.Col([
                    dbc.Label("Databricks Admin Overhead %"),
                    _num("input-dbx-admin", D["dbx_admin_overhead_pct"], min=0, max=100),
                ], width=3),
            ]),
        ]),
    ], start_collapsed=True, always_open=True),

    html.Hr(),

    dbc.Row([
        dbc.Col([
            dbc.Button("Save Assumptions", id="btn-save-assumptions",
                       color="primary", size="lg"),
            html.Span(id="assumption-save-status", className="ms-3"),
        ]),
    ]),

    dcc.Store(id="current-assumption-id"),
], fluid=True)


# --- All field IDs in order for load/save ---
_FIELD_IDS = [
    "input-name", "input-cloud", "input-tier",
    "input-hadoop-vendor", "input-hadoop-nodes", "input-hadoop-vcores",
    "input-hadoop-util", "input-hadoop-license", "input-hadoop-lic-disc",
    "input-hadoop-support", "input-hadoop-hw", "input-hadoop-dc",
    "input-hadoop-admins", "input-hadoop-salary",
    "input-etl-pct", "input-interactive-pct", "input-bisql-pct",
    "input-serverless", "input-photon",
    "input-utilization", "input-overhead", "input-discount",
    "input-dev-test", "input-ht-factor", "input-photon-gain",
    "input-worker-inst", "input-driver-inst", "input-vm-discount", "input-vm-mem",
    "input-dbsql-size", "input-dbsql-type", "input-dbsql-util",
    "input-repl", "input-compression", "input-storage-disc",
    "input-hot-pct", "input-cold-pct", "input-archive-pct", "input-storage-cost",
    "input-migration-tshirt", "input-migration-custom", "input-ecif",
    "input-migration-quarters",
    "input-dbx-support", "input-dbx-admin",
]

# Map field IDs to assumption dict keys
_FIELD_MAP = {
    "input-name": "name", "input-cloud": "target_cloud", "input-tier": "databricks_tier",
    "input-hadoop-vendor": "hadoop_vendor_type", "input-hadoop-nodes": "hadoop_node_count",
    "input-hadoop-vcores": "hadoop_vcores_per_node",
    "input-hadoop-util": "hadoop_utilization_pct",
    "input-hadoop-license": "hadoop_license_per_node",
    "input-hadoop-lic-disc": "hadoop_license_discount",
    "input-hadoop-support": "hadoop_support_pct",
    "input-hadoop-hw": "hadoop_hardware_per_node",
    "input-hadoop-dc": "hadoop_datacenter_per_node",
    "input-hadoop-admins": "hadoop_admin_count",
    "input-hadoop-salary": "hadoop_admin_salary",
    "input-etl-pct": "etl_pct", "input-interactive-pct": "interactive_pct",
    "input-bisql-pct": "bisql_pct",
    "input-serverless": "use_serverless", "input-photon": "photon_enabled",
    "input-utilization": "utilization_factor", "input-overhead": "overhead_factor",
    "input-discount": "discount_pct",
    "input-dev-test": "dev_test_uplift", "input-ht-factor": "hyperthreading_factor",
    "input-photon-gain": "photon_perf_gain",
    "input-worker-inst": "worker_instance_type",
    "input-driver-inst": "driver_instance_type",
    "input-vm-discount": "vm_discount_type", "input-vm-mem": "vm_mem_gb",
    "input-dbsql-size": "dbsql_warehouse_size", "input-dbsql-type": "dbsql_type",
    "input-dbsql-util": "dbsql_utilization",
    "input-repl": "hdfs_repl_factor", "input-compression": "delta_compression",
    "input-storage-disc": "storage_discount_pct",
    "input-hot-pct": "hot_storage_pct", "input-cold-pct": "cold_storage_pct",
    "input-archive-pct": "archive_storage_pct",
    "input-storage-cost": "storage_cost_per_gb_month",
    "input-migration-tshirt": "migration_tshirt",
    "input-migration-custom": "migration_custom_cost",
    "input-ecif": "ecif_credit",
    "input-migration-quarters": "migration_duration_quarters",
    "input-dbx-support": "dbx_support_pct", "input-dbx-admin": "dbx_admin_overhead_pct",
}

_INV_MAP = {v: k for k, v in _FIELD_MAP.items()}


@callback(
    Output("input-worker-inst", "options"),
    Output("input-driver-inst", "options"),
    Input("input-cloud", "value"),
)
def update_instance_options(cloud):
    """Populate worker/driver instance dropdowns from hardcoded catalog for selected cloud."""
    from models.constants import INSTANCE_CATALOG

    if not cloud:
        return [], []
    instances = INSTANCE_CATALOG.get(cloud, [])
    options = [
        {"label": f"{name}  ({vcpus}v / {mem_gb}GB)",
         "value": name}
        for name, vcpus, mem_gb in instances
    ]
    return options, options


@callback(
    Output("input-vm-mem", "value"),
    Input("input-worker-inst", "value"),
    State("input-cloud", "value"),
)
def auto_fill_vm_memory(worker_inst, cloud):
    """Auto-derive Ref VM Memory from the selected worker instance type."""
    if not worker_inst or not cloud:
        return dash.no_update
    from models.constants import get_instance_specs
    specs = get_instance_specs(worker_inst, cloud)
    return specs["memory_gb"]


@callback(
    Output("load-assumption-select", "options"),
    Input("store-catalog", "data"),
    Input("store-schema", "data"),
)
def load_assumption_options(catalog, schema):
    from models.cost_engine import list_assumptions
    try:
        df = list_assumptions(catalog, schema)
        return [{"label": f"{r['name']} ({r['target_cloud']})",
                 "value": r["assumption_id"]}
                for _, r in df.iterrows()]
    except Exception:
        return []


@callback(
    [Output(fid, "value") for fid in _FIELD_IDS],
    Input("btn-load-assumption", "n_clicks"),
    State("load-assumption-select", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def load_assumption(n_clicks, assumption_id, catalog, schema):
    if not assumption_id:
        return [dash.no_update] * len(_FIELD_IDS)

    from models.cost_engine import get_assumptions
    a = get_assumptions(assumption_id, catalog, schema)
    return [a.get(_FIELD_MAP[fid], DEFAULT_ASSUMPTIONS.get(_FIELD_MAP[fid], ""))
            for fid in _FIELD_IDS]


@callback(
    Output("assumption-save-status", "children"),
    Output("current-assumption-id", "data"),
    Input("btn-save-assumptions", "n_clicks"),
    [State(fid, "value") for fid in _FIELD_IDS],
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def save_assumptions_cb(n_clicks, *args):
    # Last 2 args are catalog, schema
    field_values = args[:-2]
    catalog, schema = args[-2], args[-1]

    name_val = field_values[0]
    if not name_val:
        return html.Span("Name is required.", className="text-danger"), dash.no_update

    assumptions = {}
    for fid, val in zip(_FIELD_IDS, field_values):
        key = _FIELD_MAP[fid]
        assumptions[key] = val

    from models.cost_engine import save_assumptions
    try:
        aid = save_assumptions(assumptions, catalog, schema)
        return html.Span(f"Saved: {aid[:8]}...", className="text-success"), aid
    except Exception as e:
        return html.Span(f"Error: {e}", className="text-danger"), dash.no_update

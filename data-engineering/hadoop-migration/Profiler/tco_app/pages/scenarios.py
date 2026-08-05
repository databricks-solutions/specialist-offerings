"""Page 5: Scenario Comparison — Side-by-side with Do Nothing baseline."""

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd

dash.register_page(__name__, path="/scenarios", name="Scenario Comparison", order=4)

layout = dbc.Container([
    html.H2("Scenario Comparison", className="mt-3 mb-3"),
    html.P("Compare TCO runs side by side. Auto-generates a 'Do Nothing' baseline.",
           className="text-muted"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Run A"),
            dcc.Dropdown(id="scenario-run-a", placeholder="Select run..."),
        ], width=4),
        dbc.Col([
            dbc.Label("Run B"),
            dcc.Dropdown(id="scenario-run-b", placeholder="Select run..."),
        ], width=4),
        dbc.Col([
            dbc.Label("Run C (optional)"),
            dcc.Dropdown(id="scenario-run-c", placeholder="Select run..."),
        ], width=4),
    ], className="mb-3"),

    dbc.Button("Compare", id="btn-compare", color="primary", className="mb-4"),

    html.Hr(),

    dcc.Loading(html.Div(id="comparison-output"), type="default"),

    dbc.Row([
        dbc.Col([
            dbc.Button("Export to CSV", id="btn-export-csv", color="secondary",
                       className="mt-3"),
            dcc.Download(id="download-csv"),
        ]),
    ]),
], fluid=True)


@callback(
    Output("scenario-run-a", "options"),
    Output("scenario-run-b", "options"),
    Output("scenario-run-c", "options"),
    Input("store-catalog", "data"),
    Input("store-schema", "data"),
)
def load_runs(catalog, schema):
    from models.cost_engine import list_runs
    try:
        df = list_runs(catalog, schema)
        options = [{"label": f"{r['run_name']} (${r['total_cost_annual']:,.0f})",
                    "value": r["run_id"]}
                   for _, r in df.iterrows()]
        return options, options, options
    except Exception:
        return [], [], []


@callback(
    Output("comparison-output", "children"),
    Input("btn-compare", "n_clicks"),
    State("scenario-run-a", "value"),
    State("scenario-run-b", "value"),
    State("scenario-run-c", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def compare_runs(n_clicks, run_a, run_b, run_c, catalog, schema):
    if not run_a:
        return html.P("Select at least Run A.", className="text-warning")

    from models.cost_engine import get_run, get_assumptions

    run_ids = [run_a]
    if run_b:
        run_ids.append(run_b)
    if run_c:
        run_ids.append(run_c)

    runs = []
    for rid in run_ids:
        try:
            r = get_run(rid, catalog, schema)
            a = get_assumptions(r["assumption_id"], catalog, schema)
            r["_assumptions"] = a
            runs.append(r)
        except Exception as e:
            return html.P(f"Error loading run {rid[:8]}: {e}", className="text-danger")

    # Add "Do Nothing" as implicit baseline from Run A's Hadoop costs
    run_a_data = runs[0]
    hadoop_annual = float(run_a_data.get("total_hadoop_cost_annual", 0) or 0)

    # Comparison metrics — annual + 3-year
    metrics = [
        ("Run Name", lambda r: r.get("run_name", "")),
        ("Cloud", lambda r: r["_assumptions"]["target_cloud"]),
        ("Tier", lambda r: r["_assumptions"]["databricks_tier"]),
        ("Serverless", lambda r: str(r["_assumptions"].get("use_serverless", False))),
        ("Discount %", lambda r: f"{r['_assumptions'].get('discount_pct', 0)}%"),
        ("---", lambda r: "--- Annual ---"),
        ("Hadoop License", lambda r: _fmt(r.get("hadoop_license_cost"))),
        ("Hadoop Support", lambda r: _fmt(r.get("hadoop_support_cost"))),
        ("Hadoop Hardware", lambda r: _fmt(r.get("hadoop_hardware_cost"))),
        ("Hadoop DC", lambda r: _fmt(r.get("hadoop_datacenter_cost"))),
        ("Hadoop Admin", lambda r: _fmt(r.get("hadoop_admin_cost"))),
        ("Hadoop Total", lambda r: _fmt(r.get("total_hadoop_cost_annual"))),
        ("ETL DBU", lambda r: _fmt(r.get("dbx_etl_dbu_cost"))),
        ("Interactive DBU", lambda r: _fmt(r.get("dbx_interactive_dbu_cost"))),
        ("BI/SQL DBU", lambda r: _fmt(r.get("dbx_bisql_dbu_cost"))),
        ("VM Compute", lambda r: _fmt(r.get("dbx_vm_cost"))),
        ("Storage", lambda r: _fmt(r.get("total_storage_cost_annual"))),
        ("DBX Support", lambda r: _fmt(r.get("dbx_support_cost"))),
        ("DBX Admin", lambda r: _fmt(r.get("dbx_admin_cost"))),
        ("Databricks Total", lambda r: _fmt(r.get("total_cost_annual"))),
        ("Savings %", lambda r: f"{r['savings_pct']:.1f}%" if r.get("savings_pct") else "N/A"),
        ("--- 3-Year ---", lambda r: "--- 3-Year ---"),
        ("Do Nothing (3yr)", lambda r: _fmt(r.get("three_year_hadoop_total"))),
        ("Migration Path (3yr)", lambda r: _fmt(r.get("three_year_databricks_total"))),
        ("3yr Net Savings", lambda r: _fmt(r.get("three_year_savings"))),
    ]

    # Build table
    headers = [html.Th("Metric")]
    labels = ["Do Nothing"]
    for i, run in enumerate(runs):
        headers.append(html.Th(f"Run {chr(65+i)}"))

    rows = []
    for label, accessor in metrics:
        if label.startswith("---"):
            rows.append(html.Tr([
                html.Td(html.Strong(label.replace("-", "")), colSpan=len(runs)+1,
                         className="bg-light"),
            ]))
            continue

        row = [html.Td(html.Strong(label))]
        for run in runs:
            val = accessor(run) if callable(accessor) else str(run.get(accessor, ""))
            row.append(html.Td(val))
        rows.append(html.Tr(row))

    # Grouped bar chart
    categories = ["Hadoop Total", "DBU Costs", "VM Compute", "Storage", "Support+Admin"]
    fig = go.Figure()
    for i, run in enumerate(runs):
        dbu = sum([
            float(run.get("dbx_etl_dbu_cost", 0) or 0),
            float(run.get("dbx_interactive_dbu_cost", 0) or 0),
            float(run.get("dbx_bisql_dbu_cost", 0) or 0),
        ])
        vals = [
            float(run.get("total_hadoop_cost_annual", 0) or 0),
            dbu,
            float(run.get("dbx_vm_cost", 0) or 0),
            float(run.get("total_storage_cost_annual", 0) or 0),
            float(run.get("dbx_support_cost", 0) or 0) + float(run.get("dbx_admin_cost", 0) or 0),
        ]
        fig.add_trace(go.Bar(name=run.get("run_name", f"Run {chr(65+i)}"),
                              x=categories, y=vals))

    fig.update_layout(barmode="group", margin=dict(t=20, b=20),
                      yaxis_title="Annual Cost ($)")

    return html.Div([
        html.H5("Side-by-Side Comparison"),
        dbc.Table([
            html.Thead(html.Tr(headers)),
            html.Tbody(rows),
        ], striped=True, bordered=True, hover=True, size="sm"),
        html.H5("Category Comparison", className="mt-4"),
        dcc.Graph(figure=fig),
    ])


def _fmt(val):
    """Format a dollar value."""
    if val is None:
        return "N/A"
    try:
        return f"${float(val):,.0f}"
    except (ValueError, TypeError):
        return str(val)


@callback(
    Output("download-csv", "data"),
    Input("btn-export-csv", "n_clicks"),
    State("scenario-run-a", "value"),
    State("scenario-run-b", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def export_csv(n_clicks, run_a, run_b, catalog, schema):
    if not run_a:
        return None

    from models.cost_engine import get_run

    rows = []
    for rid in [run_a, run_b]:
        if not rid:
            continue
        r = get_run(rid, catalog, schema)
        for d in r.get("details", []):
            d["run_name"] = r.get("run_name", "")
            rows.append(d)

    df = pd.DataFrame(rows)
    return dcc.send_data_frame(df.to_csv, "tco_comparison.csv", index=False)

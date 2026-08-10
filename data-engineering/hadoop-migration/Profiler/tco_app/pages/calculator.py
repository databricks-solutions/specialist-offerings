"""Page 4: TCO Calculator — Full 7-category cost breakdown.

Runs the cost engine, shows Hadoop vs Databricks breakdown,
per-workload details, right-sizing, storage tiers, and summary.
"""

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

dash.register_page(__name__, path="/calculator", name="TCO Calculator", order=3)


def _card(title, value, subtitle="", color="primary"):
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, className="card-subtitle mb-2 text-muted"),
            html.H3(value, className=f"text-{color}"),
            html.Small(subtitle, className="text-muted") if subtitle else None,
        ]),
        className="mb-3",
    )


layout = dbc.Container([
    html.H2("TCO Calculator", className="mt-3 mb-3"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Assumption Set"),
            dcc.Dropdown(id="calc-assumption-select", placeholder="Select assumptions..."),
        ], width=4),
        dbc.Col([
            dbc.Label("Run Name"),
            dbc.Input(id="calc-run-name", type="text",
                      placeholder="e.g. BNY Hadoop TCO - Apr 2026"),
        ], width=4),
        dbc.Col([
            dbc.Button("Calculate TCO", id="btn-calculate", color="danger",
                       size="lg", className="mt-4"),
        ], width=2),
    ], className="mb-4"),

    html.Hr(),

    dcc.Loading(
        children=[
            # Row 1: Hadoop cost breakdown
            html.Div(id="calc-hadoop-section", className="mb-4"),
            # Row 2: Databricks cost breakdown
            html.Div(id="calc-dbx-section", className="mb-4"),
            # Row 3: Grand comparison
            dbc.Row(id="calc-summary-cards", className="mb-4"),
            # Chart
            dcc.Graph(id="calc-cost-chart", className="mb-4"),
            # Per-workload table
            html.Div(id="calc-workload-table", className="mb-4"),
            # Sizing
            html.Div(id="calc-sizing-section", className="mb-4"),
            # Storage
            html.Div(id="calc-storage-section"),
        ],
        type="default",
    ),
], fluid=True)


@callback(
    Output("calc-assumption-select", "options"),
    Input("store-catalog", "data"),
    Input("store-schema", "data"),
)
def load_assumptions_dropdown(catalog, schema):
    from models.cost_engine import list_assumptions
    try:
        df = list_assumptions(catalog, schema)
        return [{"label": f"{r['name']} ({r['target_cloud']})",
                 "value": r["assumption_id"]}
                for _, r in df.iterrows()]
    except Exception:
        return []


@callback(
    Output("calc-hadoop-section", "children"),
    Output("calc-dbx-section", "children"),
    Output("calc-summary-cards", "children"),
    Output("calc-cost-chart", "figure"),
    Output("calc-workload-table", "children"),
    Output("calc-sizing-section", "children"),
    Output("calc-storage-section", "children"),
    Input("btn-calculate", "n_clicks"),
    State("calc-assumption-select", "value"),
    State("calc-run-name", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def run_calculation(n_clicks, assumption_id, run_name, catalog, schema):
    empty_fig = go.Figure()
    empty = html.P("", className="text-muted")

    if not assumption_id:
        msg = html.P("Select an assumption set first.", className="text-warning")
        return msg, empty, [dbc.Col(msg)], empty_fig, empty, empty, empty

    from models.cost_engine import calculate_tco, get_assumptions
    from models.sizing import get_sizing_recommendations

    try:
        result = calculate_tco(
            assumption_id=assumption_id,
            catalog=catalog,
            schema=schema,
            run_name=run_name or "Untitled Run",
        )
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        msg = html.Pre(f"Error: {e}\n\n{tb}",
                       style={"fontSize": "11px", "color": "red", "maxHeight": "300px",
                              "overflowY": "auto"})
        return msg, empty, [dbc.Col(msg)], empty_fig, empty, empty, empty

    # ---- Hadoop cost breakdown ----
    hc = result.get("hadoop_costs", {})
    hadoop_section = html.Div([
        html.H5("Hadoop On-Prem Annual Costs"),
        dbc.Row([
            dbc.Col(_card("License", f"${hc.get('license_cost', 0):,.0f}", color="dark")),
            dbc.Col(_card("Support", f"${hc.get('support_cost', 0):,.0f}", color="dark")),
            dbc.Col(_card("Hardware", f"${hc.get('hardware_cost', 0):,.0f}", color="dark")),
            dbc.Col(_card("Datacenter", f"${hc.get('datacenter_cost', 0):,.0f}", color="dark")),
            dbc.Col(_card("Admin", f"${hc.get('admin_cost', 0):,.0f}", color="dark")),
            dbc.Col(_card("TOTAL Hadoop", f"${result.get('hadoop_annual', 0):,.0f}", color="danger")),
        ]),
    ])

    # ---- Databricks cost breakdown ----
    sdbu = result.get("stream_dbu_costs", {})
    dbx_section = html.Div([
        html.H5("Databricks Annual Costs"),
        dbc.Row([
            dbc.Col(_card("ETL DBU", f"${sdbu.get('etl', 0):,.0f}", color="primary")),
            dbc.Col(_card("Interactive DBU", f"${sdbu.get('interactive', 0):,.0f}", color="primary")),
            dbc.Col(_card("BI/SQL DBU", f"${sdbu.get('bisql', 0):,.0f}", color="primary")),
            dbc.Col(_card("VM Compute", f"${result.get('vm_cost_annual', 0):,.0f}", color="info")),
            dbc.Col(_card("Support", f"${result.get('dbx_support_cost', 0):,.0f}", color="secondary")),
            dbc.Col(_card("Admin", f"${result.get('dbx_admin_cost', 0):,.0f}", color="secondary")),
            dbc.Col(_card("TOTAL Databricks", f"${result.get('total_dbx_annual', 0):,.0f}", color="success")),
        ]),
    ])

    # ---- Grand comparison cards ----
    cards = [
        dbc.Col(_card("Hadoop Annual", f"${result.get('hadoop_annual', 0):,.0f}", color="danger")),
        dbc.Col(_card("Databricks Annual", f"${result.get('total_dbx_annual', 0):,.0f}", color="success")),
        dbc.Col(_card("Storage Annual", f"${result.get('total_storage_cost_annual', 0):,.0f}", color="info")),
    ]
    if result.get("savings_pct") is not None:
        sp = result["savings_pct"]
        cards.append(dbc.Col(_card(
            "Savings vs Hadoop", f"{sp:.1f}%",
            color="success" if sp > 0 else "warning",
        )))

    # ---- Stacked bar chart: Hadoop vs Databricks ----
    chart_data = pd.DataFrame([
        {"Category": "Hadoop", "Component": "License", "Cost": hc.get("license_cost", 0)},
        {"Category": "Hadoop", "Component": "Support", "Cost": hc.get("support_cost", 0)},
        {"Category": "Hadoop", "Component": "Hardware", "Cost": hc.get("hardware_cost", 0)},
        {"Category": "Hadoop", "Component": "Datacenter", "Cost": hc.get("datacenter_cost", 0)},
        {"Category": "Hadoop", "Component": "Admin", "Cost": hc.get("admin_cost", 0)},
        {"Category": "Databricks", "Component": "ETL DBU", "Cost": sdbu.get("etl", 0)},
        {"Category": "Databricks", "Component": "Interactive DBU", "Cost": sdbu.get("interactive", 0)},
        {"Category": "Databricks", "Component": "BI/SQL DBU", "Cost": sdbu.get("bisql", 0)},
        {"Category": "Databricks", "Component": "VM Compute", "Cost": result.get("vm_cost_annual", 0)},
        {"Category": "Databricks", "Component": "Storage", "Cost": result.get("total_storage_cost_annual", 0)},
        {"Category": "Databricks", "Component": "Support", "Cost": result.get("dbx_support_cost", 0)},
        {"Category": "Databricks", "Component": "Admin", "Cost": result.get("dbx_admin_cost", 0)},
    ])

    cost_fig = px.bar(
        chart_data, x="Category", y="Cost", color="Component",
        barmode="stack",
        labels={"Cost": "Annual Cost ($)"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    cost_fig.update_layout(margin=dict(t=20, b=20), showlegend=True)

    # ---- Workload breakdown table ----
    details_df = pd.DataFrame(result.get("details", []))
    if not details_df.empty:
        for col in ["estimated_cost", "hadoop_equivalent_cost", "estimated_dbu_hours",
                     "total_memory_gb_hours", "dbu_list_price", "dbu_effective_price"]:
            if col in details_df.columns:
                details_df[col] = details_df[col].round(2)

        workload_section = html.Div([
            html.H5("Per-Workload Cost Breakdown"),
            dbc.Table.from_dataframe(
                details_df[["job_type", "total_apps", "total_memory_gb_hours",
                             "target_sku", "estimated_dbu_hours",
                             "dbu_effective_price", "estimated_cost"]],
                striped=True, bordered=True, hover=True, size="sm",
            ),
            html.Small(f"Run ID: {result['run_id']}", className="text-muted"),
        ])
    else:
        workload_section = html.P("No workload data.", className="text-muted")

    # ---- Right-sizing ----
    try:
        assumptions = get_assumptions(assumption_id, catalog, schema)
        sizing = get_sizing_recommendations(catalog, schema, assumptions["target_cloud"])
        peak = sizing["peak"]
        recs_df = sizing["recommendations"]

        sizing_section = html.Div([
            html.H5("Right-Sizing Recommendations"),
            dbc.Row([
                dbc.Col(_card("Peak Memory (GB)", f"{peak.get('peak_memory_gb', 0):.1f}")),
                dbc.Col(_card("Peak vCores", f"{peak.get('peak_vcores', 0):.0f}")),
                dbc.Col(_card("Avg Memory (GB)", f"{peak.get('avg_memory_gb', 0):.1f}")),
            ]),
            dbc.Table.from_dataframe(
                recs_df, striped=True, bordered=True, hover=True, size="sm",
            ) if not recs_df.empty else html.P("No recommendations available."),
        ])
    except Exception:
        sizing_section = html.P("Sizing data unavailable.", className="text-muted")

    # ---- Storage ----
    storage = result.get("storage", {})
    tiers = storage.get("tiers", {})
    tier_cards = []
    for t in ["hot", "cold", "archive"]:
        if t in tiers:
            tier_cards.append(
                dbc.Col(_card(
                    f"{t.title()} ({tiers[t]['pct']}%)",
                    f"${tiers[t]['monthly_cost']:,.2f}/mo",
                    f"{tiers[t]['gb']:,.0f} GB @ ${tiers[t]['price_per_gb']}/GB",
                    color="info",
                ))
            )

    storage_section = html.Div([
        html.H5("Storage Cost Estimate (Tiered)"),
        dbc.Row([
            dbc.Col(_card("HDFS Used", f"{storage.get('hdfs_used_gb', 0):,.0f} GB")),
            dbc.Col(_card("Delta Storage", f"{storage.get('delta_storage_gb', 0):,.0f} GB", color="info")),
        ] + tier_cards + [
            dbc.Col(_card("Annual Storage Cost",
                          f"${storage.get('annual_cost', 0):,.2f}", color="success")),
        ]),
    ])

    return hadoop_section, dbx_section, cards, cost_fig, workload_section, sizing_section, storage_section

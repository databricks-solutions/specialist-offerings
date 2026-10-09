"""Page 6: Migration Timeline — 3-year quarterly migration ramp visualization.

Shows 12-quarter cost timeline, Do Nothing comparison, area chart,
and 3-year summary with payback quarter.
"""

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd

dash.register_page(__name__, path="/migration", name="Migration Timeline", order=5)


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
    html.H2("Migration Timeline", className="mt-3 mb-3"),
    html.P("View 3-year migration costs vs 'Do Nothing' baseline.", className="text-muted"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Select TCO Run"),
            dcc.Dropdown(id="timeline-run-select", placeholder="Select a completed run..."),
        ], width=5),
        dbc.Col([
            dbc.Button("Show Timeline", id="btn-show-timeline", color="primary",
                       className="mt-4"),
        ], width=2),
    ], className="mb-4"),

    html.Hr(),

    dcc.Loading(
        children=[
            # Summary cards
            dbc.Row(id="timeline-summary-cards", className="mb-4"),
            # Area chart
            dcc.Graph(id="timeline-chart", className="mb-4"),
            # Quarterly table
            html.Div(id="timeline-table", className="mb-4"),
            # Export
            dbc.Button("Export Timeline CSV", id="btn-export-timeline", color="secondary",
                       className="mt-2"),
            dcc.Download(id="download-timeline-csv"),
        ],
        type="default",
    ),
], fluid=True)


@callback(
    Output("timeline-run-select", "options"),
    Input("store-catalog", "data"),
    Input("store-schema", "data"),
)
def load_runs(catalog, schema):
    from models.cost_engine import list_runs
    try:
        df = list_runs(catalog, schema)
        return [{"label": f"{r['run_name']} (${r['total_cost_annual']:,.0f})",
                 "value": r["run_id"]}
                for _, r in df.iterrows()]
    except Exception:
        return []


@callback(
    Output("timeline-summary-cards", "children"),
    Output("timeline-chart", "figure"),
    Output("timeline-table", "children"),
    Input("btn-show-timeline", "n_clicks"),
    State("timeline-run-select", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def show_timeline(n_clicks, run_id, catalog, schema):
    empty_fig = go.Figure()
    empty = html.P("", className="text-muted")

    if not run_id:
        return [dbc.Col(html.P("Select a run.", className="text-warning"))], empty_fig, empty

    from models.cost_engine import get_run

    try:
        run = get_run(run_id, catalog, schema)
    except Exception as e:
        return [dbc.Col(html.P(f"Error: {e}", className="text-danger"))], empty_fig, empty

    timeline = run.get("timeline", [])
    if not timeline:
        return [dbc.Col(html.P("No timeline data for this run.", className="text-muted"))], empty_fig, empty

    tl_df = pd.DataFrame(timeline)

    # 3-year summaries from tco_runs columns
    do_nothing_total = float(run.get("three_year_hadoop_total", 0) or 0)
    migration_total = float(run.get("three_year_databricks_total", 0) or 0)
    net_savings = float(run.get("three_year_savings", 0) or 0)

    # If run-level columns are empty, compute from timeline
    if do_nothing_total == 0 and not tl_df.empty:
        hadoop_annual = float(run.get("total_hadoop_cost_annual", 0) or 0)
        do_nothing_total = hadoop_annual * 3
        migration_total = tl_df["total_cost"].sum()
        net_savings = do_nothing_total - migration_total

    # Summary cards
    cards = [
        dbc.Col(_card("Do Nothing (3yr)", f"${do_nothing_total:,.0f}", color="danger")),
        dbc.Col(_card("With Migration (3yr)", f"${migration_total:,.0f}", color="success")),
        dbc.Col(_card("Net Savings (3yr)", f"${net_savings:,.0f}",
                       color="success" if net_savings > 0 else "warning")),
        dbc.Col(_card("Savings %",
                       f"{net_savings / do_nothing_total * 100:.1f}%" if do_nothing_total > 0 else "N/A",
                       color="success")),
    ]

    # Area chart — stacked Hadoop + Databricks + Migration vs Do Nothing line
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=tl_df["quarter_label"], y=tl_df["hadoop_cost"],
        fill="tozeroy", name="Hadoop (declining)",
        line=dict(color="#dc3545"),
    ))
    fig.add_trace(go.Scatter(
        x=tl_df["quarter_label"], y=tl_df["hadoop_cost"] + tl_df["databricks_cost"],
        fill="tonexty", name="Databricks (growing)",
        line=dict(color="#198754"),
    ))
    fig.add_trace(go.Scatter(
        x=tl_df["quarter_label"],
        y=tl_df["hadoop_cost"] + tl_df["databricks_cost"] + tl_df["migration_cost"],
        fill="tonexty", name="Migration Services",
        line=dict(color="#ffc107"),
    ))

    # Do Nothing reference line
    if do_nothing_total > 0:
        quarterly_dn = do_nothing_total / 12
        fig.add_trace(go.Scatter(
            x=tl_df["quarter_label"],
            y=[quarterly_dn] * len(tl_df),
            mode="lines", name="Do Nothing (quarterly)",
            line=dict(color="#6c757d", dash="dash", width=2),
        ))

    fig.update_layout(
        title="Quarterly Cost Timeline: Migration vs Do Nothing",
        xaxis_title="Quarter",
        yaxis_title="Cost ($)",
        margin=dict(t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Quarterly table
    display_df = tl_df[["quarter_label", "migration_pct", "hadoop_cost",
                         "databricks_cost", "migration_cost", "total_cost"]].copy()
    display_df["migration_pct"] = (display_df["migration_pct"] * 100).round(1).astype(str) + "%"
    for col in ["hadoop_cost", "databricks_cost", "migration_cost", "total_cost"]:
        display_df[col] = display_df[col].apply(lambda x: f"${x:,.0f}")

    display_df.columns = ["Quarter", "Migrated %", "Hadoop", "Databricks",
                           "Migration", "Total"]

    table = html.Div([
        html.H5("Quarterly Breakdown"),
        dbc.Table.from_dataframe(display_df, striped=True, bordered=True, hover=True, size="sm"),
    ])

    return cards, fig, table


@callback(
    Output("download-timeline-csv", "data"),
    Input("btn-export-timeline", "n_clicks"),
    State("timeline-run-select", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def export_timeline(n_clicks, run_id, catalog, schema):
    if not run_id:
        return None

    from models.cost_engine import get_run
    run = get_run(run_id, catalog, schema)
    timeline = run.get("timeline", [])
    if not timeline:
        return None

    df = pd.DataFrame(timeline)
    return dcc.send_data_frame(df.to_csv, "migration_timeline.csv", index=False)

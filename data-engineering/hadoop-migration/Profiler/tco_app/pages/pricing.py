"""Page 2: Pricing & SKU Mapping.

Shows current list prices from system.billing.list_prices,
editable SKU mapping table, and price history chart.
"""

import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go

dash.register_page(__name__, path="/pricing", name="Pricing & SKU Mapping", order=1)

layout = dbc.Container([
    html.H2("Pricing & SKU Mapping", className="mt-3 mb-3"),

    # Cloud selector
    dbc.Row([
        dbc.Col([
            dbc.Label("Target Cloud"),
            dcc.Dropdown(
                id="pricing-cloud-select",
                options=[
                    {"label": "AWS", "value": "AWS"},
                    {"label": "Azure", "value": "AZURE"},
                    {"label": "GCP", "value": "GCP"},
                ],
                value="AWS",
            ),
        ], width=3),
        dbc.Col([
            dbc.Button("Snapshot Prices", id="btn-snapshot", color="primary",
                       className="mt-4"),
            html.Span(id="snapshot-status", className="ms-2 text-success"),
        ], width=3),
    ], className="mb-4"),

    # Current prices table
    html.H5("Current List Prices"),
    dcc.Loading(html.Div(id="current-prices-table", className="mb-4"), type="default"),

    html.Hr(),

    # SKU Mapping (editable)
    html.H5("Workload → SKU Mapping"),
    html.P("Edit mappings below. Changes are saved when you click 'Save Mappings'.",
           className="text-muted"),
    dcc.Loading(html.Div(id="sku-mapping-table"), type="default"),
    dbc.Button("Save Mappings", id="btn-save-mappings", color="success",
               className="mt-2 mb-4"),
    html.Span(id="mapping-save-status", className="ms-2"),

    html.Hr(),

    # VM Instance Pricing
    html.H5("VM Instance Pricing"),
    html.P("Refresh live prices from cloud APIs. Azure works without auth; AWS/GCP need credentials.",
           className="text-muted small"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Refresh VM Prices", id="btn-refresh-vm", color="outline-info",
                       className="me-2"),
            html.Span(id="vm-refresh-status", className="ms-2"),
        ], width=6),
    ], className="mb-3"),
    dcc.Loading(html.Div(id="vm-prices-table", className="mb-4"), type="default"),

    html.Hr(),

    # Price history
    html.H5("Price History"),
    dbc.Row([
        dbc.Col([
            dbc.Label("SKU"),
            dcc.Dropdown(id="history-sku-select"),
        ], width=4),
    ], className="mb-3"),
    dcc.Loading(dcc.Graph(id="price-history-chart"), type="default"),
], fluid=True)


@callback(
    Output("current-prices-table", "children"),
    Output("history-sku-select", "options"),
    Input("pricing-cloud-select", "value"),
)
def update_prices(cloud):
    from models.pricing import get_current_prices

    try:
        df = get_current_prices(cloud)
        if df.empty:
            return html.P("No prices found.", className="text-muted"), []

        # Filter to compute-related SKUs for relevance
        compute_skus = df[df["sku_name"].str.contains(
            "COMPUTE|SQL|JOBS|ALL_PURPOSE|SERVERLESS", case=False, na=False
        )]

        display_df = compute_skus[["sku_name", "list_price", "effective_price"]].round(4)
        table = dash_table.DataTable(
            columns=[{"name": c, "id": c} for c in display_df.columns],
            data=display_df.to_dict("records"),
            page_size=10,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px", "fontSize": "13px"},
            style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#f9f9f9"},
            ],
        )

        sku_options = [{"label": s, "value": s} for s in df["sku_name"].tolist()]
        return table, sku_options
    except Exception as e:
        return html.P(f"Error loading prices: {e}", className="text-danger"), []


@callback(
    Output("sku-mapping-table", "children"),
    Input("store-catalog", "data"),
    Input("store-schema", "data"),
)
def load_mapping(catalog, schema):
    from utils.db_connector import execute_query, qualified_table

    try:
        tbl = qualified_table("tco_workload_sku_mapping", catalog, schema)
        df = execute_query(f"SELECT job_type, target_sku, target_sku_alt, compute_category, notes FROM {tbl}")

        return dash_table.DataTable(
            id="mapping-datatable",
            columns=[{"name": c, "id": c, "editable": c != "job_type"}
                     for c in df.columns],
            data=df.to_dict("records"),
            editable=True,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "left", "padding": "8px", "fontSize": "13px"},
            style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
        )
    except Exception as e:
        return html.P(f"Error loading mappings: {e}", className="text-danger")


@callback(
    Output("snapshot-status", "children"),
    Input("btn-snapshot", "n_clicks"),
    State("pricing-cloud-select", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def take_snapshot(n_clicks, cloud, catalog, schema):
    from models.pricing import create_pricing_snapshot
    try:
        sid = create_pricing_snapshot(cloud, catalog, schema)
        return f"Snapshot created: {sid[:8]}..."
    except Exception as e:
        return f"Error: {e}"


@callback(
    Output("mapping-save-status", "children"),
    Input("btn-save-mappings", "n_clicks"),
    State("mapping-datatable", "data"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def save_mappings(n_clicks, rows, catalog, schema):
    from utils.db_connector import execute_statement, qualified_table

    tbl = qualified_table("tco_workload_sku_mapping", catalog, schema)
    try:
        execute_statement(f"DELETE FROM {tbl}")
        for r in rows:
            alt = f"'{r['target_sku_alt']}'" if r.get("target_sku_alt") else "NULL"
            notes = f"'{r['notes']}'" if r.get("notes") else "NULL"
            execute_statement(f"""
                INSERT INTO {tbl} (job_type, target_sku, target_sku_alt, compute_category, notes)
                VALUES ('{r['job_type']}', '{r['target_sku']}', {alt},
                        '{r['compute_category']}', {notes})
            """)
        return html.Span("Saved!", className="text-success")
    except Exception as e:
        return html.Span(f"Error: {e}", className="text-danger")


@callback(
    Output("price-history-chart", "figure"),
    Input("history-sku-select", "value"),
    State("pricing-cloud-select", "value"),
)
def update_history(sku, cloud):
    empty = go.Figure()
    empty.update_layout(annotations=[{
        "text": "Select a SKU to view price history",
        "showarrow": False, "font": {"size": 14}
    }])
    if not sku:
        return empty

    from models.pricing import get_price_history
    try:
        df = get_price_history(sku, cloud)
        if df.empty:
            return empty
        fig = px.line(df, x="price_start_time", y="list_price",
                      title=f"{sku} — {cloud}",
                      markers=True)
        fig.update_layout(margin=dict(t=40, b=20))
        return fig
    except Exception:
        return empty


@callback(
    Output("vm-refresh-status", "children"),
    Output("vm-prices-table", "children"),
    Input("btn-refresh-vm", "n_clicks"),
    State("pricing-cloud-select", "value"),
    State("store-catalog", "data"),
    State("store-schema", "data"),
    prevent_initial_call=True,
)
def refresh_vm_prices(n_clicks, cloud, catalog, schema):
    from utils.cloud_pricing import refresh_vm_prices as do_refresh

    try:
        result = do_refresh(cloud, catalog, schema)
        status = html.Span(
            f"{result['status']}: {result.get('updated_instances', 0)} instances updated "
            f"(fetch_id: {result.get('fetch_id', 'N/A')[:8]}...)",
            className="text-success" if result["status"] == "ok" else "text-warning",
        )
    except Exception as e:
        status = html.Span(f"Error: {e}", className="text-danger")

    # Show current VM prices from lookup table
    try:
        from utils.db_connector import execute_query, qualified_table
        tbl = qualified_table("tco_lookup_vm_instances", catalog, schema)
        df = execute_query(f"""
            SELECT instance_type, vcpus, memory_gb,
                   ROUND(on_demand_price, 4) AS on_demand,
                   ROUND(reserved_price, 4) AS reserved,
                   ROUND(spot_price, 4) AS spot,
                   category, last_refreshed
            FROM {tbl}
            WHERE cloud = '{cloud}'
            ORDER BY category, vcpus
        """)
        if df.empty:
            table = html.P("No VM prices in lookup table.", className="text-muted")
        else:
            table = dbc.Table.from_dataframe(
                df, striped=True, bordered=True, hover=True, size="sm",
            )
    except Exception:
        table = html.P("Could not load VM prices.", className="text-muted")

    return status, table

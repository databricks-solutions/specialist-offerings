"""Hadoop → Databricks TCO Calculator — Dash App Entry Point.

Multi-page Dash application for calculating total cost of ownership
when migrating Hadoop workloads to Databricks.
"""

import os
import logging
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True,
    title="Hadoop → Databricks TCO Calculator",
)

# Sidebar navigation
sidebar = html.Div([
    html.H4("TCO Calculator", className="text-primary mb-3 mt-3"),
    html.Hr(),

    # Catalog / Schema selector
    dbc.Label("Catalog", className="small fw-bold"),
    dbc.Input(id="input-catalog", type="text", value="profiler",
              size="sm", className="mb-2"),
    dbc.Label("Schema", className="small fw-bold"),
    dbc.Input(id="input-schema", type="text", value="demo",
              size="sm", className="mb-2"),
    dbc.Button("Initialize TCO Tables", id="btn-init-schema", color="outline-primary",
               size="sm", className="w-100 mb-1"),
    html.Small(id="init-schema-status", className="d-block mb-3"),

    html.Hr(),

    dbc.Nav(
        [
            dbc.NavLink(
                [html.I(className="bi bi-bar-chart me-2"), "Workload Profile"],
                href="/", active="exact",
            ),
            dbc.NavLink(
                [html.I(className="bi bi-tags me-2"), "Pricing & SKU Mapping"],
                href="/pricing", active="exact",
            ),
            dbc.NavLink(
                [html.I(className="bi bi-sliders me-2"), "Assumptions"],
                href="/assumptions", active="exact",
            ),
            dbc.NavLink(
                [html.I(className="bi bi-calculator me-2"), "TCO Calculator"],
                href="/calculator", active="exact",
            ),
            dbc.NavLink(
                [html.I(className="bi bi-calendar3 me-2"), "Migration Timeline"],
                href="/migration", active="exact",
            ),
            dbc.NavLink(
                [html.I(className="bi bi-columns-gap me-2"), "Scenario Comparison"],
                href="/scenarios", active="exact",
            ),
        ],
        vertical=True,
        pills=True,
    ),
])

app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [sidebar],
                    width=2,
                    className="bg-light vh-100 position-fixed",
                    style={"overflowY": "auto"},
                ),
                dbc.Col(
                    [dash.page_container],
                    width=10,
                    className="ms-auto",
                    style={"marginLeft": "16.67%"},
                ),
            ]
        ),
        # Shared stores — catalog/schema driven by sidebar inputs
        dcc.Store(id="store-catalog", data="profiler"),
        dcc.Store(id="store-schema", data="demo"),
        dcc.Store(id="store-active-assumption"),
        dcc.Store(id="store-active-snapshot"),
    ],
    fluid=True,
)


@callback(
    Output("store-catalog", "data"),
    Output("store-schema", "data"),
    Input("input-catalog", "value"),
    Input("input-schema", "value"),
)
def sync_catalog_schema(catalog, schema):
    return catalog or "profiler", schema or "demo"


@callback(
    Output("init-schema-status", "children"),
    Input("btn-init-schema", "n_clicks"),
    State("input-catalog", "value"),
    State("input-schema", "value"),
    prevent_initial_call=True,
)
def initialize_tco_tables(n_clicks, catalog, schema):
    import traceback
    try:
        from utils.db_connector import init_schema
        init_schema(catalog, schema)
        return html.Span("TCO tables ready.", className="text-success")
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("init_schema failed: %s", tb)
        return html.Pre(
            f"{type(e).__name__}: {e}\n\n{tb}",
            style={"fontSize": "11px", "color": "red",
                   "maxHeight": "400px", "overflowY": "auto"})


if __name__ == "__main__":
    # Databricks Apps sets DATABRICKS_APP_PORT; PORT is the generic fallback,
    # and 8050 is the Dash default for local development.
    port = int(os.getenv("DATABRICKS_APP_PORT") or os.getenv("PORT") or 8050)
    app.run(host="0.0.0.0", port=port, debug=False)

"""Page 1: Workload Profile -- Read-only profiler data summary.

Shows summary cards, job type breakdown, hourly heatmap,
and top users/queues from the profiler data.
Uses pre-aggregated summary views from the DuckDB exporter.
"""

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

dash.register_page(__name__, path="/", name="Workload Profile", order=0)


def _summary_card(title, value, color="primary"):
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, className="card-subtitle mb-2 text-muted"),
            html.H3(value, className=f"text-{color}"),
        ]),
        className="mb-3",
    )


layout = dbc.Container([
    html.H2("Workload Profile", className="mt-3 mb-3"),
    html.P("Read-only view of profiler data loaded into Unity Catalog.",
           className="text-muted"),

    dcc.Loading(children=[
        # Summary cards
        dbc.Row(id="summary-cards", className="mb-4"),

        # Job type breakdown
        dbc.Row([
            dbc.Col([
                html.H5("Job Type Breakdown"),
                dcc.Graph(id="job-type-pie"),
            ], width=5),
            dbc.Col([
                html.H5("Job Type Details"),
                html.Div(id="job-type-table"),
            ], width=7),
        ], className="mb-4"),

        # Hourly heatmap
        dbc.Row([
            dbc.Col([
                html.H5("Hourly Utilization Heatmap"),
                dcc.Graph(id="hourly-heatmap"),
            ]),
        ], className="mb-4"),

        # Top users and queues
        dbc.Row([
            dbc.Col([
                html.H5("Top Users by Resource Consumption"),
                html.Div(id="top-users-table"),
            ], width=6),
            dbc.Col([
                html.H5("Top Queues by Resource Consumption"),
                html.Div(id="top-queues-table"),
            ], width=6),
        ]),
    ], type="default"),
], fluid=True)


@callback(
    Output("summary-cards", "children"),
    Output("job-type-pie", "figure"),
    Output("job-type-table", "children"),
    Output("hourly-heatmap", "figure"),
    Output("top-users-table", "children"),
    Output("top-queues-table", "children"),
    Input("store-catalog", "data"),
    Input("store-schema", "data"),
)
def update_profile(catalog, schema):
    from utils.db_connector import execute_query

    empty_fig = go.Figure()
    empty_fig.update_layout(annotations=[{
        "text": "No data available", "showarrow": False, "font": {"size": 16}
    }])
    no_data = html.P("No data available.", className="text-muted")

    try:
        # Summary stats from pre-aggregated views
        type_df = execute_query(f"""
            SELECT SUM(total_jobs) AS total_apps,
                   ROUND(SUM(total_memory_gb_hours), 1) AS total_gb_hours
            FROM {catalog}.{schema}.workload_summary_by_type
        """)
        user_count = execute_query(f"""
            SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.workload_summary_by_user
        """)
        queue_count = execute_query(f"""
            SELECT COUNT(*) AS cnt FROM {catalog}.{schema}.workload_summary_by_queue
        """)
        row = type_df.iloc[0]
        cards = [
            dbc.Col(_summary_card("Total Applications", f"{int(row['total_apps']):,}")),
            dbc.Col(_summary_card("Total GB-Hours", f"{row['total_gb_hours']:,.1f}", "info")),
            dbc.Col(_summary_card("Unique Users", f"{int(user_count.iloc[0]['cnt']):,}", "success")),
            dbc.Col(_summary_card("Unique Queues", f"{int(queue_count.iloc[0]['cnt']):,}", "warning")),
        ]
    except Exception as e:
        cards = [dbc.Col(html.P(f"Error loading summary: {e}", className="text-danger"))]

    # Job type breakdown
    try:
        job_df = execute_query(f"""
            SELECT job_type,
                   total_jobs AS app_count,
                   ROUND(total_memory_gb_hours, 4) AS total_gb_hours,
                   ROUND(total_cost, 4) AS total_cost
            FROM {catalog}.{schema}.workload_summary_by_type
            ORDER BY total_memory_gb_hours DESC
        """)
        pie_fig = px.pie(job_df, names="job_type", values="total_gb_hours",
                         hole=0.4, color_discrete_sequence=px.colors.qualitative.Set2)
        pie_fig.update_layout(margin=dict(t=20, b=20))

        job_table = dbc.Table.from_dataframe(
            job_df, striped=True, bordered=True, hover=True, size="sm"
        )
    except Exception as e:
        pie_fig = empty_fig
        job_table = html.P(f"Error: {e}", className="text-danger")

    # Hourly heatmap
    try:
        hourly = execute_query(f"""
            SELECT HOUR(hour) AS hour_of_day,
                   DAYOFWEEK(hour) AS day_of_week,
                   ROUND(avg_memory_mb / 1024.0, 1) AS gb_hours
            FROM {catalog}.{schema}.hourly_yarn_view
        """)
        if not hourly.empty:
            pivot = hourly.pivot_table(
                index="day_of_week", columns="hour_of_day",
                values="gb_hours", aggfunc="sum"
            )
            day_labels = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed",
                          5: "Thu", 6: "Fri", 7: "Sat"}
            pivot.index = pivot.index.map(lambda x: day_labels.get(int(x), str(x)))
            heatmap_fig = px.imshow(
                pivot, labels=dict(x="Hour", y="Day of Week", color="Avg Memory GB"),
                color_continuous_scale="YlOrRd", aspect="auto"
            )
            heatmap_fig.update_layout(margin=dict(t=20, b=20))
        else:
            heatmap_fig = empty_fig
    except Exception:
        heatmap_fig = empty_fig

    # Top users
    try:
        users_df = execute_query(f"""
            SELECT user AS user_name,
                   total_jobs AS apps,
                   ROUND(total_memory_gb_hours, 4) AS gb_hours
            FROM {catalog}.{schema}.workload_summary_by_user
            ORDER BY total_memory_gb_hours DESC
            LIMIT 10
        """)
        users_table = dbc.Table.from_dataframe(
            users_df, striped=True, bordered=True, hover=True, size="sm"
        )
    except Exception:
        users_table = no_data

    # Top queues
    try:
        queues_df = execute_query(f"""
            SELECT queue,
                   total_jobs AS apps,
                   ROUND(total_memory_gb_hours, 4) AS gb_hours
            FROM {catalog}.{schema}.workload_summary_by_queue
            ORDER BY total_memory_gb_hours DESC
            LIMIT 10
        """)
        queues_table = dbc.Table.from_dataframe(
            queues_df, striped=True, bordered=True, hover=True, size="sm"
        )
    except Exception:
        queues_table = no_data

    return cards, pie_fig, job_table, heatmap_fig, users_table, queues_table

# Phase 1: Data Ingestion

How data enters the Databricks platform from various sources.

## Patterns

| Source Type | Pattern | When to Use |
|-------------|---------|-------------|
| Files (CSV, JSON, Parquet) | Auto Loader | Schema evolution, incremental processing |
| Fixed-Width Files | Auto Loader + parsing | Mainframe/legacy exports |
| Small Database (<500GB) | JDBC Connector | Daily/hourly batch loads |
| Large Database (>500GB) | Lakeflow Connect CDC | Real-time, low source impact |
| SaaS (Salesforce, SAP) | Lakeflow Connect | Pre-built connectors |
| Rarely-Used Data | Lakehouse Federation | No data movement needed |

## Files

- `auto-loader/` - Streaming file ingestion
- `jdbc-connectors/` - Database batch loads
- `lakeflow-connect/` - Managed CDC ingestion
- `lakehouse-federation/` - Query external data in place

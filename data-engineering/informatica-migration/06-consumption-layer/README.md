# Phase 3: Consumption Layer

How data is delivered to end users and applications.

## Patterns

| Consumer | Pattern | When to Use |
|----------|---------|-------------|
| Data Analysts | AI/BI Dashboards + Genie | Ad-hoc, natural language |
| Business Users | AI/BI Dashboards | Self-service, no SQL |
| Power BI / Tableau | SQL Warehouses | Enterprise BI |
| Web/Mobile Apps | Lakebase | Low-latency lookups |
| Data Scientists | Notebooks + Clusters | ML, full Spark API |

## Key Benefit

**No Oracle/SQL Server needed for BI!**

Old: Informatica → Oracle → Power BI
New: DLT → Gold Tables → SQL Warehouse → Power BI

## Files

- `ai-bi-dashboards/` - Genie natural language queries
- `lakebase/` - Online tables for app serving
- `sql-warehouses/` - BI tool connectivity

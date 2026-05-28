# Informatica PowerCenter to Databricks Lakehouse Migration

Code examples and patterns for migrating from Informatica PowerCenter (On-Prem) to Databricks Lakehouse.

## Structure

| Folder | Description |
|--------|-------------|
| `04-data-ingestion/` | Auto Loader, JDBC, Lakeflow Connect, Federation |
| `05-etl-transformation/` | DLT pipelines, Medallion architecture, Data quality |
| `06-consumption-layer/` | AI/BI, Lakebase, SQL Warehouses |
| `07-use-cases/` | End-to-end migration scenarios |
| `08-verification/` | Dual-run validation patterns |

## Three Sequential Phases

1. **Ingestion** - How data enters the platform
2. **Transformation** - How data is processed (Medallion: Bronze → Silver → Gold)
3. **Consumption** - How data is delivered to end users

## Quick Reference

### Informatica → Databricks Mapping

| Informatica | Databricks |
|-------------|------------|
| PowerCenter Designer | Notebooks + DLT |
| Workflow Manager | Databricks Workflows |
| Repository | Unity Catalog + Git |
| PowerExchange | Lakeflow Connect |
| Integration Service | Job Clusters |
| Update Strategy | Delta Lake MERGE INTO |
| Lookup (LKP) | Broadcast Join |

# Resource Mapping: Ranger → Unity Catalog

## Hive Resource Mappings

| Ranger Resource | UC Securable | Transform Rule |
|----------------|-------------|----------------|
| `database=*` | `CATALOG` or all `SCHEMA`s | Wildcard → grant on catalog or enumerate schemas |
| `database=mydb` | `SCHEMA catalog.mydb` | Prepend catalog name |
| `database=mydb, table=*` | `SCHEMA catalog.mydb` | Wildcard table → grant on schema |
| `database=mydb, table=orders` | `TABLE catalog.mydb.orders` | Full 3-level name |
| `database=mydb, table=orders, column=*` | `TABLE catalog.mydb.orders` | Wildcard column → grant on table |
| `database=mydb, table=orders, column=id` | Column grant | `GRANT SELECT (id) ON TABLE catalog.mydb.orders` |
| `database=*, table=*, column=*` | `CATALOG` | Full wildcard → catalog-level grant |
| `udf=mydb:my_func` | `FUNCTION catalog.mydb.my_func` | 3-level function name |

## HDFS Resource Mappings

| Ranger HDFS Path | UC Securable | Transform Rule |
|-----------------|-------------|----------------|
| `/user/hive/warehouse/` | — | Managed table storage; no separate grant needed |
| `/data/raw/events` | `EXTERNAL LOCATION 'abfss://container@account.dfs.core.windows.net/data/raw/events'` | Map HDFS path to cloud storage URL |
| `/data/*` | `EXTERNAL LOCATION` (pattern) | Wildcard path → flag for manual mapping |
| `hdfs://namenode/path` | `EXTERNAL LOCATION 'cloud://...'` | Strip HDFS scheme, map to cloud URL |

### HDFS Path to Cloud Storage Mapping

The converter cannot automatically determine the cloud storage mapping. Output a template:

```sql
-- TODO: Replace HDFS path with cloud storage URL
-- Original HDFS path: /data/raw/events
-- Example mappings:
--   AWS:   s3://bucket-name/data/raw/events
--   Azure: abfss://container@account.dfs.core.windows.net/data/raw/events
--   GCP:   gs://bucket-name/data/raw/events

GRANT READ FILES ON EXTERNAL LOCATION `raw_events_location` TO `data_readers`;

-- Prerequisite: Create the external location first:
-- CREATE EXTERNAL LOCATION raw_events_location
--   URL 's3://bucket-name/data/raw/events'
--   WITH (STORAGE CREDENTIAL my_credential);
```

## Namespace Transform Rules

### Default Catalog

When converting, use `main` as the default catalog unless the user specifies otherwise:

```
Ranger: database=analytics, table=orders
UC:     main.analytics.orders
```

### Database → Schema Renaming

If the Hive database name conflicts with UC reserved words or naming conventions:

```sql
-- Ranger: database=default
-- UC: main.default is valid, but consider renaming:
-- GRANT SELECT ON SCHEMA main.hive_default TO `group`;
```

### Wildcard Expansion

| Ranger Wildcard | Expansion Strategy |
|----------------|-------------------|
| `database=*` | Grant on catalog (covers all schemas) |
| `table=*` | Grant on schema (covers all tables in that schema) |
| `column=*` | Grant on table (covers all columns) |
| `database=prod_*` | Flag for manual review — UC has no pattern-based grants |
| `table=tmp_*` | Flag for manual review — UC has no pattern-based grants |

Pattern wildcards (`prod_*`, `tmp_*`) cannot be directly mapped. Output:

```sql
-- WARNING: Ranger pattern wildcard 'database=prod_*' has no UC equivalent.
-- List all matching schemas and grant individually:
-- GRANT SELECT ON SCHEMA main.prod_sales TO `group`;
-- GRANT SELECT ON SCHEMA main.prod_inventory TO `group`;
-- ... enumerate all matching schemas
```

## UC Securable Hierarchy

For reference, the UC object hierarchy that grants cascade through:

```
METASTORE
  └── CATALOG
        └── SCHEMA
              ├── TABLE
              │     └── COLUMN (select only)
              ├── VIEW
              ├── FUNCTION
              ├── VOLUME
              └── MODEL

EXTERNAL LOCATION
STORAGE CREDENTIAL
```

A GRANT on a parent cascades to children. For example:
- `GRANT SELECT ON CATALOG main` → grants SELECT on all schemas, tables, views in `main`
- `GRANT SELECT ON SCHEMA main.analytics` → grants SELECT on all tables/views in `main.analytics`

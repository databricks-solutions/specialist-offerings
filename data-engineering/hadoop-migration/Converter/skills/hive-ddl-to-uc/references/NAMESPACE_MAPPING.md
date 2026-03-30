# Namespace Mapping: Hive 2-Level → Unity Catalog 3-Level

## Core Rule

Hive uses a 2-level namespace: `database.table`
Unity Catalog uses a 3-level namespace: `catalog.schema.table`

### Default Mapping Strategy

| Hive | Unity Catalog | Notes |
|------|---------------|-------|
| `default.table_name` | `main.default.table_name` | Use `main` catalog or customer-specified |
| `mydb.table_name` | `main.mydb.table_name` | Database becomes schema |
| `table_name` (no db) | `main.default.table_name` | Assume default schema |

### Multi-Database Mapping

When migrating multiple Hive databases, the recommended approach:

```
Hive:  database_a.table1    →  UC:  prod_catalog.database_a.table1
Hive:  database_b.table2    →  UC:  prod_catalog.database_b.table2
```

### CREATE DATABASE → CREATE SCHEMA

```sql
-- Hive
CREATE DATABASE IF NOT EXISTS analytics
COMMENT 'Analytics data warehouse'
LOCATION '/user/hive/warehouse/analytics.db';

-- Unity Catalog
CREATE SCHEMA IF NOT EXISTS main.analytics
COMMENT 'Analytics data warehouse (migrated from Hive)';
-- Note: LOCATION removed — UC manages storage for managed schemas
```

### USE Statement

```sql
-- Hive
USE analytics;

-- Unity Catalog
USE CATALOG main;
USE SCHEMA analytics;
-- Or: USE main.analytics;
```

### Cross-Database References

```sql
-- Hive
SELECT a.* FROM db1.table1 a JOIN db2.table2 b ON a.id = b.id;

-- Unity Catalog
SELECT a.* FROM main.db1.table1 a JOIN main.db2.table2 b ON a.id = b.id;
```

---
name: emr-hive-to-unity-catalog
description: "Migrate Hive/Glue Data Catalog to Databricks Unity Catalog. Use when: (1) 'migrate Glue catalog', (2) 'Hive to Unity Catalog', (3) 'metastore migration', (4) 'table migration EMR to Databricks', (5) 'convert Hive tables to Delta', (6) migrating any catalog or metastore from AWS to Databricks."
---

# Hive/Glue Data Catalog to Unity Catalog Migration

## Overview

Migrating from AWS Glue Data Catalog (or a standalone Hive metastore) to Databricks Unity Catalog (UC) is a foundational step when moving EMR workloads to Databricks. The Glue Data Catalog serves as the metastore for EMR Spark/Hive jobs, storing database definitions, table schemas, partition metadata, and storage locations. Unity Catalog replaces this with a unified governance layer that provides:

- **Three-level namespace**: `catalog.schema.table` (vs Glue's `database.table`)
- **Centralized governance**: Fine-grained access control, data lineage, audit logging
- **Multi-format support**: Delta, Parquet, ORC, CSV, JSON, Avro, Iceberg
- **Cross-workspace sharing**: Tables accessible across multiple Databricks workspaces
- **Data discovery**: Built-in search, tagging, and documentation

The migration involves exporting metadata from Glue, mapping it to UC constructs, creating the corresponding UC objects, converting table formats where needed, and migrating permissions.

## Critical Rules

1. **Always use Unity Catalog** -- never register tables under the legacy `hive_metastore` catalog. All new tables must be created under a named UC catalog (`catalog.schema.table`).
2. **Always convert to Delta format for managed tables** -- managed tables in UC must be Delta. External tables can remain in their original format (Parquet, ORC, etc.) but Delta is strongly recommended for full governance benefits.
3. **Preserve data lineage** -- maintain clear mapping between source Glue tables and target UC tables. Document the migration in table properties and comments for traceability.
4. **Validate row counts and schema** after every table migration. Never assume a migration succeeded without verification.
5. **Use storage credentials and external locations** for S3 access -- do not rely on instance profiles or cluster-scoped IAM roles for data access in UC.

## Migration Strategy Decision

### Option A: External Tables (Register in Place)

Keep data files in their existing S3 locations. Register them as external tables in Unity Catalog pointing to the same paths.

```sql
CREATE EXTERNAL TABLE uc_catalog.schema.table (
  col1 STRING,
  col2 INT
)
USING PARQUET
LOCATION 's3://existing-bucket/path/to/data';
```

**Best for:**
- Large datasets (multi-TB) where copying is prohibitively slow or expensive
- Data shared with non-Databricks consumers (other tools reading from S3)
- Incremental migration where you need both systems running in parallel
- Data that must stay in a specific S3 location for compliance reasons

**Trade-offs:**
- Data lifecycle not managed by UC (no automatic VACUUM, etc.)
- Must set up external locations and storage credentials
- Format remains as-is (Parquet, ORC) unless explicitly converted

### Option B: Managed Tables (Copy + Convert to Delta)

Create new managed Delta tables in UC using CTAS (CREATE TABLE AS SELECT) from the source data.

```sql
CREATE TABLE uc_catalog.schema.table AS
SELECT * FROM parquet.`s3://existing-bucket/path/to/data`;
```

**Best for:**
- Smaller datasets where copy time is acceptable
- Full Databricks ownership of the data lifecycle
- Maximum UC governance benefits (VACUUM, OPTIMIZE, time travel)
- Clean break from legacy systems

**Trade-offs:**
- Requires data copy (time, compute, storage cost)
- Temporary increase in storage (old + new copies)
- Need to coordinate cutover from old to new tables

### Option C: Hybrid (Recommended)

Use external tables for bronze/raw layers and managed tables for silver/gold layers.

```
Bronze (raw):     External tables pointing to existing S3 locations
Silver (cleaned): Managed Delta tables (CTAS from bronze)
Gold (business):  Managed Delta tables (CTAS from silver)
```

**Best for:** Most migrations. Minimizes data movement for raw data while giving full UC benefits for curated data.

## Step-by-Step Migration Workflow

### Step 1: Export Glue Catalog Metadata

Extract all database, table, partition, and storage metadata from AWS Glue Data Catalog using boto3. See [glue-catalog-export.md](./glue-catalog-export.md) for detailed code.

```bash
python scripts/export_glue_catalog.py --region us-east-1 --output catalog_export.json
```

### Step 2: Create UC Catalog and Schemas

```sql
-- Create a catalog for the migrated data
CREATE CATALOG IF NOT EXISTS migrated_from_emr;
USE CATALOG migrated_from_emr;

-- Create schemas matching Glue databases
CREATE SCHEMA IF NOT EXISTS migrated_from_emr.raw_data
  COMMENT 'Migrated from Glue database: raw_data';

CREATE SCHEMA IF NOT EXISTS migrated_from_emr.processed
  COMMENT 'Migrated from Glue database: processed';
```

### Step 3: Create Storage Credentials and External Locations

```sql
-- Create a storage credential using an IAM role
CREATE STORAGE CREDENTIAL IF NOT EXISTS emr_migration_cred
  WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789012:role/databricks-uc-access');

-- Create external locations for S3 paths
CREATE EXTERNAL LOCATION IF NOT EXISTS emr_raw_data
  URL 's3://my-emr-data-bucket/raw/'
  WITH (STORAGE CREDENTIAL emr_migration_cred)
  COMMENT 'Raw data from EMR pipelines';
```

### Step 4: Migrate Tables

For each table, choose external or managed based on the strategy decision above. See [table-format-conversion.md](./table-format-conversion.md) for format-specific patterns.

```sql
-- External table (register in place)
CREATE EXTERNAL TABLE migrated_from_emr.raw_data.events
  USING PARQUET
  LOCATION 's3://my-emr-data-bucket/raw/events/'
  COMMENT 'Migrated from Glue: raw_data.events';

-- Managed table (copy + convert to Delta)
CREATE TABLE migrated_from_emr.processed.daily_aggregates AS
  SELECT * FROM parquet.`s3://my-emr-data-bucket/processed/daily_aggregates/`;
```

### Step 5: Migrate Views

Views must be recreated with updated references pointing to UC tables.

```sql
-- Original Hive view
-- CREATE VIEW processed.daily_summary AS
--   SELECT date, SUM(amount) FROM raw_data.events GROUP BY date;

-- Migrated UC view
CREATE VIEW migrated_from_emr.processed.daily_summary AS
  SELECT date, SUM(amount)
  FROM migrated_from_emr.raw_data.events
  GROUP BY date;
```

### Step 6: Migrate Permissions

Map AWS IAM / Lake Formation permissions to UC grants. See [permissions-migration.md](./permissions-migration.md) for detailed mapping.

```sql
-- Grant schema-level access
GRANT USE SCHEMA ON SCHEMA migrated_from_emr.raw_data TO `data-engineers`;
GRANT SELECT ON SCHEMA migrated_from_emr.raw_data TO `data-engineers`;

-- Grant table-level access
GRANT SELECT ON TABLE migrated_from_emr.processed.daily_aggregates TO `analysts`;
```

### Step 7: Validate

```sql
-- Compare row counts
SELECT 'source' AS origin, COUNT(*) AS cnt FROM parquet.`s3://my-emr-data-bucket/raw/events/`
UNION ALL
SELECT 'target' AS origin, COUNT(*) AS cnt FROM migrated_from_emr.raw_data.events;

-- Compare schemas
DESCRIBE TABLE EXTENDED migrated_from_emr.raw_data.events;

-- Spot-check data
SELECT * FROM migrated_from_emr.raw_data.events LIMIT 100;
```

## Quick Reference: Glue Concept to UC Concept Mapping

| Glue / Hive Concept | Unity Catalog Equivalent | Notes |
|----------------------|--------------------------|-------|
| Database | Schema | UC adds a catalog level above schema |
| Table | Table | External or Managed |
| Partition | Partition | Delta uses partition pruning; consider liquid clustering for new tables |
| Crawler | Auto Loader | Auto Loader infers and evolves schema automatically |
| Glue Job | Databricks Job | Workflows with tasks (notebook, Python, JAR, SQL) |
| Glue Trigger | Job Schedule / Trigger | Cron-based or file-arrival triggers |
| Lake Formation | UC Grants | Fine-grained GRANT/REVOKE, column masks, row filters |
| Lake Formation Tags | UC Tags | Attribute-based access control via tags |
| Glue Connection | UC Connection | For external data sources (JDBC, etc.) |
| Glue Registry (Avro/Schema) | UC Schema Evolution | Delta handles schema evolution natively |
| S3 Location | External Location | Governed S3 access via storage credentials |
| IAM Role (data access) | Storage Credential | Maps IAM role to UC-managed credential |

## Related Skills

- **emr-migration-assessment** -- Assess EMR workloads before migration
- **emr-spark-code-migration** -- Convert EMR Spark code for Databricks
- **databricks-unity-catalog** -- Unity Catalog best practices and governance

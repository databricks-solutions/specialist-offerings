# Table Format Conversion Patterns

## Overview

When migrating from Hive/Glue to Unity Catalog, tables often need to be converted to Delta format. This document covers conversion patterns for every common source format.

## Parquet to Delta

### In-Place Conversion (No Data Copy)

The fastest option. Adds a Delta transaction log to existing Parquet files without rewriting them.

```sql
-- Convert Parquet files at an S3 path to Delta in place
CONVERT TO DELTA parquet.`s3://my-bucket/data/events/`;

-- With partitioning
CONVERT TO DELTA parquet.`s3://my-bucket/data/events/`
PARTITIONED BY (year INT, month INT, day INT);

-- Then register as an external table in UC
CREATE TABLE uc_catalog.schema.events
USING DELTA
LOCATION 's3://my-bucket/data/events/';
```

**Caveats:**
- Files must be valid Parquet with a consistent schema
- After conversion, only Delta-aware tools can write to this location
- Partition columns must be specified if the data is partitioned

### Copy Conversion (CTAS)

Creates a new managed Delta table by reading and rewriting all data.

```sql
-- Simple CTAS
CREATE TABLE uc_catalog.schema.events AS
SELECT * FROM parquet.`s3://my-bucket/data/events/`;

-- With explicit schema and partitioning
CREATE TABLE uc_catalog.schema.events (
  event_id STRING,
  event_type STRING,
  event_time TIMESTAMP,
  payload STRING,
  year INT,
  month INT
)
USING DELTA
PARTITIONED BY (year, month)
AS SELECT * FROM parquet.`s3://my-bucket/data/events/`;
```

### DEEP CLONE (Large Tables with Incremental Support)

For very large Parquet tables already converted to Delta (or existing Delta tables), use DEEP CLONE for an initial full copy that supports incremental updates later.

```sql
-- First convert in-place to Delta (if Parquet)
CONVERT TO DELTA parquet.`s3://my-bucket/data/events/`
PARTITIONED BY (year INT, month INT);

-- Then deep clone into a managed UC table
CREATE TABLE uc_catalog.schema.events
DEEP CLONE delta.`s3://my-bucket/data/events/`;

-- Incremental clone (subsequent runs -- only copies new files)
CREATE OR REPLACE TABLE uc_catalog.schema.events
DEEP CLONE delta.`s3://my-bucket/data/events/`;
```

## ORC to Delta

ORC does not support in-place CONVERT TO DELTA. Always use CTAS.

```sql
-- Basic CTAS
CREATE TABLE uc_catalog.schema.transactions AS
SELECT * FROM orc.`s3://my-bucket/data/transactions/`;

-- With partitioning
CREATE TABLE uc_catalog.schema.transactions
USING DELTA
PARTITIONED BY (year, month)
AS SELECT * FROM orc.`s3://my-bucket/data/transactions/`;

-- Using a temporary view for complex transformations
CREATE TEMPORARY VIEW orc_source AS
SELECT
  *,
  CAST(event_time AS TIMESTAMP) AS event_ts  -- fix type issues during migration
FROM orc.`s3://my-bucket/data/transactions/`;

CREATE TABLE uc_catalog.schema.transactions AS
SELECT * FROM orc_source;
```

## CSV to Delta

### Schema Inference Considerations

CSV files require careful handling of schema since they have no embedded type information.

```sql
-- Basic CSV read with header and inference
CREATE TABLE uc_catalog.schema.customer_data AS
SELECT * FROM read_files(
  's3://my-bucket/data/customers/',
  format => 'csv',
  header => true,
  inferSchema => true,
  delimiter => ','
);

-- With explicit schema (recommended for production)
CREATE TABLE uc_catalog.schema.customer_data (
  customer_id BIGINT,
  name STRING,
  email STRING,
  signup_date DATE,
  balance DECIMAL(18,2)
)
AS SELECT
  CAST(_c0 AS BIGINT) AS customer_id,
  _c1 AS name,
  _c2 AS email,
  CAST(_c3 AS DATE) AS signup_date,
  CAST(_c4 AS DECIMAL(18,2)) AS balance
FROM read_files(
  's3://my-bucket/data/customers/',
  format => 'csv',
  header => false
);
```

### Header and Delimiter Configuration

```sql
-- Tab-separated values
SELECT * FROM read_files(
  's3://my-bucket/data/tsv_data/',
  format => 'csv',
  header => true,
  delimiter => '\t'
);

-- Pipe-delimited
SELECT * FROM read_files(
  's3://my-bucket/data/pipe_data/',
  format => 'csv',
  header => true,
  delimiter => '|'
);

-- With quote and escape characters
SELECT * FROM read_files(
  's3://my-bucket/data/quoted_csv/',
  format => 'csv',
  header => true,
  delimiter => ',',
  quote => '"',
  escape => '\\'
);
```

## Avro to Delta

### Basic Conversion

```sql
-- CTAS from Avro files
CREATE TABLE uc_catalog.schema.avro_data AS
SELECT * FROM avro.`s3://my-bucket/data/avro_table/`;
```

### Schema Evolution Handling

Avro supports schema evolution natively. When converting to Delta, be aware that:

```sql
-- If Avro files have different schemas across partitions, use mergeSchema
SET spark.databricks.delta.schema.autoMerge.enabled = true;

CREATE TABLE uc_catalog.schema.evolved_data AS
SELECT * FROM avro.`s3://my-bucket/data/avro_evolved/`;
```

### Complex Type Mapping

Avro's complex types generally map well to Delta:

| Avro Type | Delta Type |
|-----------|-----------|
| record | STRUCT |
| array | ARRAY |
| map | MAP |
| union | Nullable type (for ["null", "type"]) or STRUCT with type tags |
| enum | STRING |
| fixed | BINARY |

```sql
-- Avro with nested records becomes Delta with nested STRUCTs
-- No special handling needed; Spark reads Avro complex types natively
CREATE TABLE uc_catalog.schema.nested_avro AS
SELECT * FROM avro.`s3://my-bucket/data/nested_avro/`;
```

## Iceberg to Delta

Supported in Databricks Runtime 13.0+ (newer DBR versions).

```sql
-- In-place conversion from Iceberg to Delta
CONVERT TO DELTA iceberg.`s3://my-bucket/data/iceberg_table/`;

-- Then register in UC
CREATE TABLE uc_catalog.schema.iceberg_migrated
USING DELTA
LOCATION 's3://my-bucket/data/iceberg_table/';

-- Alternative: CTAS if in-place conversion is not available
CREATE TABLE uc_catalog.schema.iceberg_migrated AS
SELECT * FROM iceberg.`s3://my-bucket/data/iceberg_table/`;
```

**Notes:**
- Iceberg metadata (snapshots, history) is not preserved in the conversion
- Partition transforms (e.g., `bucket`, `truncate`) may need to be recreated manually
- Time travel history restarts from the conversion point

## Hudi to Delta

No direct in-place conversion is supported. Use CTAS.

```sql
-- Read Hudi table and write as Delta
CREATE TABLE uc_catalog.schema.hudi_migrated AS
SELECT * FROM hudi.`s3://my-bucket/data/hudi_table/`;

-- For Hudi Copy-on-Write tables
CREATE TABLE uc_catalog.schema.hudi_cow AS
SELECT * FROM parquet.`s3://my-bucket/data/hudi_table/*/`;
-- Note: Reading as Parquet skips Hudi metadata; use the Hudi reader for correctness

-- For incremental/CDC data from Hudi, consider reading with Hudi options
-- then inserting into a Delta table
```

## Large Table Migration Strategies

### Partition-by-Partition Migration

For multi-TB tables, migrate one partition at a time to manage resource usage and enable restartability.

```python
# PySpark: Partition-by-partition migration
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

source_path = "s3://my-bucket/data/large_table/"
target_table = "uc_catalog.schema.large_table"

# Get list of partitions
partitions = spark.read.parquet(source_path).select("year", "month").distinct().collect()

# Create target table with schema (empty)
schema_df = spark.read.parquet(source_path).limit(0)
schema_df.write.format("delta").partitionBy("year", "month").saveAsTable(target_table)

# Migrate partition by partition
for row in sorted(partitions, key=lambda r: (r.year, r.month)):
    year, month = row.year, row.month
    print(f"Migrating partition year={year}/month={month}")

    partition_df = (
        spark.read.parquet(source_path)
        .filter(f"year = {year} AND month = {month}")
    )

    partition_df.write.format("delta").mode("append").saveAsTable(target_table)

    # Verify row count
    source_count = partition_df.count()
    target_count = spark.table(target_table).filter(
        f"year = {year} AND month = {month}"
    ).count()
    assert source_count == target_count, (
        f"Row count mismatch for year={year}/month={month}: "
        f"source={source_count}, target={target_count}"
    )
```

### DEEP CLONE for Incremental Migration

```sql
-- Initial full clone
CREATE TABLE uc_catalog.schema.large_table
DEEP CLONE delta.`s3://my-bucket/data/large_delta_table/`;

-- Subsequent incremental clones (only new files since last clone)
CREATE OR REPLACE TABLE uc_catalog.schema.large_table
DEEP CLONE delta.`s3://my-bucket/data/large_delta_table/`;
```

### Parallel Migration with Multiple Jobs

```python
# Submit multiple migration jobs in parallel using Databricks Jobs API
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import SubmitTask, NotebookTask

w = WorkspaceClient()

tables_to_migrate = [
    ("raw_data", "events"),
    ("raw_data", "clickstream"),
    ("processed", "daily_agg"),
    ("processed", "user_profiles"),
]

# Submit parallel migration jobs
run_ids = []
for db, table in tables_to_migrate:
    run = w.jobs.submit(
        run_name=f"migrate_{db}_{table}",
        tasks=[
            SubmitTask(
                task_key=f"migrate_{table}",
                notebook_task=NotebookTask(
                    notebook_path="/Repos/migration/migrate_table",
                    base_parameters={
                        "source_database": db,
                        "source_table": table,
                        "target_catalog": "uc_catalog",
                        "target_schema": db,
                    },
                ),
                existing_cluster_id="0123-456789-abcdef",
            )
        ],
    )
    run_ids.append(run.run_id)
    print(f"Submitted migration for {db}.{table}: run_id={run.run_id}")
```

## Post-Conversion Optimization

### OPTIMIZE and ZORDER

After migration, optimize the Delta table for query performance.

```sql
-- Compact small files
OPTIMIZE uc_catalog.schema.events;

-- ZORDER by frequently filtered columns
OPTIMIZE uc_catalog.schema.events
ZORDER BY (event_type, event_date);

-- For partitioned tables, optimize specific partitions
OPTIMIZE uc_catalog.schema.events
WHERE year = 2024 AND month = 12
ZORDER BY (event_type);
```

### Liquid Clustering (Recommended for New Tables)

For newly created tables, use liquid clustering instead of ZORDER. It is self-tuning and does not require explicit ZORDER commands.

```sql
-- Create a table with liquid clustering
CREATE TABLE uc_catalog.schema.events_v2 (
  event_id STRING,
  event_type STRING,
  event_date DATE,
  user_id BIGINT,
  payload STRING
)
CLUSTER BY (event_type, event_date);

-- Insert data from migrated table
INSERT INTO uc_catalog.schema.events_v2
SELECT * FROM uc_catalog.schema.events;

-- Trigger clustering (or it happens automatically on writes)
OPTIMIZE uc_catalog.schema.events_v2;
```

### VACUUM to Clean Up Old Files

```sql
-- Remove files older than the default retention period (7 days)
VACUUM uc_catalog.schema.events;

-- Specify custom retention (e.g., 30 days)
VACUUM uc_catalog.schema.events RETAIN 720 HOURS;

-- Dry run to see what would be deleted
VACUUM uc_catalog.schema.events DRY RUN;
```

### Analyze Table for Statistics

```sql
-- Collect statistics for the query optimizer
ANALYZE TABLE uc_catalog.schema.events COMPUTE STATISTICS;

-- Compute statistics for specific columns
ANALYZE TABLE uc_catalog.schema.events
COMPUTE STATISTICS FOR COLUMNS event_type, event_date, user_id;
```

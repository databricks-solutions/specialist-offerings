# Schema Mapping: Hive to Spark/Delta Types

## Core Type Mapping Table

| Hive Type | Spark/Delta Type | Notes |
|-----------|-----------------|-------|
| TINYINT | TINYINT | Direct mapping |
| SMALLINT | SMALLINT | Direct mapping |
| INT | INT | Direct mapping |
| BIGINT | BIGINT | Direct mapping |
| FLOAT | FLOAT | Direct mapping |
| DOUBLE | DOUBLE | Direct mapping |
| DECIMAL(p,s) | DECIMAL(p,s) | Check precision limits |
| STRING | STRING | Direct mapping |
| VARCHAR(n) | STRING | Hive VARCHAR becomes STRING in Spark |
| CHAR(n) | STRING | Hive CHAR becomes STRING in Spark |
| BOOLEAN | BOOLEAN | Direct mapping |
| BINARY | BINARY | Direct mapping |
| DATE | DATE | Direct mapping |
| TIMESTAMP | TIMESTAMP | Watch for timezone handling (Hive TIMESTAMP is timezone-less, Spark TIMESTAMP is session-timezone-dependent) |
| ARRAY\<T\> | ARRAY\<T\> | Recursive type mapping |
| MAP\<K,V\> | MAP\<K,V\> | Recursive type mapping |
| STRUCT\<...\> | STRUCT\<...\> | Field name and type mapping |
| UNIONTYPE | Not supported | Must flatten to STRUCT with type discriminator |

## Complex / Nested Type Migration Patterns

### ARRAY Types

```sql
-- Hive
CREATE TABLE hive_table (
  tags ARRAY<STRING>,
  scores ARRAY<DOUBLE>
);

-- Unity Catalog (Delta) -- identical syntax
CREATE TABLE uc_catalog.schema.table (
  tags ARRAY<STRING>,
  scores ARRAY<DOUBLE>
);
```

Nested arrays work the same way:
```sql
-- ARRAY of STRUCT
CREATE TABLE uc_catalog.schema.table (
  addresses ARRAY<STRUCT<street: STRING, city: STRING, zip: STRING>>
);
```

### MAP Types

```sql
-- Hive
CREATE TABLE hive_table (
  metadata MAP<STRING, STRING>,
  counts MAP<STRING, INT>
);

-- Unity Catalog (Delta) -- identical syntax
CREATE TABLE uc_catalog.schema.table (
  metadata MAP<STRING, STRING>,
  counts MAP<STRING, INT>
);
```

### STRUCT Types

```sql
-- Hive
CREATE TABLE hive_table (
  address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING>
);

-- Unity Catalog (Delta) -- identical syntax
CREATE TABLE uc_catalog.schema.table (
  address STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>
);
```

### Deeply Nested Types

```sql
-- Complex nested structure (same syntax in both Hive and Spark/Delta)
CREATE TABLE uc_catalog.schema.events (
  event_id STRING,
  payload STRUCT<
    user: STRUCT<id: BIGINT, name: STRING, tags: ARRAY<STRING>>,
    items: ARRAY<STRUCT<sku: STRING, qty: INT, price: DECIMAL(10,2)>>,
    metadata: MAP<STRING, STRING>
  >
);
```

### UNIONTYPE Migration

UNIONTYPE is not supported in Spark/Delta. It must be manually converted to a STRUCT with a type discriminator.

```sql
-- Hive UNIONTYPE
CREATE TABLE hive_table (
  value UNIONTYPE<INT, STRING, DOUBLE>
);

-- Migrated to Delta: flatten to STRUCT with tag
CREATE TABLE uc_catalog.schema.table (
  value STRUCT<
    tag: TINYINT,         -- 0=INT, 1=STRING, 2=DOUBLE
    int_value: INT,
    string_value: STRING,
    double_value: DOUBLE
  >
);

-- Migration query to populate the flattened struct
-- This requires custom logic in PySpark since SQL cannot directly
-- decompose a UNIONTYPE. Example:
```

```python
from pyspark.sql import functions as F

# Read the Hive table (Spark can read UNIONTYPE as a struct with tag)
df = spark.table("hive_metastore.db.hive_table")

# The UNIONTYPE is read by Spark as a struct with fields:
#   _tag (byte), _0 (first type), _1 (second type), etc.
migrated_df = df.withColumn("value",
    F.struct(
        F.col("value._tag").alias("tag"),
        F.col("value._0").alias("int_value"),
        F.col("value._1").alias("string_value"),
        F.col("value._2").alias("double_value"),
    )
)

migrated_df.write.format("delta").saveAsTable("uc_catalog.schema.table")
```

## Decimal Precision Edge Cases

Delta Lake supports DECIMAL with precision up to 38 digits (same as Hive). However, be aware of:

```sql
-- Hive allows implicit widening in some operations
-- Spark is stricter about precision overflow

-- Check for any DECIMAL columns with max precision
-- These may overflow during aggregation
SELECT
  col_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'my_schema'
  AND data_type LIKE 'decimal%'
  AND CAST(REGEXP_EXTRACT(data_type, 'decimal\\((\\d+)', 1) AS INT) > 28;
```

Common edge cases:
- **DECIMAL without precision**: Hive defaults to `DECIMAL(10,0)`, Spark defaults to `DECIMAL(10,0)` -- same, but verify.
- **Aggregation overflow**: `SUM(DECIMAL(18,2))` can exceed precision 38. Use `CAST` to widen before aggregation.
- **Division results**: `DECIMAL / DECIMAL` may produce different precision/scale in Spark vs Hive. Test results.

```sql
-- Safeguard: explicitly cast decimals in migration queries
CREATE TABLE uc_catalog.schema.financial_data AS
SELECT
  CAST(amount AS DECIMAL(18,2)) AS amount,
  CAST(rate AS DECIMAL(10,6)) AS rate,
  CAST(total AS DECIMAL(28,2)) AS total
FROM parquet.`s3://my-bucket/data/financial/`;
```

## Timestamp and Timezone Handling

This is the most common source of subtle bugs in Hive-to-Spark migrations.

### The Problem

- **Hive TIMESTAMP**: Represents a point in time without timezone. The value `2024-01-15 10:30:00` means exactly that, regardless of session timezone.
- **Spark TIMESTAMP**: Internally stored as UTC microseconds since epoch. When displayed, it is adjusted to the session timezone (`spark.sql.session.timeZone`).

### Impact

```sql
-- A Hive table with TIMESTAMP column containing '2024-01-15 10:30:00'
-- When read by Spark with session timezone = 'America/New_York':
--   Spark interprets the value as 2024-01-15 10:30:00 EST
--   Internally stores it as 2024-01-15 15:30:00 UTC
--   If you later read it with session timezone = 'UTC', you get 2024-01-15 15:30:00

-- This can shift all your timestamps!
```

### Recommendations

```sql
-- Option 1: Set session timezone to UTC during migration (RECOMMENDED)
SET spark.sql.session.timeZone = 'UTC';

-- Then all timestamps are interpreted as UTC, preserving the raw values
CREATE TABLE uc_catalog.schema.events AS
SELECT * FROM parquet.`s3://my-bucket/data/events/`;

-- Option 2: Use TIMESTAMP_NTZ (no-timezone) type in Databricks Runtime 13.3+
-- This behaves like Hive's TIMESTAMP (timezone-less)
CREATE TABLE uc_catalog.schema.events (
  event_id STRING,
  event_time TIMESTAMP_NTZ  -- no timezone conversion
) AS
SELECT
  event_id,
  CAST(event_time AS TIMESTAMP_NTZ) AS event_time
FROM parquet.`s3://my-bucket/data/events/`;

-- Option 3: Store timezone explicitly in a separate column
CREATE TABLE uc_catalog.schema.events AS
SELECT
  *,
  'UTC' AS event_time_tz  -- document the original timezone assumption
FROM parquet.`s3://my-bucket/data/events/`;
```

### Validation

```sql
-- Compare timestamps before and after migration
-- Run with SET spark.sql.session.timeZone = 'UTC';
SELECT
  source.event_time AS source_ts,
  target.event_time AS target_ts,
  source.event_time = target.event_time AS match
FROM parquet.`s3://original/path/` AS source
JOIN uc_catalog.schema.events AS target
  ON source.event_id = target.event_id
WHERE source.event_time != target.event_time
LIMIT 10;
```

## Null Handling Differences

Hive and Spark generally agree on NULL semantics, but there are edge cases:

| Scenario | Hive Behavior | Spark Behavior |
|----------|--------------|----------------|
| NULL = NULL | NULL (3-valued logic) | NULL (same) |
| NULL in GROUP BY | NULLs grouped together | NULLs grouped together (same) |
| NULL in ORDER BY | NULLS FIRST by default | NULLS LAST for ASC, NULLS FIRST for DESC |
| NULL in COUNT | COUNT(*) includes, COUNT(col) excludes | Same |
| NULL in CONCAT | Returns NULL | Returns NULL (same) |
| Empty string vs NULL | Distinct values | Distinct values (same) |

**Key action item:** If your Hive queries rely on `ORDER BY` with NULLs, add explicit `NULLS FIRST` or `NULLS LAST` to preserve the original ordering behavior.

```sql
-- Hive default: NULLs first in ASC
ORDER BY col ASC  -- Hive: NULLs first

-- Spark default: NULLs last in ASC
ORDER BY col ASC  -- Spark: NULLs last

-- Make explicit to avoid ambiguity
ORDER BY col ASC NULLS FIRST  -- same behavior in both
```

## Character Encoding (UTF-8)

- Both Hive and Spark use UTF-8 as the default string encoding.
- Delta stores all string data as UTF-8.
- **VARCHAR(n) / CHAR(n) to STRING**: When Hive VARCHAR(n) is converted to Spark STRING, the length constraint is lost. If your application relies on length enforcement, add a CHECK constraint in Delta:

```sql
CREATE TABLE uc_catalog.schema.table (
  code STRING CONSTRAINT code_length CHECK (LENGTH(code) <= 10),
  name STRING
);
```

- **Binary data**: If binary columns contain non-UTF-8 data, keep them as BINARY type. Do not cast to STRING.
- **Special characters**: Verify that special characters (accented letters, CJK, emoji) survive migration by spot-checking rows with such data.

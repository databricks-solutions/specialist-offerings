# DDL Transformation Rules: Hive → Unity Catalog

## CREATE TABLE

### Managed Tables

```sql
-- Hive
CREATE TABLE sales (
    id INT,
    product STRING,
    amount DOUBLE,
    sale_date DATE
)
STORED AS ORC;

-- Unity Catalog
CREATE TABLE main.default.sales (
    id INT,
    product STRING,
    amount DOUBLE,
    sale_date DATE
)
USING DELTA;
```

### External Tables

```sql
-- Hive
CREATE EXTERNAL TABLE raw_events (
    event_id STRING,
    payload STRING
)
STORED AS PARQUET
LOCATION '/data/raw/events';

-- Unity Catalog (as managed Delta table — preferred)
CREATE TABLE main.raw.events (
    event_id STRING,
    payload STRING
)
USING DELTA;

-- Unity Catalog (as external table — when keeping original format)
CREATE TABLE main.raw.events (
    event_id STRING,
    payload STRING
)
USING PARQUET
LOCATION 's3://bucket/data/raw/events';
```

### STORED AS Mapping

| Hive Format | UC Equivalent | Recommendation |
|-------------|---------------|----------------|
| `STORED AS ORC` | `USING DELTA` | Convert to Delta |
| `STORED AS PARQUET` | `USING DELTA` or `USING PARQUET` | Prefer Delta |
| `STORED AS TEXTFILE` | `USING DELTA` or `USING CSV` | Convert to Delta |
| `STORED AS AVRO` | `USING DELTA` or `USING AVRO` | Convert to Delta |
| `STORED AS SEQUENCEFILE` | `USING DELTA` | Must convert |
| `STORED AS RCFILE` | `USING DELTA` | Must convert |
| `STORED AS JSONFILE` | `USING DELTA` or `USING JSON` | Convert to Delta |

### Partitioning

```sql
-- Hive (partition columns separate from schema)
CREATE TABLE events (
    event_id STRING,
    payload STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;

-- Unity Catalog (partition columns are part of schema)
CREATE TABLE main.default.events (
    event_id STRING,
    payload STRING,
    year INT,
    month INT,
    day INT
)
USING DELTA
PARTITIONED BY (year, month, day);
```

### CLUSTERED BY / SORTED BY

```sql
-- Hive
CREATE TABLE user_events (
    user_id INT,
    event_type STRING,
    ts TIMESTAMP
)
CLUSTERED BY (user_id) SORTED BY (ts) INTO 32 BUCKETS
STORED AS ORC;

-- Unity Catalog (use liquid clustering instead of bucketing)
CREATE TABLE main.default.user_events (
    user_id INT,
    event_type STRING,
    ts TIMESTAMP
)
USING DELTA
CLUSTER BY (user_id, ts);
-- Note: Delta Liquid Clustering replaces bucketing and Z-ORDER
```

### TBLPROPERTIES

```sql
-- Hive
CREATE TABLE data (id INT)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'transactional'='true',
    'transactional_properties'='insert_only'
);

-- Unity Catalog
CREATE TABLE main.default.data (id INT)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
-- Note: ORC compression, transactional props removed — Delta handles these natively
```

### Properties to Remove

These Hive-specific properties should be dropped:
- `orc.compress`, `orc.stripe.size`, `orc.row.index.stride`
- `parquet.compression`
- `transactional`, `transactional_properties`
- `EXTERNAL` (handled differently in UC)
- `auto.purge`
- `hive.mapred.*` properties

### Properties to Add (recommended)

```sql
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-24'
)
```

## ALTER TABLE

```sql
-- Hive
ALTER TABLE sales ADD COLUMNS (region STRING);
ALTER TABLE sales RENAME TO sales_v2;

-- Unity Catalog (same syntax)
ALTER TABLE main.default.sales ADD COLUMNS (region STRING);
ALTER TABLE main.default.sales RENAME TO main.default.sales_v2;
```

## DROP TABLE

```sql
-- Hive
DROP TABLE IF EXISTS temp_data;

-- Unity Catalog
DROP TABLE IF EXISTS main.default.temp_data;
```

## CREATE VIEW

```sql
-- Hive
CREATE VIEW sales_summary AS
SELECT product, SUM(amount) as total FROM sales GROUP BY product;

-- Unity Catalog
CREATE VIEW main.default.sales_summary AS
SELECT product, SUM(amount) as total FROM main.default.sales GROUP BY product;
```

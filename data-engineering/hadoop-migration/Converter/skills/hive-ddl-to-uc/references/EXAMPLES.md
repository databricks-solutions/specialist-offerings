# Examples: Hive DDL → Unity Catalog DDL

## Example 1: Simple Managed Table

### Before (Hive)
```sql
CREATE TABLE sales (
    order_id INT,
    customer_id INT,
    product STRING,
    amount DECIMAL(10,2),
    order_date DATE
)
COMMENT 'Daily sales transactions'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
```

### After (Unity Catalog)
```sql
CREATE TABLE main.default.sales (
    order_id INT,
    customer_id INT,
    product STRING,
    amount DECIMAL(10,2),
    order_date DATE
)
USING DELTA
COMMENT 'Daily sales transactions'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

## Example 2: Partitioned External Table

### Before (Hive)
```sql
CREATE EXTERNAL TABLE events (
    event_id STRING,
    user_id INT,
    event_type STRING,
    payload STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 'hdfs:///data/events';
```

### After (Unity Catalog)
```sql
CREATE TABLE main.analytics.events (
    event_id STRING,
    user_id INT,
    event_type STRING,
    payload STRING,
    year INT,
    month INT,
    day INT
)
USING DELTA
PARTITIONED BY (year, month, day)
COMMENT 'Migrated from Hive external table';

-- Load existing data:
-- COPY INTO main.analytics.events
--   FROM 's3://datalake/data/events'
--   FILEFORMAT = PARQUET;
```

## Example 3: Bucketed Table with SerDe

### Before (Hive)
```sql
CREATE TABLE user_sessions (
    session_id STRING,
    user_id INT,
    page_views ARRAY<STRING>,
    session_data MAP<STRING, STRING>,
    start_time TIMESTAMP,
    end_time TIMESTAMP
)
CLUSTERED BY (user_id) SORTED BY (start_time) INTO 64 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS ORC
TBLPROPERTIES (
    'transactional'='true',
    'orc.compress'='ZLIB',
    'orc.stripe.size'='67108864'
);
```

### After (Unity Catalog)
```sql
CREATE TABLE main.analytics.user_sessions (
    session_id STRING,
    user_id INT,
    page_views ARRAY<STRING>,
    session_data MAP<STRING, STRING>,
    start_time TIMESTAMP,
    end_time TIMESTAMP
)
USING DELTA
CLUSTER BY (user_id, start_time)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
-- Note: Bucketing replaced by Liquid Clustering
-- Note: ORC-specific and transactional props removed (Delta handles natively)
```

## Example 4: CSV External Table

### Before (Hive)
```sql
CREATE EXTERNAL TABLE csv_import (
    id STRING,
    name STRING,
    email STRING,
    signup_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"'
)
STORED AS TEXTFILE
LOCATION '/data/imports/users';
```

### After (Unity Catalog)
```sql
-- Option A: Keep as CSV (for landing zone)
CREATE TABLE main.staging.csv_import (
    id STRING,
    name STRING,
    email STRING,
    signup_date STRING
)
USING CSV
OPTIONS (sep = ',', quote = '"', header = 'false')
LOCATION 's3://bucket/data/imports/users';

-- Option B: Convert to Delta (recommended)
CREATE TABLE main.staging.csv_import (
    id STRING,
    name STRING,
    email STRING,
    signup_date STRING
)
USING DELTA;

COPY INTO main.staging.csv_import
FROM 's3://bucket/data/imports/users'
FILEFORMAT = CSV
FORMAT_OPTIONS ('sep' = ',', 'quote' = '"', 'header' = 'false');
```

## Example 5: Full Database Migration

### Before (Hive)
```sql
CREATE DATABASE analytics
COMMENT 'Analytics warehouse'
LOCATION '/user/hive/warehouse/analytics.db';

USE analytics;

CREATE TABLE dim_customers (
    customer_id INT,
    name STRING,
    segment STRING
)
STORED AS ORC;

CREATE TABLE fact_orders (
    order_id INT,
    customer_id INT,
    amount DECIMAL(12,2),
    order_date DATE
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC;

CREATE VIEW monthly_revenue AS
SELECT year, month, SUM(amount) as revenue
FROM fact_orders
GROUP BY year, month;
```

### After (Unity Catalog)
```sql
CREATE SCHEMA IF NOT EXISTS main.analytics
COMMENT 'Analytics warehouse (migrated from Hive)';

CREATE TABLE main.analytics.dim_customers (
    customer_id INT,
    name STRING,
    segment STRING
)
USING DELTA;

CREATE TABLE main.analytics.fact_orders (
    order_id INT,
    customer_id INT,
    amount DECIMAL(12,2),
    order_date DATE,
    year INT,
    month INT
)
USING DELTA
PARTITIONED BY (year, month);

CREATE VIEW main.analytics.monthly_revenue AS
SELECT year, month, SUM(amount) as revenue
FROM main.analytics.fact_orders
GROUP BY year, month;
```

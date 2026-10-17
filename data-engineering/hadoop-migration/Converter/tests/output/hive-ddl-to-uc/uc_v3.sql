-- =============================================================================
-- Unity Catalog DDL — Converted from Hive DDL
-- Source: Converter/tests/input/hive-ddl-to-uc/hive_schema.hql
-- Target: catalog=aa_catalog, schema=retail_analytics
-- Migration date: 2026-03-31
-- =============================================================================

-- ----------------------------------------------------------------------------
-- USE statement
-- Hive:  USE retail_analytics;
-- UC:    2-level → 3-level namespace; USE CATALOG + USE SCHEMA
-- ----------------------------------------------------------------------------
USE CATALOG aa_catalog;
USE SCHEMA retail_analytics;

-- ----------------------------------------------------------------------------
-- Database → Schema
-- Hive:  CREATE DATABASE retail_analytics LOCATION 'hdfs://...'
-- UC:    CREATE SCHEMA (LOCATION removed — UC manages storage)
--        Original DBPROPERTIES (owner=data_engineering, environment=production)
--        should be recreated via Unity Catalog tags if needed.
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS aa_catalog.retail_analytics
COMMENT 'Retail analytics data warehouse (migrated from Hive database: retail_analytics)';

-- ----------------------------------------------------------------------------
-- raw_clickstream — External table with JsonSerDe
-- Changes:
--   - EXTERNAL keyword removed (managed Delta table)
--   - JsonSerDe → USING DELTA (load JSON data via Auto Loader / COPY INTO)
--   - PARTITIONED BY (dt, hour) → CLUSTER BY (Liquid Clustering)
--   - Partition columns merged into schema definition
--   - HDFS LOCATION removed (UC-managed storage)
--   - Hive-specific TBLPROPERTIES removed (skip.header.line.count, transient_lastDdlTime)
--   - Delta optimization properties added
-- FLAG: Original data at hdfs://namenode:8020/data/raw/clickstream must be
--       ingested via COPY INTO or Auto Loader after table creation.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.raw_clickstream (
    session_id      STRING,
    user_id         STRING,
    event_type      STRING,
    page_url        STRING,
    referrer_url    STRING,
    user_agent      STRING,
    ip_address      STRING,
    event_timestamp TIMESTAMP,
    event_date      STRING,
    properties      MAP<STRING, STRING>,
    dt              STRING,           -- moved from PARTITIONED BY into schema
    hour            INT               -- moved from PARTITIONED BY into schema
)
USING DELTA
CLUSTER BY (dt, hour)
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- Data loading (run after table creation):
-- COPY INTO aa_catalog.retail_analytics.raw_clickstream
--   FROM 's3://<bucket>/data/raw/clickstream'
--   FILEFORMAT = JSON
--   FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true');

-- ----------------------------------------------------------------------------
-- dim_customers — Managed ORC table with bucketing
-- Changes:
--   - STORED AS ORC → USING DELTA
--   - CLUSTERED BY (customer_id) INTO 16 BUCKETS → CLUSTER BY (Liquid Clustering)
--   - ORC-specific props removed (orc.compress, orc.create.index)
--   - transactional prop removed (Delta is ACID-native)
--   - Delta optimization properties added
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.dim_customers (
    customer_id     BIGINT,
    customer_key    STRING,
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    phone           STRING,
    created_date    DATE,
    updated_date    DATE,
    is_active       BOOLEAN,
    tier            STRING COMMENT 'Customer loyalty tier: bronze, silver, gold, platinum',
    lifetime_value  DECIMAL(12,2)
)
USING DELTA
CLUSTER BY (customer_id)
COMMENT 'Customer dimension table - SCD Type 2'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- ----------------------------------------------------------------------------
-- fact_orders — Partitioned fact table with Parquet storage
-- Changes:
--   - STORED AS PARQUET → USING DELTA
--   - PARTITIONED BY (order_year, order_month) → CLUSTER BY (Liquid Clustering)
--   - Partition columns merged into schema definition
--   - parquet.compression prop removed (Delta handles compression natively)
--   - Delta optimization properties added
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.fact_orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    product_id      BIGINT,
    order_date      DATE,
    quantity         INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(5,2),
    total_amount    DECIMAL(12,2),
    status          STRING,
    payment_method  STRING,
    shipping_address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING, country:STRING>,
    order_year      INT,              -- moved from PARTITIONED BY into schema
    order_month     INT               -- moved from PARTITIONED BY into schema
)
USING DELTA
CLUSTER BY (order_year, order_month)
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- ----------------------------------------------------------------------------
-- product_catalog — Table with complex types (MAP, ARRAY, STRUCT)
-- Changes:
--   - STORED AS ORC → USING DELTA
--   - ORC-specific props removed (orc.compress)
--   - Complex types preserved as-is (Delta supports MAP, ARRAY, STRUCT)
--   - Delta optimization properties added
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.product_catalog (
    product_id      BIGINT,
    sku             STRING,
    name            STRING,
    description     STRING,
    category        STRING,
    subcategory     STRING,
    brand           STRING,
    attributes      MAP<STRING, STRING>,
    tags            ARRAY<STRING>,
    variants        ARRAY<STRUCT<variant_id:STRING, color:STRING, size:STRING, price:DECIMAL(10,2), stock:INT>>,
    images          ARRAY<STRING>,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- ----------------------------------------------------------------------------
-- vw_active_customers — View definition
-- Changes:
--   - 3-level namespace applied to all table references (aa_catalog.retail_analytics.*)
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS aa_catalog.retail_analytics.vw_active_customers AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.tier,
    c.lifetime_value,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spend,
    MAX(o.order_date) AS last_order_date
FROM aa_catalog.retail_analytics.dim_customers c
LEFT JOIN aa_catalog.retail_analytics.fact_orders o ON c.customer_id = o.customer_id
WHERE c.is_active = true
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.tier, c.lifetime_value;

-- ----------------------------------------------------------------------------
-- raw_logs — External table with custom InputFormat/OutputFormat
-- Changes:
--   - EXTERNAL keyword removed (managed Delta table)
--   - Custom INPUTFORMAT/OUTPUTFORMAT removed → USING DELTA
--   - ROW FORMAT DELIMITED removed (not applicable with Delta)
--   - PARTITIONED BY (log_date, log_source) → CLUSTER BY (Liquid Clustering)
--   - Partition columns merged into schema definition
--   - HDFS LOCATION removed
-- FLAG: Custom InputFormat/OutputFormat detected
--       (org.apache.hadoop.mapred.TextInputFormat /
--        org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat)
--       Verify that tab-delimited data is loaded correctly via COPY INTO.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.raw_logs (
    log_line    STRING,
    log_date    STRING,              -- moved from PARTITIONED BY into schema
    log_source  STRING               -- moved from PARTITIONED BY into schema
)
USING DELTA
CLUSTER BY (log_date, log_source)
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- Data loading (run after table creation):
-- COPY INTO aa_catalog.retail_analytics.raw_logs
--   FROM 's3://<bucket>/data/raw/logs'
--   FILEFORMAT = CSV
--   FORMAT_OPTIONS ('sep' = '\t', 'header' = 'false');

-- ----------------------------------------------------------------------------
-- ALTER TABLE ADD PARTITION — Not needed for Delta
-- Delta manages partitions automatically; no explicit ADD PARTITION required.
-- With Liquid Clustering, data layout is optimized automatically on write.
-- Original statements (removed):
--   ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=1);
--   ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=2);
--   ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=3);
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- CREATE INDEX — Not supported in Unity Catalog / Delta
-- Delta uses data skipping, Z-ORDER, and Liquid Clustering instead of indexes.
-- Original statement (removed):
--   CREATE INDEX idx_customer_email ON TABLE dim_customers (email)
--   AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
--   WITH DEFERRED REBUILD;
--
-- Recommendation: If email lookups are frequent, add it to CLUSTER BY:
--   ALTER TABLE aa_catalog.retail_analytics.dim_customers CLUSTER BY (customer_id, email);
-- ----------------------------------------------------------------------------

-- =============================================================================
-- MIGRATION SUMMARY
-- =============================================================================
-- Target namespace: aa_catalog.retail_analytics
--
-- Statements converted:
--   1. USE retail_analytics          → USE CATALOG aa_catalog; USE SCHEMA retail_analytics;
--   2. CREATE DATABASE               → CREATE SCHEMA aa_catalog.retail_analytics (LOCATION removed)
--   3. CREATE EXTERNAL TABLE         → CREATE TABLE USING DELTA (2 tables)
--      raw_clickstream                  JsonSerDe removed; CLUSTER BY (dt, hour); load via COPY INTO
--      raw_logs                         Custom I/O formats removed; CLUSTER BY (log_date, log_source); load via COPY INTO
--   4. CREATE TABLE                  → CREATE TABLE USING DELTA (3 tables)
--      dim_customers                    Bucketing → Liquid Clustering CLUSTER BY (customer_id)
--      fact_orders                      PARTITIONED BY → CLUSTER BY (order_year, order_month)
--      product_catalog                  Complex types preserved
--   5. CREATE VIEW                   → CREATE VIEW with 3-level namespace refs
--   6. ALTER TABLE ADD PARTITION     → Removed (Liquid Clustering auto-manages data layout)
--   7. CREATE INDEX                  → Removed (use Liquid Clustering instead)
--
-- Key change: All PARTITIONED BY and CLUSTERED BY clauses converted to
-- CLUSTER BY (Liquid Clustering) — automatic data layout optimization
-- without the rigidity of static partitions or fixed bucket counts.
--
-- Items requiring manual review:
--   [!] raw_clickstream: Data at hdfs://namenode:8020/data/raw/clickstream
--       needs to be ingested via COPY INTO or Auto Loader
--   [!] raw_logs: Custom InputFormat/OutputFormat — verify tab-delimited
--       loading works correctly via COPY INTO
--   [!] Original DBPROPERTIES (owner=data_engineering, environment=production)
--       should be recreated using Unity Catalog tags if needed
-- =============================================================================

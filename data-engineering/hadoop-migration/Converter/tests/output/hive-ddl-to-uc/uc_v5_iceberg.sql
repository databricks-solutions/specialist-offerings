-- =============================================================================
-- Unity Catalog DDL — Converted from Hive DDL (Managed Iceberg format)
-- Source: Converter/tests/input/hive-ddl-to-uc/hive_schema.hql
-- Target: catalog=aa_catalog, schema=retail_analytics
-- Format: ICEBERG (with DVs and row tracking disabled for Liquid Clustering)
-- Table suffix: _ice
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
-- raw_clickstream_ice — External table with JsonSerDe
-- Changes:
--   - Table renamed: raw_clickstream → raw_clickstream_ice
--   - EXTERNAL keyword removed (managed Iceberg table)
--   - JsonSerDe → USING ICEBERG (load JSON data via Auto Loader / COPY INTO)
--   - PARTITIONED BY (dt, hour) → CLUSTER BY (Liquid Clustering)
--   - Partition columns merged into schema definition
--   - HDFS LOCATION removed (UC-managed storage)
--   - Hive-specific TBLPROPERTIES removed (skip.header.line.count, transient_lastDdlTime)
--   - Deletion vectors and row tracking disabled (required for Iceberg v2 + Liquid Clustering)
-- FLAG: Original data at hdfs://namenode:8020/data/raw/clickstream must be
--       ingested via COPY INTO or Auto Loader after table creation.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.raw_clickstream_ice (
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
USING ICEBERG
CLUSTER BY (dt, hour)
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = false,
    'delta.enableRowTracking' = false,
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);
-- Note: Iceberg v2 does not support deletion vectors or row tracking.
-- These must be disabled to enable Liquid Clustering on managed Iceberg tables.

-- Data loading (run after table creation):
-- COPY INTO aa_catalog.retail_analytics.raw_clickstream_ice
--   FROM 's3://<bucket>/data/raw/clickstream'
--   FILEFORMAT = JSON
--   FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true');

-- ----------------------------------------------------------------------------
-- dim_customers_ice — Managed ORC table with bucketing
-- Changes:
--   - Table renamed: dim_customers → dim_customers_ice
--   - STORED AS ORC → USING ICEBERG
--   - CLUSTERED BY (customer_id) INTO 16 BUCKETS → CLUSTER BY (Liquid Clustering)
--   - ORC-specific props removed (orc.compress, orc.create.index)
--   - transactional prop removed (Iceberg provides ACID natively)
--   - Deletion vectors and row tracking disabled (required for Iceberg v2 + Liquid Clustering)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.dim_customers_ice (
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
USING ICEBERG
CLUSTER BY (customer_id)
COMMENT 'Customer dimension table - SCD Type 2'
TBLPROPERTIES (
    'delta.enableDeletionVectors' = false,
    'delta.enableRowTracking' = false,
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- ----------------------------------------------------------------------------
-- fact_orders_ice — Partitioned fact table with Parquet storage
-- Changes:
--   - Table renamed: fact_orders → fact_orders_ice
--   - STORED AS PARQUET → USING ICEBERG
--   - PARTITIONED BY (order_year, order_month) → CLUSTER BY (Liquid Clustering)
--   - Partition columns merged into schema definition
--   - parquet.compression prop removed (Iceberg handles compression natively)
--   - Deletion vectors and row tracking disabled (required for Iceberg v2 + Liquid Clustering)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.fact_orders_ice (
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
USING ICEBERG
CLUSTER BY (order_year, order_month)
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = false,
    'delta.enableRowTracking' = false,
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- ----------------------------------------------------------------------------
-- product_catalog_ice — Table with complex types (MAP, ARRAY, STRUCT)
-- Changes:
--   - Table renamed: product_catalog → product_catalog_ice
--   - STORED AS ORC → USING ICEBERG
--   - ORC-specific props removed (orc.compress)
--   - Complex types preserved as-is (Iceberg supports MAP, ARRAY, STRUCT)
--   - No CLUSTER BY → no need to disable DVs/row tracking
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.product_catalog_ice (
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
USING ICEBERG
TBLPROPERTIES (
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);
-- Note: No CLUSTER BY on this table, so deletion vectors / row tracking
-- properties are not needed.

-- ----------------------------------------------------------------------------
-- vw_active_customers — View definition
-- Changes:
--   - 3-level namespace applied to all table references (aa_catalog.retail_analytics.*)
--   - Table references updated to _ice suffixed names
--   - Views are not format-specific; no _ice suffix on the view name
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
FROM aa_catalog.retail_analytics.dim_customers_ice c
LEFT JOIN aa_catalog.retail_analytics.fact_orders_ice o ON c.customer_id = o.customer_id
WHERE c.is_active = true
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.tier, c.lifetime_value;

-- ----------------------------------------------------------------------------
-- raw_logs_ice — External table with custom InputFormat/OutputFormat
-- Changes:
--   - Table renamed: raw_logs → raw_logs_ice
--   - EXTERNAL keyword removed (managed Iceberg table)
--   - Custom INPUTFORMAT/OUTPUTFORMAT removed → USING ICEBERG
--   - ROW FORMAT DELIMITED removed (not applicable with Iceberg)
--   - PARTITIONED BY (log_date, log_source) → CLUSTER BY (Liquid Clustering)
--   - Partition columns merged into schema definition
--   - HDFS LOCATION removed
--   - Deletion vectors and row tracking disabled (required for Iceberg v2 + Liquid Clustering)
-- FLAG: Custom InputFormat/OutputFormat detected
--       (org.apache.hadoop.mapred.TextInputFormat /
--        org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat)
--       Verify that tab-delimited data is loaded correctly via COPY INTO.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.raw_logs_ice (
    log_line    STRING,
    log_date    STRING,              -- moved from PARTITIONED BY into schema
    log_source  STRING               -- moved from PARTITIONED BY into schema
)
USING ICEBERG
CLUSTER BY (log_date, log_source)
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = false,
    'delta.enableRowTracking' = false,
    'migrated_from' = 'hive',
    'migration_date' = '2026-03-31'
);

-- Data loading (run after table creation):
-- COPY INTO aa_catalog.retail_analytics.raw_logs_ice
--   FROM 's3://<bucket>/data/raw/logs'
--   FILEFORMAT = CSV
--   FORMAT_OPTIONS ('sep' = '\t', 'header' = 'false');

-- ----------------------------------------------------------------------------
-- ALTER TABLE ADD PARTITION — Not needed for Iceberg
-- Iceberg manages partitions automatically; no explicit ADD PARTITION required.
-- With Liquid Clustering, data layout is optimized automatically on write.
-- Original statements (removed):
--   ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=1);
--   ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=2);
--   ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=3);
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- CREATE INDEX — Not supported in Unity Catalog / Iceberg
-- Iceberg uses hidden partitioning and metadata for data skipping.
-- Liquid Clustering provides additional data layout optimization.
-- Original statement (removed):
--   CREATE INDEX idx_customer_email ON TABLE dim_customers (email)
--   AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
--   WITH DEFERRED REBUILD;
--
-- Recommendation: If email lookups are frequent, add it to CLUSTER BY:
--   ALTER TABLE aa_catalog.retail_analytics.dim_customers_ice CLUSTER BY (customer_id, email);
--   (Remember: deletion vectors and row tracking must remain disabled)
-- ----------------------------------------------------------------------------

-- =============================================================================
-- MIGRATION SUMMARY
-- =============================================================================
-- Target namespace: aa_catalog.retail_analytics
-- Format: ICEBERG (managed, all tables)
-- Table suffix: _ice
--
-- Statements converted:
--   1. USE retail_analytics          → USE CATALOG aa_catalog; USE SCHEMA retail_analytics;
--   2. CREATE DATABASE               → CREATE SCHEMA aa_catalog.retail_analytics (LOCATION removed)
--   3. CREATE EXTERNAL TABLE         → CREATE TABLE USING ICEBERG (2 tables)
--      raw_clickstream_ice              JsonSerDe removed; CLUSTER BY (dt, hour); load via COPY INTO
--      raw_logs_ice                     Custom I/O formats removed; CLUSTER BY (log_date, log_source); load via COPY INTO
--   4. CREATE TABLE                  → CREATE TABLE USING ICEBERG (3 tables)
--      dim_customers_ice                Bucketing → Liquid Clustering CLUSTER BY (customer_id)
--      fact_orders_ice                  PARTITIONED BY → CLUSTER BY (order_year, order_month)
--      product_catalog_ice              Complex types preserved (no clustering)
--   5. CREATE VIEW                   → CREATE VIEW with 3-level namespace refs
--                                      (references updated to _ice table names)
--   6. ALTER TABLE ADD PARTITION     → Removed (Liquid Clustering auto-manages data layout)
--   7. CREATE INDEX                  → Removed (use Liquid Clustering instead)
--
-- Iceberg v2 Liquid Clustering requirement:
--   Tables with CLUSTER BY must include:
--     'delta.enableDeletionVectors' = false
--     'delta.enableRowTracking' = false
--   This disables deletion vectors and row tracking (unsupported in Iceberg v2)
--   to allow Liquid Clustering with reduced concurrency control.
--   Applied to: raw_clickstream_ice, dim_customers_ice, fact_orders_ice, raw_logs_ice
--   NOT applied to: product_catalog_ice (no CLUSTER BY)
--
-- Items requiring manual review:
--   [!] raw_clickstream_ice: Data at hdfs://namenode:8020/data/raw/clickstream
--       needs to be ingested via COPY INTO or Auto Loader
--   [!] raw_logs_ice: Custom InputFormat/OutputFormat — verify tab-delimited
--       loading works correctly via COPY INTO
--   [!] Original DBPROPERTIES (owner=data_engineering, environment=production)
--       should be recreated using Unity Catalog tags if needed
--   [!] Reduced concurrency: With DVs and row tracking disabled, row-level
--       concurrency control is not available on clustered Iceberg tables.
--       Concurrent writes will use table-level conflict resolution.
-- =============================================================================

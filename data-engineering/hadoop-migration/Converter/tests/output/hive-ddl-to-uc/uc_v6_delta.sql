-- =============================================================================
-- Unity Catalog DDL — Converted from Hive (retail_analytics)
-- Config: catalog=aa_catalog, schema=retail_analytics, format=DELTA,
--         naming=unchanged, clustering=liquid
-- =============================================================================

-- Database → Schema
-- Hive: CREATE DATABASE retail_analytics ... LOCATION 'hdfs://...'
-- UC: LOCATION removed — Unity Catalog manages storage for managed schemas
CREATE SCHEMA IF NOT EXISTS aa_catalog.retail_analytics
COMMENT 'Retail analytics data warehouse (migrated from Hive)'
WITH DBPROPERTIES (
    'owner' = 'data_engineering',
    'environment' = 'production',
    'migrated_from' = 'hive'
);

-- USE database → USE CATALOG + USE SCHEMA
USE CATALOG aa_catalog;
USE SCHEMA retail_analytics;

-- =============================================================================
-- Table 1: raw_clickstream
-- Source: EXTERNAL TABLE with JsonSerDe, PARTITIONED BY (dt, hour), TEXTFILE
-- Changes:
--   [a] Namespace: retail_analytics.raw_clickstream → aa_catalog.retail_analytics.raw_clickstream
--   [b] STORED AS TEXTFILE → USING DELTA
--   [c] LOCATION removed (managed table)
--   [d] JsonSerDe removed (Delta handles JSON ingestion via COPY INTO / Auto Loader)
--   [e] skip.header.line.count, transient_lastDdlTime removed (Hive-specific)
--   [f] PARTITIONED BY (dt, hour) → CLUSTER BY (dt, hour); columns merged into schema
--   [g] Lineage TBLPROPERTIES added
-- =============================================================================
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
    dt              STRING,    -- formerly partition column
    hour            INT        -- formerly partition column
)
USING DELTA
CLUSTER BY (dt, hour)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'original_format' = 'TEXTFILE/JsonSerDe'
);
-- MANUAL REVIEW: Data loading — use COPY INTO or Auto Loader to ingest from
-- original HDFS path hdfs://namenode:8020/data/raw/clickstream with JSON parsing

-- =============================================================================
-- Table 2: dim_customers
-- Source: Managed table, ORC, CLUSTERED BY (customer_id) INTO 16 BUCKETS
-- Changes:
--   [a] Namespace: → aa_catalog.retail_analytics.dim_customers
--   [b] STORED AS ORC → USING DELTA
--   [d] OrcSerde implicit — removed
--   [e] orc.compress, orc.create.index, transactional removed (Delta handles natively)
--   [f] CLUSTERED BY ... INTO BUCKETS → CLUSTER BY (Liquid Clustering)
--   [g] Lineage TBLPROPERTIES added
-- =============================================================================
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
COMMENT 'Customer dimension table - SCD Type 2'
CLUSTER BY (customer_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'original_format' = 'ORC'
);
-- Note: CLUSTERED BY (customer_id) INTO 16 BUCKETS replaced by Liquid Clustering

-- =============================================================================
-- Table 3: fact_orders
-- Source: Managed table, Parquet, PARTITIONED BY (order_year, order_month)
-- Changes:
--   [a] Namespace: → aa_catalog.retail_analytics.fact_orders
--   [b] STORED AS PARQUET → USING DELTA
--   [e] parquet.compression removed (Delta handles natively)
--   [f] PARTITIONED BY (order_year, order_month) → CLUSTER BY; columns merged into schema
--   [g] Lineage TBLPROPERTIES added
-- =============================================================================
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.fact_orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    product_id      BIGINT,
    order_date      DATE,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(5,2),
    total_amount    DECIMAL(12,2),
    status          STRING,
    payment_method  STRING,
    shipping_address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING, country:STRING>,
    order_year      INT,    -- formerly partition column
    order_month     INT     -- formerly partition column
)
USING DELTA
CLUSTER BY (order_year, order_month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'original_format' = 'PARQUET'
);
-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)

-- =============================================================================
-- Table 4: product_catalog
-- Source: Managed table, ORC, no partitioning
-- Changes:
--   [a] Namespace: → aa_catalog.retail_analytics.product_catalog
--   [b] STORED AS ORC → USING DELTA
--   [e] orc.compress removed
--   [g] Lineage TBLPROPERTIES added
-- =============================================================================
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
    'original_format' = 'ORC'
);

-- =============================================================================
-- View: vw_active_customers
-- Changes:
--   [a] All table references updated to 3-level namespace
-- =============================================================================
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

-- =============================================================================
-- Table 5: raw_logs
-- Source: EXTERNAL TABLE, custom InputFormat/OutputFormat, PARTITIONED BY (log_date, log_source)
-- Changes:
--   [a] Namespace: → aa_catalog.retail_analytics.raw_logs
--   [b] STORED AS INPUTFORMAT/OUTPUTFORMAT → USING DELTA
--   [c] LOCATION removed (managed table)
--   [d] ROW FORMAT DELIMITED removed (Delta handles natively)
--   [f] PARTITIONED BY (log_date, log_source) → CLUSTER BY; columns merged into schema
--   [g] Lineage TBLPROPERTIES added
-- =============================================================================
CREATE TABLE IF NOT EXISTS aa_catalog.retail_analytics.raw_logs (
    log_line    STRING,
    log_date    STRING,    -- formerly partition column
    log_source  STRING     -- formerly partition column
)
USING DELTA
CLUSTER BY (log_date, log_source)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'migrated_from' = 'hive',
    'original_format' = 'TEXTFILE/CustomInputFormat'
);
-- MANUAL REVIEW: Custom InputFormat (TextInputFormat) / OutputFormat (HiveIgnoreKeyTextOutputFormat)
-- Data loading — use COPY INTO or Auto Loader from hdfs://namenode:8020/data/raw/logs

-- =============================================================================
-- ALTER TABLE — Partition additions (no longer needed with Liquid Clustering)
-- =============================================================================
-- SKIPPED: ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=1);
-- SKIPPED: ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=2);
-- SKIPPED: ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=3);
-- Note: Liquid Clustering does not require explicit partition management.
-- Data is automatically organized by cluster keys.

-- =============================================================================
-- Index (deprecated) — not supported in Unity Catalog
-- =============================================================================
-- SKIPPED: CREATE INDEX idx_customer_email ON TABLE dim_customers (email) ...
-- Note: Hive indexes are deprecated since Hive 3.x and have no UC equivalent.
-- Liquid Clustering on relevant columns provides similar query acceleration.

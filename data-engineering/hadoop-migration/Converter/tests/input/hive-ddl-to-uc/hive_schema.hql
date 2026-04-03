-- Hive DDL statements for a retail analytics data warehouse

-- Use database statement
USE retail_analytics;

-- External table with custom SerDe and storage format
CREATE EXTERNAL TABLE IF NOT EXISTS raw_clickstream (
    session_id      STRING,
    user_id         STRING,
    event_type      STRING,
    page_url        STRING,
    referrer_url    STRING,
    user_agent      STRING,
    ip_address      STRING,
    event_timestamp TIMESTAMP,
    event_date      STRING,
    properties      MAP<STRING, STRING>
)
PARTITIONED BY (dt STRING, hour INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/data/raw/clickstream'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'transient_lastDdlTime' = '1640000000'
);

-- Managed ORC table with bucketing
CREATE TABLE IF NOT EXISTS dim_customers (
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
COMMENT 'Customer dimension table - SCD Type 2'
CLUSTERED BY (customer_id) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.create.index' = 'true',
    'transactional' = 'true'
);

-- Partitioned fact table with Parquet storage
CREATE TABLE IF NOT EXISTS fact_orders (
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
    shipping_address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING, country:STRING>
)
PARTITIONED BY (order_year INT, order_month INT)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- Table with complex types
CREATE TABLE IF NOT EXISTS product_catalog (
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
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'ZLIB'
);

-- View definition
CREATE VIEW IF NOT EXISTS vw_active_customers AS
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
FROM dim_customers c
LEFT JOIN fact_orders o ON c.customer_id = o.customer_id
WHERE c.is_active = true
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.tier, c.lifetime_value;

-- Table with custom InputFormat/OutputFormat
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
    log_line STRING
)
PARTITIONED BY (log_date STRING, log_source STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs://namenode:8020/data/raw/logs';

-- Database-level operations
CREATE DATABASE IF NOT EXISTS retail_analytics
COMMENT 'Retail analytics data warehouse'
LOCATION 'hdfs://namenode:8020/warehouse/retail_analytics'
WITH DBPROPERTIES ('owner' = 'data_engineering', 'environment' = 'production');

ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=1);
ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=2);
ALTER TABLE fact_orders ADD PARTITION (order_year=2024, order_month=3);

-- Index (deprecated in Hive 3.x but still found in legacy systems)
CREATE INDEX idx_customer_email ON TABLE dim_customers (email)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;

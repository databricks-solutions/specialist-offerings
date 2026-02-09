-- =============================================================================
-- USE CASE 2: File → ETL → Power BI (No Oracle!)
-- =============================================================================
-- STEP 2: DLT ETL PIPELINE (Deploy as DLT Pipeline)
--
-- SCENARIO:
-- Transform sales data from Bronze to Gold using Medallion architecture.
-- Power BI connects directly to Gold tables via SQL Warehouse.
--
-- OLD PATTERN:
--   Informatica mapping → Oracle staging → Oracle DW → Power BI
--   (Multiple hops, ETL contention with BI)
--
-- NEW PATTERN:
--   Bronze → Silver (DLT) → Gold (DLT) → SQL Warehouse → Power BI
--   (Single pipeline, isolated BI compute)
--
-- POWER BI SETUP:
-- 1. Create SQL Warehouse in Databricks
-- 2. In Power BI: Get Data → Databricks
-- 3. Enter: Server hostname, HTTP path
-- 4. Select tables: gold_daily_sales, gold_dim_store, etc.
-- 5. Build dashboards!
-- =============================================================================


-- =============================================================================
-- SILVER: Cleaned and Validated Sales
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_sales (
  -- Quality constraints
  CONSTRAINT valid_sale_id EXPECT (sale_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (sale_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_store EXPECT (store_id IS NOT NULL) ON VIOLATION DROP ROW,

  -- Business rule warnings
  CONSTRAINT reasonable_amount EXPECT (amount < 100000),
  CONSTRAINT reasonable_quantity EXPECT (quantity > 0 AND quantity < 1000)
)
COMMENT "Validated sales transactions ready for analytics"
AS SELECT
  -- Core fields
  CAST(sale_id AS BIGINT) as sale_id,
  CAST(store_id AS INT) as store_id,
  CAST(customer_id AS BIGINT) as customer_id,
  CAST(product_id AS BIGINT) as product_id,

  -- Metrics
  CAST(amount AS DECIMAL(10,2)) as amount,
  CAST(quantity AS INT) as quantity,
  CAST(discount AS DECIMAL(5,2)) as discount,

  -- Dates
  TO_DATE(sale_date) as sale_date,
  TO_TIMESTAMP(sale_timestamp) as sale_timestamp,

  -- Calculated
  CAST(amount AS DECIMAL(10,2)) * (1 - COALESCE(CAST(discount AS DECIMAL(5,2)), 0)) as net_amount,

  -- Metadata
  _ingestion_timestamp,
  current_timestamp() as processed_timestamp
FROM STREAM(bronze_sales);


-- =============================================================================
-- GOLD: Daily Sales Summary (Primary Power BI table)
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_daily_sales
COMMENT "Daily sales aggregates for Power BI dashboards"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  sale_date,
  store_id,
  COUNT(sale_id) as transaction_count,
  SUM(amount) as gross_sales,
  SUM(net_amount) as net_sales,
  SUM(quantity) as total_units,
  AVG(amount) as avg_transaction_value,
  COUNT(DISTINCT customer_id) as unique_customers
FROM LIVE.silver_sales
GROUP BY sale_date, store_id;


-- =============================================================================
-- GOLD: Store Dimension (for Power BI joins)
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_dim_store
COMMENT "Store dimension for Power BI"
AS SELECT DISTINCT
  store_id,
  store_name,
  store_region,
  store_city,
  store_state,
  store_manager,
  store_open_date
FROM LIVE.silver_stores
WHERE is_active = TRUE;


-- =============================================================================
-- GOLD: Product Dimension (for Power BI joins)
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_dim_product
COMMENT "Product dimension for Power BI"
AS SELECT DISTINCT
  product_id,
  product_name,
  category as product_category,
  subcategory as product_subcategory,
  brand,
  unit_cost,
  unit_price
FROM LIVE.silver_products
WHERE is_active = TRUE;


-- =============================================================================
-- GOLD: Time Intelligence Table (for DAX calculations)
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_dim_date
COMMENT "Date dimension for Power BI time intelligence"
AS
WITH date_range AS (
  SELECT explode(sequence(
    DATE '2020-01-01',
    DATE_ADD(CURRENT_DATE, 365),
    INTERVAL 1 DAY
  )) as date_id
)
SELECT
  date_id,
  YEAR(date_id) as year,
  QUARTER(date_id) as quarter,
  MONTH(date_id) as month,
  DAY(date_id) as day,
  DAYOFWEEK(date_id) as day_of_week,
  DATE_FORMAT(date_id, 'EEEE') as day_name,
  DATE_FORMAT(date_id, 'MMMM') as month_name,
  WEEKOFYEAR(date_id) as week_of_year,
  CASE WHEN DAYOFWEEK(date_id) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as day_type,
  DATE_FORMAT(date_id, 'yyyy-MM') as year_month,
  CONCAT('Q', QUARTER(date_id), ' ', YEAR(date_id)) as year_quarter
FROM date_range;

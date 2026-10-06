-- =============================================================================
-- Delta Live Tables (DLT): SQL Pipeline Definition
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Defines declarative data pipeline using SQL
-- - Same capabilities as Python DLT but in SQL syntax
-- - Ideal for SQL-savvy teams migrating from Informatica
--
-- REPLACES IN INFORMATICA:
-- - SQL Transformation in mappings
-- - Aggregator, Filter, Expression transformations
-- - Session/Workflow scheduling
--
-- TABLE TYPES:
-- - STREAMING LIVE TABLE: Incremental processing (append-only sources)
-- - LIVE TABLE: Full refresh on each run (aggregates, dimensions)
--
-- EXPECTATION ACTIONS:
-- | Syntax                         | Behavior                        |
-- |--------------------------------|---------------------------------|
-- | EXPECT (expr)                  | Log violation, continue         |
-- | EXPECT (expr) ON VIOLATION DROP ROW | Drop bad rows, continue    |
-- | EXPECT (expr) ON VIOLATION FAIL ALL | Stop entire pipeline       |
--
-- STREAMING vs NON-STREAMING:
-- - STREAM(table): Read incrementally (for Bronze → Silver)
-- - LIVE.table: Read full table (for Silver → Gold aggregates)
-- =============================================================================


-- =============================================================================
-- SILVER: Cleaned sales data with quality constraints
-- =============================================================================
CREATE OR REFRESH STREAMING LIVE TABLE silver_sales (
  -- Quality expectations as table constraints
  CONSTRAINT valid_sale_id EXPECT (sale_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (sale_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_store EXPECT (store_id IS NOT NULL) ON VIOLATION DROP ROW,

  -- Warning only (logged but rows kept)
  CONSTRAINT reasonable_amount EXPECT (amount < 100000)
)
COMMENT "Validated and cleaned sales transactions"
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  sale_id,
  store_id,
  product_id,
  customer_id,
  CAST(amount AS DECIMAL(10,2)) as amount,
  CAST(quantity AS INT) as quantity,
  TO_DATE(sale_date) as sale_date,
  current_timestamp() as processed_timestamp
FROM STREAM(bronze_sales);  -- Incremental from Bronze


-- =============================================================================
-- GOLD: Daily sales aggregates by store (for BI dashboards)
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_daily_sales_by_store
COMMENT "Daily aggregated sales metrics by store - ready for Power BI"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  store_id,
  sale_date,
  COUNT(sale_id) as transaction_count,
  SUM(amount) as total_sales,
  SUM(quantity) as total_units,
  AVG(amount) as avg_transaction_value,
  MIN(amount) as min_transaction,
  MAX(amount) as max_transaction
FROM LIVE.silver_sales  -- Full read from Silver
GROUP BY store_id, sale_date;


-- =============================================================================
-- GOLD: Product performance metrics
-- =============================================================================
CREATE OR REFRESH LIVE TABLE gold_product_metrics
COMMENT "Product-level sales performance"
AS SELECT
  s.product_id,
  p.product_name,
  p.category,
  COUNT(DISTINCT s.customer_id) as unique_customers,
  COUNT(s.sale_id) as total_transactions,
  SUM(s.quantity) as total_units_sold,
  SUM(s.amount) as total_revenue,
  AVG(s.amount) as avg_order_value
FROM LIVE.silver_sales s
JOIN LIVE.silver_products p ON s.product_id = p.product_id
GROUP BY s.product_id, p.product_name, p.category;

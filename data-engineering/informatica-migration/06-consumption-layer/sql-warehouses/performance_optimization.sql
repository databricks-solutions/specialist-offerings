-- =============================================================================
-- SQL Warehouse: Performance Optimization for BI Queries
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Optimizes Delta tables for faster BI query performance
-- - Configures caching and compute settings
-- - Enables efficient workload isolation
--
-- KEY OPTIMIZATION TECHNIQUES:
-- 1. OPTIMIZE + ZORDER: Colocate frequently-filtered data
-- 2. Statistics: Help query optimizer make better decisions
-- 3. Caching: Reuse results for repeated queries
-- 4. Partitioning: Prune data for date-range queries
-- 5. Clustering: Physical data organization
--
-- WORKLOAD ISOLATION PATTERN:
-- - ETL Pipelines     → Job Clusters (isolated, auto-terminate)
-- - Ad-hoc Analysts   → SQL Warehouse A (Medium)
-- - Executive Reports → SQL Warehouse B (Large, priority)
-- - External Partners → SQL Warehouse C (Small, rate-limited)
-- =============================================================================

-- =============================================================================
-- 1. OPTIMIZE and ZORDER for Query Performance
-- =============================================================================

-- OPTIMIZE compacts small files into larger ones (reduces file overhead)
-- ZORDER colocates related data for filter pushdown
OPTIMIZE gold_fact_sales
ZORDER BY (sale_date, store_id, product_id);

-- For customer lookups
OPTIMIZE gold_dim_customer
ZORDER BY (customer_id, customer_tier);

-- Run OPTIMIZE regularly (daily for active tables)
-- Can be scheduled via Databricks Workflows


-- =============================================================================
-- 2. Collect Statistics for Query Planning
-- =============================================================================

-- Analyze table to collect statistics
ANALYZE TABLE gold_fact_sales COMPUTE STATISTICS FOR ALL COLUMNS;

-- Or specific columns (faster for wide tables)
ANALYZE TABLE gold_fact_sales
COMPUTE STATISTICS FOR COLUMNS
  sale_date, store_id, product_id, customer_id, net_amount;

-- Check existing statistics
DESCRIBE EXTENDED gold_fact_sales;


-- =============================================================================
-- 3. Enable Query Result Caching
-- =============================================================================

-- Enable result cache (session-level)
SET spark.databricks.io.cache.enabled = true;

-- Enable Delta Cache (caches remote data locally on SSD)
SET spark.databricks.delta.optimizeWrite.enabled = true;

-- Caching is automatic for SQL Warehouses - repeated queries use cache


-- =============================================================================
-- 4. Partitioning for Date-Range Queries
-- =============================================================================

-- Partition by date for efficient time-based filtering
-- Best for: Tables frequently filtered by date ranges

-- Create partitioned table (do during initial table creation)
CREATE OR REPLACE TABLE gold_fact_sales_partitioned
PARTITIONED BY (sale_year, sale_month)
AS SELECT
  *,
  YEAR(sale_date) as sale_year,
  MONTH(sale_date) as sale_month
FROM gold_fact_sales;

-- Queries filtering by year/month will skip irrelevant partitions
-- Example: This only reads 2024 partitions
SELECT * FROM gold_fact_sales_partitioned
WHERE sale_year = 2024 AND sale_month IN (1, 2, 3);


-- =============================================================================
-- 5. Create Materialized Views for Common Aggregations
-- =============================================================================

-- Pre-aggregate common BI queries for instant results
CREATE OR REPLACE TABLE gold_mv_daily_sales AS
SELECT
  sale_date,
  store_region,
  product_category,
  customer_tier,
  SUM(net_amount) as total_sales,
  SUM(quantity) as total_units,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(*) as transactions
FROM gold_fact_sales f
JOIN gold_dim_store s ON f.store_id = s.store_id
JOIN gold_dim_product p ON f.product_id = p.product_id
JOIN gold_dim_customer c ON f.customer_id = c.customer_id
GROUP BY sale_date, store_region, product_category, customer_tier;

-- Optimize the materialized view
OPTIMIZE gold_mv_daily_sales ZORDER BY (sale_date, store_region);


-- =============================================================================
-- 6. Query Tuning Examples
-- =============================================================================

-- BAD: Full table scan
SELECT * FROM gold_fact_sales WHERE product_name LIKE '%Widget%';

-- GOOD: Use indexed columns in WHERE clause
SELECT f.*
FROM gold_fact_sales f
JOIN gold_dim_product p ON f.product_id = p.product_id
WHERE p.product_name LIKE '%Widget%';

-- BAD: SELECT * on wide tables
SELECT * FROM gold_dim_customer;

-- GOOD: Select only needed columns
SELECT customer_id, customer_name, customer_tier
FROM gold_dim_customer;

-- BAD: Expensive DISTINCT on large tables
SELECT DISTINCT customer_id FROM gold_fact_sales;

-- GOOD: Use pre-aggregated table or approximate
SELECT customer_id FROM gold_dim_customer;
-- OR: SELECT approx_count_distinct(customer_id) FROM gold_fact_sales;


-- =============================================================================
-- 7. Monitor Query Performance
-- =============================================================================

-- View query history (via SQL Warehouse UI or system tables)
SELECT
  query_id,
  user_name,
  query_text,
  execution_status,
  total_time_ms,
  rows_produced,
  query_start_time
FROM system.query.history
WHERE warehouse_id = 'your-warehouse-id'
  AND query_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY total_time_ms DESC
LIMIT 20;

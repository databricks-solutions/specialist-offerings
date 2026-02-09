-- =============================================================================
-- SQL Warehouses: Power BI / Tableau Connectivity
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Provides JDBC/ODBC connectivity for enterprise BI tools
-- - Optimizes queries for interactive analytics
-- - Auto-scales compute based on query load
--
-- REPLACES IN INFORMATICA:
-- - Loading data to Oracle/SQL Server for BI access
-- - Manual data mart refresh jobs
-- - Separate BI serving infrastructure
--
-- BENEFITS vs ORACLE/SQL SERVER:
-- | Aspect          | Old (Informatica→Oracle→BI) | New (DLT→Gold→SQL Warehouse) |
-- |-----------------|-----------------------------|-----------------------------|
-- | Data Movement   | Extra ETL step              | Direct access to Gold       |
-- | Freshness       | Batch refresh lag           | Near real-time              |
-- | Compute         | Shared with ETL             | Isolated BI compute         |
-- | Cost            | Fixed database license      | Pay per query               |
-- | Scaling         | Manual                      | Auto-scale                  |
--
-- WAREHOUSE TYPES:
-- | Type        | Startup | Cost       | Best For                    |
-- |-------------|---------|------------|-----------------------------|
-- | Serverless  | Instant | Per-query  | Variable workloads, ad-hoc  |
-- | Pro         | ~2 min  | Per-hour   | Steady workloads            |
-- | Classic     | ~2 min  | Per-hour   | Legacy/specific needs       |
--
-- CONNECTION SETTINGS FOR POWER BI:
-- - Connector: "Databricks" (built-in)
-- - Server: <workspace>.cloud.databricks.com
-- - HTTP Path: /sql/1.0/warehouses/<warehouse-id>
-- - Authentication: Azure AD (recommended) or Personal Access Token
-- =============================================================================

-- =============================================================================
-- Sample Queries for Power BI / Tableau Dashboards
-- =============================================================================

-- Sales Overview Dashboard
SELECT
  d.sale_date,
  d.sale_year,
  d.sale_month,
  d.day_of_week,
  d.day_type,
  st.store_region,
  st.store_name,
  p.product_category,
  p.brand,
  c.customer_tier,
  SUM(f.net_amount) as total_sales,
  SUM(f.gross_margin) as total_margin,
  COUNT(f.sale_id) as transaction_count,
  COUNT(DISTINCT f.customer_id) as unique_customers,
  AVG(f.net_amount) as avg_transaction_value
FROM gold_fact_sales f
JOIN gold_dim_date d ON f.sale_date = d.date_id
JOIN gold_dim_store st ON f.store_id = st.store_id
JOIN gold_dim_product p ON f.product_id = p.product_id
JOIN gold_dim_customer c ON f.customer_id = c.customer_id
WHERE d.sale_year >= YEAR(CURRENT_DATE) - 2  -- Last 2 years
GROUP BY
  d.sale_date, d.sale_year, d.sale_month, d.day_of_week, d.day_type,
  st.store_region, st.store_name,
  p.product_category, p.brand,
  c.customer_tier;


-- Customer Segmentation Dashboard
SELECT
  c.customer_tier,
  c.customer_segment,
  c.activity_status,
  COUNT(*) as customer_count,
  AVG(c.lifetime_value) as avg_lifetime_value,
  AVG(c.total_orders) as avg_orders,
  AVG(c.days_since_last_order) as avg_days_inactive
FROM gold_dim_customer c
GROUP BY c.customer_tier, c.customer_segment, c.activity_status;


-- Product Performance Dashboard
SELECT
  p.product_category,
  p.product_subcategory,
  p.brand,
  p.product_name,
  SUM(f.quantity) as units_sold,
  SUM(f.net_amount) as revenue,
  SUM(f.gross_margin) as margin,
  SUM(f.gross_margin) / NULLIF(SUM(f.net_amount), 0) * 100 as margin_pct,
  COUNT(DISTINCT f.customer_id) as unique_buyers
FROM gold_fact_sales f
JOIN gold_dim_product p ON f.product_id = p.product_id
WHERE f.sale_date >= CURRENT_DATE - INTERVAL 90 DAYS
GROUP BY p.product_category, p.product_subcategory, p.brand, p.product_name
ORDER BY revenue DESC;


-- =============================================================================
-- Incremental/DirectQuery Optimization
-- For Power BI DirectQuery mode (real-time, no import)
-- =============================================================================

-- Create aggregated summary table for faster DirectQuery
CREATE OR REPLACE TABLE gold_sales_summary_daily AS
SELECT
  sale_date,
  store_id,
  product_category,
  customer_tier,
  SUM(net_amount) as total_sales,
  SUM(gross_margin) as total_margin,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT customer_id) as unique_customers
FROM gold_fact_sales f
JOIN gold_dim_product p ON f.product_id = p.product_id
JOIN gold_dim_customer c ON f.customer_id = c.customer_id
GROUP BY sale_date, store_id, product_category, customer_tier;

-- Optimize for common filter patterns
OPTIMIZE gold_sales_summary_daily ZORDER BY (sale_date, store_id);

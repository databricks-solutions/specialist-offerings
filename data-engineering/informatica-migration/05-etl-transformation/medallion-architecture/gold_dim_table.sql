-- =============================================================================
-- Gold Layer: Customer Dimension Table
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Creates analytics-ready dimension table from Silver data
-- - Denormalizes and enriches with calculated attributes
-- - Applies business logic (customer tiers, segments)
--
-- REPLACES IN INFORMATICA:
-- - Dimension table load mappings
-- - Lookup enrichment transformations
-- - Expression transformations for derived columns
--
-- DIMENSION TABLE BEST PRACTICES:
-- - Use business-friendly column names
-- - Include derived/calculated attributes
-- - Denormalize for query performance
-- - Only include current records (is_current = TRUE)
-- - Add surrogate keys if needed for fact table joins
--
-- STAR SCHEMA PATTERN:
--     dim_customer ──┐
--     dim_product  ──┼── fact_sales
--     dim_store    ──┤
--     dim_date     ──┘
-- =============================================================================

CREATE OR REFRESH LIVE TABLE gold_dim_customer
COMMENT "Customer dimension with latest attributes and calculated tiers"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "customer_id"
)
AS
SELECT
  -- Primary key
  c.customer_id,

  -- Descriptive attributes
  c.first_name || ' ' || c.last_name as full_name,
  c.first_name,
  c.last_name,
  c.email,

  -- Address (denormalized)
  c.address,
  c.city,
  c.state,
  c.country,
  CONCAT(c.city, ', ', c.state, ' ', c.country) as full_address,

  -- Business attributes
  c.customer_segment,
  c.registration_date,
  DATEDIFF(CURRENT_DATE, c.registration_date) as customer_tenure_days,

  -- Metrics from aggregates
  COALESCE(m.total_orders, 0) as total_orders,
  COALESCE(m.total_purchases, 0) as total_purchases,
  m.last_order_date,
  DATEDIFF(CURRENT_DATE, m.last_order_date) as days_since_last_order,

  -- Calculated tier based on lifetime value
  CASE
    WHEN COALESCE(m.total_purchases, 0) > 10000 THEN 'VIP'
    WHEN COALESCE(m.total_purchases, 0) > 5000 THEN 'Premium'
    WHEN COALESCE(m.total_purchases, 0) > 1000 THEN 'Regular'
    ELSE 'New'
  END as customer_tier,

  -- Activity status
  CASE
    WHEN m.last_order_date IS NULL THEN 'Never Purchased'
    WHEN DATEDIFF(CURRENT_DATE, m.last_order_date) <= 30 THEN 'Active'
    WHEN DATEDIFF(CURRENT_DATE, m.last_order_date) <= 90 THEN 'Recent'
    WHEN DATEDIFF(CURRENT_DATE, m.last_order_date) <= 365 THEN 'Lapsed'
    ELSE 'Dormant'
  END as activity_status,

  -- Audit
  current_timestamp() as last_updated

FROM LIVE.silver_customers c
LEFT JOIN LIVE.silver_customer_metrics m
  ON c.customer_id = m.customer_id
WHERE c.is_current = TRUE;  -- Only current SCD2 records

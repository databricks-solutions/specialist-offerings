-- =============================================================================
-- Gold Layer: Sales Fact Table
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Creates analytics-ready fact table for reporting
-- - Joins transactional data with dimension attributes
-- - Pre-calculates metrics for query performance
--
-- REPLACES IN INFORMATICA:
-- - Fact table load mappings
-- - Multiple Lookup transformations for dimensions
-- - Aggregator/Expression for calculated measures
--
-- FACT TABLE BEST PRACTICES:
-- - Include foreign keys to all relevant dimensions
-- - Pre-calculate common measures (net_amount, margin, etc.)
-- - Denormalize frequently-used dimension attributes
-- - Partition by date for query performance
-- - Use ZORDER for common filter columns
--
-- FACT TABLE TYPES:
-- | Type          | Description                    | Example              |
-- |---------------|--------------------------------|----------------------|
-- | Transaction   | One row per event              | Sales, clicks        |
-- | Periodic      | Snapshot at intervals          | Daily inventory      |
-- | Accumulating  | Updated as process progresses  | Order fulfillment    |
-- =============================================================================

CREATE OR REFRESH LIVE TABLE gold_fact_sales
COMMENT "Sales fact table with all dimension keys and calculated measures"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "sale_date,store_id"
)
AS
SELECT
  -- ==========================================================================
  -- Fact keys and identifiers
  -- ==========================================================================
  s.sale_id,
  s.sale_date,

  -- Dimension foreign keys
  s.customer_id,
  s.product_id,
  s.store_id,

  -- ==========================================================================
  -- Measures (facts)
  -- ==========================================================================
  s.quantity,
  s.unit_price,
  s.discount,
  s.quantity * s.unit_price as gross_amount,
  s.quantity * s.unit_price * (1 - COALESCE(s.discount, 0)) as net_amount,

  -- Cost and margin (if available)
  p.unit_cost,
  s.quantity * COALESCE(p.unit_cost, 0) as total_cost,
  (s.quantity * s.unit_price * (1 - COALESCE(s.discount, 0)))
    - (s.quantity * COALESCE(p.unit_cost, 0)) as gross_margin,

  -- ==========================================================================
  -- Denormalized dimension attributes (for query convenience)
  -- ==========================================================================
  -- Product attributes
  p.product_name,
  p.product_category,
  p.product_subcategory,
  p.brand,

  -- Customer attributes
  c.customer_tier,
  c.customer_segment,

  -- Store/location attributes
  st.store_name,
  st.store_region,
  st.store_city,
  st.store_state,

  -- ==========================================================================
  -- Date attributes (for easy filtering/grouping)
  -- ==========================================================================
  YEAR(s.sale_date) as sale_year,
  MONTH(s.sale_date) as sale_month,
  QUARTER(s.sale_date) as sale_quarter,
  DAYOFWEEK(s.sale_date) as day_of_week,
  CASE WHEN DAYOFWEEK(s.sale_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as day_type,

  -- Audit
  current_timestamp() as etl_timestamp

FROM LIVE.silver_sales s
JOIN LIVE.gold_dim_product p ON s.product_id = p.product_id
JOIN LIVE.gold_dim_customer c ON s.customer_id = c.customer_id
JOIN LIVE.gold_dim_store st ON s.store_id = st.store_id;

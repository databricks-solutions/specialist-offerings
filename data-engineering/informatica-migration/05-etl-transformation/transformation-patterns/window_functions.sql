-- =============================================================================
-- Window Functions: Running Totals, Moving Averages, Rankings
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Performs calculations across sets of rows related to current row
-- - Calculates running totals, moving averages, rankings
-- - Enables complex analytics without multiple passes
--
-- REPLACES IN INFORMATICA:
-- - Rank Transformation
-- - Aggregator with sorted input for running totals
-- - Multiple passes through data for cumulative calculations
--
-- WINDOW FUNCTION SYNTAX:
--   function() OVER (
--       PARTITION BY column     -- Reset calculation for each group
--       ORDER BY column         -- Order within partition
--       ROWS/RANGE frame        -- Define window boundaries
--   )
--
-- FRAME SPECIFICATIONS:
-- | Frame                              | Description                    |
-- |------------------------------------|--------------------------------|
-- | ROWS BETWEEN UNBOUNDED PRECEDING   | All rows from start            |
-- |   AND CURRENT ROW                  |   to current row               |
-- | ROWS BETWEEN 3 PRECEDING           | 3 rows before current          |
-- |   AND CURRENT ROW                  |   to current row               |
-- | ROWS BETWEEN CURRENT ROW           | Current row to end             |
-- |   AND UNBOUNDED FOLLOWING          |                                |
--
-- COMMON WINDOW FUNCTIONS:
-- - ROW_NUMBER(): Sequential number within partition
-- - RANK(): Rank with gaps for ties
-- - DENSE_RANK(): Rank without gaps
-- - LAG()/LEAD(): Access previous/next row values
-- - SUM/AVG/MIN/MAX OVER(): Running aggregates
-- =============================================================================

CREATE OR REFRESH LIVE TABLE gold_customer_metrics
COMMENT "Customer order metrics with running totals and rankings"
AS
SELECT
  customer_id,
  order_id,
  order_date,
  order_amount,

  -- ==========================================================================
  -- Running Total (Cumulative Sum)
  -- Lifetime value up to this order
  -- ==========================================================================
  SUM(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as lifetime_value_to_date,

  -- ==========================================================================
  -- Running Count
  -- Order number for this customer
  -- ==========================================================================
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
  ) as order_sequence_number,

  -- ==========================================================================
  -- Moving Average
  -- Average of last 3 orders (smoothed trend)
  -- ==========================================================================
  AVG(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) as avg_last_3_orders,

  -- ==========================================================================
  -- 90-Day Moving Average
  -- Trend over rolling 90-day window
  -- ==========================================================================
  AVG(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
  ) as avg_order_90d,

  -- ==========================================================================
  -- Ranking (Top Orders)
  -- Rank orders by amount within customer
  -- ==========================================================================
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY order_amount DESC
  ) as order_rank_by_amount,

  RANK() OVER (
    PARTITION BY customer_id
    ORDER BY order_amount DESC
  ) as order_rank_with_ties,

  -- ==========================================================================
  -- Previous/Next Values
  -- Compare to previous order
  -- ==========================================================================
  LAG(order_amount, 1) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
  ) as previous_order_amount,

  order_amount - LAG(order_amount, 1) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
  ) as change_from_previous,

  LAG(order_date, 1) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
  ) as previous_order_date,

  DATEDIFF(order_date, LAG(order_date, 1) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
  )) as days_since_previous_order,

  -- ==========================================================================
  -- First/Last Values
  -- Reference points within partition
  -- ==========================================================================
  FIRST_VALUE(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
  ) as first_order_amount,

  LAST_VALUE(order_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date, order_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) as latest_order_amount

FROM LIVE.silver_orders;

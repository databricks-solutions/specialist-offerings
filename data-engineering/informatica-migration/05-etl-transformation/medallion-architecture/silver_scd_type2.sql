-- =============================================================================
-- Silver Layer: SCD Type 2 - Track Historical Changes
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Tracks full history of changes to dimension records
-- - Each change creates a new row with effective dates
-- - Maintains is_current flag for latest version
--
-- REPLACES IN INFORMATICA:
-- - SCD Type 2 Wizard / Slowly Changing Dimension transformation
-- - Complex mapping with Lookup + Router + Update Strategy
-- - Custom effective date logic
--
-- SCD TYPES COMPARISON:
-- | Type | Description                    | Use Case                    |
-- |------|--------------------------------|-----------------------------|
-- | 0    | No change tracking             | Static data                 |
-- | 1    | Overwrite (no history)         | Current state only          |
-- | 2    | Full history with dates        | Audit, compliance, trends   |
-- | 3    | Limited history (prev value)   | Rarely used                 |
--
-- OUTPUT COLUMNS:
-- - record_hash: Detects if business attributes changed
-- - effective_start_date: When this version became active
-- - effective_end_date: When this version was superseded (NULL if current)
-- - is_current: TRUE for latest version
--
-- NOTE: For SCD Type 1 (overwrite), use APPLY CHANGES instead (simpler)
-- =============================================================================

-- Create streaming Silver table with SCD2 structure
CREATE OR REFRESH STREAMING LIVE TABLE silver_customer_scd (
  CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Customer dimension with SCD Type 2 history tracking"
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  address,
  city,
  state,
  country,

  -- Hash of business attributes to detect changes
  MD5(CONCAT_WS('|',
    COALESCE(first_name, ''),
    COALESCE(last_name, ''),
    COALESCE(email, ''),
    COALESCE(address, ''),
    COALESCE(city, ''),
    COALESCE(state, '')
  )) as record_hash,

  -- CDC metadata
  CAST(_change_type AS STRING) as change_type,

  -- SCD2 tracking columns
  _commit_timestamp as effective_start_date,
  CAST(NULL AS TIMESTAMP) as effective_end_date,
  TRUE as is_current

FROM STREAM(bronze_customer_cdc);


-- =============================================================================
-- Query patterns for SCD2 tables
-- =============================================================================

-- Get current state of all customers
-- SELECT * FROM silver_customer_scd WHERE is_current = TRUE;

-- Get customer state as of specific date (point-in-time)
-- SELECT * FROM silver_customer_scd
-- WHERE effective_start_date <= '2024-06-15'
--   AND (effective_end_date > '2024-06-15' OR effective_end_date IS NULL);

-- Get change history for specific customer
-- SELECT * FROM silver_customer_scd
-- WHERE customer_id = 12345
-- ORDER BY effective_start_date;

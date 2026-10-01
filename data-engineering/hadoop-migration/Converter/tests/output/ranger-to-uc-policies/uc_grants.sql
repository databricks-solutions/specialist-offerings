-- ============================================================
-- Unity Catalog Grants & Security Policies
-- Converted from: Ranger policy export (service: retail_hive)
-- Target namespace: aa_catalog.retail_analytics
-- Source: Converter/tests/input/ranger-to-uc-policies/ranger_policies.json
-- ============================================================

-- ============================================================
-- Policy 1: data_engineering_full_access
-- Description: Full access to all databases for the data engineering team
-- Type: Resource-based allow + delegateAdmin
-- Ranger resources: database=[retail_analytics, staging, raw_data], table=*, column=*
-- Principals: user=etl_service_account, group=data_engineering
-- ============================================================

-- database=retail_analytics, table=*, column=* → schema-level grant
-- delegateAdmin=true → MANAGE privilege (allows granting access to others)
GRANT MANAGE ON SCHEMA aa_catalog.retail_analytics TO `etl_service_account`;
GRANT MANAGE ON SCHEMA aa_catalog.retail_analytics TO `data_engineering`;

-- database=staging, table=*, column=* → schema-level grant
-- NOTE: Schema 'staging' must exist in aa_catalog before applying these grants
GRANT MANAGE ON SCHEMA aa_catalog.staging TO `etl_service_account`;
GRANT MANAGE ON SCHEMA aa_catalog.staging TO `data_engineering`;

-- database=raw_data, table=*, column=* → schema-level grant
-- NOTE: Schema 'raw_data' must exist in aa_catalog before applying these grants
GRANT MANAGE ON SCHEMA aa_catalog.raw_data TO `etl_service_account`;
GRANT MANAGE ON SCHEMA aa_catalog.raw_data TO `data_engineering`;

-- Note: Ranger permissions [select, update, create, drop, alter, index, lock, all]
-- with delegateAdmin=true → consolidated to MANAGE (includes ALL PRIVILEGES + grant ability)
-- 'index' and 'lock' permissions omitted — no UC equivalent (Delta handles natively)

-- ============================================================
-- Policy 2: analyst_read_access
-- Description: Read-only access for analysts to production tables
-- Type: Resource-based allow
-- Ranger resources: database=retail_analytics, table=[dim_customers, fact_orders, product_catalog, vw_active_customers], column=*
-- Principals: groups=data_analysts, business_analysts
-- ============================================================

GRANT SELECT ON TABLE aa_catalog.retail_analytics.dim_customers TO `data_analysts`;
GRANT SELECT ON TABLE aa_catalog.retail_analytics.dim_customers TO `business_analysts`;

GRANT SELECT ON TABLE aa_catalog.retail_analytics.fact_orders TO `data_analysts`;
GRANT SELECT ON TABLE aa_catalog.retail_analytics.fact_orders TO `business_analysts`;

GRANT SELECT ON TABLE aa_catalog.retail_analytics.product_catalog TO `data_analysts`;
GRANT SELECT ON TABLE aa_catalog.retail_analytics.product_catalog TO `business_analysts`;

GRANT SELECT ON TABLE aa_catalog.retail_analytics.vw_active_customers TO `data_analysts`;
GRANT SELECT ON TABLE aa_catalog.retail_analytics.vw_active_customers TO `business_analysts`;

-- ============================================================
-- Policy 3: pii_column_masking
-- Description: Mask PII columns for non-privileged users
-- Type: Column masking (policyType=1)
-- Ranger resources: database=retail_analytics, table=dim_customers, column=[email, phone, first_name, last_name]
-- Mask type: MASK_HASH for group=data_analysts
-- ============================================================

-- Step 1: Create mask functions (one per column, per reference naming convention)
-- MASK_HASH → sha2(col, 256)

CREATE OR REPLACE FUNCTION aa_catalog.retail_analytics.mask_dim_customers_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data_engineering') THEN email    -- data engineers see unmasked (full access from policy 1)
  WHEN is_member('data_analysts') THEN sha2(email, 256)
  ELSE NULL  -- default: fully masked for all other groups
END;

CREATE OR REPLACE FUNCTION aa_catalog.retail_analytics.mask_dim_customers_phone(phone STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data_engineering') THEN phone
  WHEN is_member('data_analysts') THEN sha2(phone, 256)
  ELSE NULL
END;

CREATE OR REPLACE FUNCTION aa_catalog.retail_analytics.mask_dim_customers_first_name(first_name STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data_engineering') THEN first_name
  WHEN is_member('data_analysts') THEN sha2(first_name, 256)
  ELSE NULL
END;

CREATE OR REPLACE FUNCTION aa_catalog.retail_analytics.mask_dim_customers_last_name(last_name STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('data_engineering') THEN last_name
  WHEN is_member('data_analysts') THEN sha2(last_name, 256)
  ELSE NULL
END;

-- Step 2: Apply masks to columns
ALTER TABLE aa_catalog.retail_analytics.dim_customers ALTER COLUMN email SET MASK aa_catalog.retail_analytics.mask_dim_customers_email;
ALTER TABLE aa_catalog.retail_analytics.dim_customers ALTER COLUMN phone SET MASK aa_catalog.retail_analytics.mask_dim_customers_phone;
ALTER TABLE aa_catalog.retail_analytics.dim_customers ALTER COLUMN first_name SET MASK aa_catalog.retail_analytics.mask_dim_customers_first_name;
ALTER TABLE aa_catalog.retail_analytics.dim_customers ALTER COLUMN last_name SET MASK aa_catalog.retail_analytics.mask_dim_customers_last_name;

-- Note: business_analysts group is not listed in the masking policy.
-- They have SELECT from policy 2 but no mask exception — they will see NULL (fully masked).
-- Review: If business_analysts should see hashed values like data_analysts, add them to the CASE.

-- ============================================================
-- Policy 4: row_level_security_orders
-- Description: Regional managers can only see their region's orders
-- Type: Row filter (policyType=2)
-- Ranger resources: database=retail_analytics, table=fact_orders
-- Filter: region = '{USER}.region' for group=regional_managers
-- ============================================================

-- WARNING: fact_orders (as defined in hive_schema.hql) does NOT have a 'region' column.
-- The Ranger filter references 'region' which may come from a different schema version
-- or may be a derived/virtual column. Manual review required before applying.

-- Step 1: Create the row filter function
CREATE OR REPLACE FUNCTION aa_catalog.retail_analytics.fact_orders_row_filter(region STRING)
RETURNS BOOLEAN
RETURN (
  is_member('data_engineering')     -- data engineers see all rows (full access from policy 1)
  OR is_member('data_analysts')     -- analysts see all rows (SELECT from policy 2, no filter)
  OR (is_member('regional_managers') AND region = current_user())
  -- Note: Ranger '{USER}.region' converted to current_user()
  -- This is a simplification — '{USER}.region' likely references a user attribute, not username.
  -- WARNING: Implement a proper mapping from user → region if needed.
);

-- Step 2: Apply the filter to the table
ALTER TABLE aa_catalog.retail_analytics.fact_orders SET ROW FILTER aa_catalog.retail_analytics.fact_orders_row_filter ON (region);

-- Step 3: Grant SELECT to regional_managers (row filter enforces visibility)
GRANT SELECT ON TABLE aa_catalog.retail_analytics.fact_orders TO `regional_managers`;

-- WARNING: The column 'region' does not exist in the current fact_orders schema.
-- You must either:
--   1. Add a 'region' column to fact_orders, OR
--   2. Modify the filter function to use an existing column (e.g., shipping_address.state), OR
--   3. Remove this row filter if the use case no longer applies.

-- ============================================================
-- Policy 5: ml_team_staging_access
-- Description: ML team read/write on staging, read on production
-- Type: Resource-based allow
-- Ranger resources: database=staging, table=*, column=*
-- Principals: user=ml_service_account, group=ml_engineering
-- ============================================================

-- database=staging, table=*, column=* → schema-level grant
-- Permissions: select, update, create, drop → ALL PRIVILEGES
GRANT ALL PRIVILEGES ON SCHEMA aa_catalog.staging TO `ml_service_account`;
GRANT ALL PRIVILEGES ON SCHEMA aa_catalog.staging TO `ml_engineering`;

-- Note: The policy description says "read on production" but the Ranger policy JSON
-- only targets the 'staging' database. Production read access may be handled by
-- policy 2 (analyst_read_access) or a separate policy not included in this export.
-- WARNING: If ml_engineering needs read access to retail_analytics, add:
-- GRANT SELECT ON SCHEMA aa_catalog.retail_analytics TO `ml_engineering`;

-- ============================================================
-- Policy 6: deny_raw_data_access
-- Description: Deny access to raw data for all except data engineering
-- Type: Deny with exceptions
-- Ranger resources: database=raw_data, table=*, column=*
-- Deny: group=public (all users)
-- Exception: user=etl_service_account, group=data_engineering
-- ============================================================

-- WARNING: UC uses default-deny model. If no GRANT is issued for raw_data,
-- access is already denied to everyone. The deny policy is effectively a no-op
-- in UC UNLESS broader grants (e.g., catalog-level) provide unintended access.

-- Ensure no broad grants leak access to raw_data:
REVOKE ALL PRIVILEGES ON SCHEMA aa_catalog.raw_data FROM `public`;

-- Re-affirm access for the exceptions (data engineering already has MANAGE from policy 1):
-- etl_service_account and data_engineering already have MANAGE on raw_data from policy 1.
-- No additional grants needed for them.

-- Review: If a catalog-level GRANT was issued (e.g., GRANT SELECT ON CATALOG aa_catalog),
-- it would cascade to raw_data. In that case, either:
--   1. Remove the catalog-level grant and use schema-level grants instead, OR
--   2. Use UC's fine-grained access control to restrict specific schemas.

-- ============================================================
-- SUMMARY
-- ============================================================
-- Policies converted: 6
--   - Resource-based allow:    3 (policies 1, 2, 5)
--   - Column masking:          1 (policy 3 — 4 columns masked)
--   - Row filter:              1 (policy 4 — requires manual review)
--   - Deny with exceptions:    1 (policy 6 — mostly no-op in UC default-deny)
--
-- Groups referenced:
--   - data_engineering       → MANAGE on retail_analytics, staging, raw_data
--   - data_analysts          → SELECT on 4 tables, PII columns hashed
--   - business_analysts      → SELECT on 4 tables, PII columns fully masked (NULL)
--   - regional_managers      → SELECT on fact_orders with row filter
--   - ml_engineering         → ALL PRIVILEGES on staging
--
-- Users referenced:
--   - etl_service_account    → MANAGE on retail_analytics, staging, raw_data
--   - ml_service_account     → ALL PRIVILEGES on staging
--
-- ============================================================
-- ITEMS REQUIRING MANUAL REVIEW
-- ============================================================
-- 1. [Policy 4] fact_orders does not have a 'region' column — row filter will fail
--    until the column is added or the filter is modified.
-- 2. [Policy 4] Ranger '{USER}.region' is a user attribute reference, not a simple
--    username match. Implement proper user-to-region mapping logic.
-- 3. [Policy 5] ML team production read access may be missing — verify if
--    ml_engineering should have SELECT on retail_analytics.
-- 4. [Policy 6] Verify no catalog-level grants leak access to raw_data schema.
-- 5. [Policy 3] business_analysts see NULL for PII columns (not hashed). Confirm
--    this is intended or add them to the masking functions.
-- 6. Schemas 'staging' and 'raw_data' must exist in aa_catalog before running grants.

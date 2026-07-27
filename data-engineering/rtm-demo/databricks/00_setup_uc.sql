-- Unity Catalog objects for the Customer RTM harness.
-- Run on a DBR 16.4 LTS+ cluster or a SQL warehouse (serverless OK for this DDL).
--
-- ⚠️ MANAGED ICEBERG WRITE PREVIEW CHECK ------------------------------------------------
-- Writes to managed Apache Iceberg tables are in Private Preview. Before relying on UC-2,
-- run the tiny probe at the bottom of this file (section 4). If the INSERT fails with an
-- entitlement/preview error, managed-Iceberg writes are NOT enabled on this workspace —
-- request enablement, or fall back to Delta+UniForm (only with sign-off).
-- ---------------------------------------------------------------------------------------

-- 1. Catalog + schema ------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS customer_rtm;
CREATE SCHEMA  IF NOT EXISTS customer_rtm.streaming;

-- 2. Checkpoints volume (persistent, NOT dbfs) -----------------------------------------
CREATE VOLUME IF NOT EXISTS customer_rtm.streaming.checkpoints;
-- checkpoint paths (one per stream) will live under:
--   /Volumes/customer_rtm/streaming/checkpoints/uc1_rtm_fraud
--   /Volumes/customer_rtm/streaming/checkpoints/uc2_iceberg_standardize

-- 3. UC-2 target: managed ICEBERG table ------------------------------------------------
--    Standardized event schema. USING iceberg => managed Iceberg (not Delta).
CREATE TABLE IF NOT EXISTS customer_rtm.streaming.events_std (
    event_id        STRING,
    event_type      STRING,     -- standardized: UPPER, trimmed
    user_id         STRING,
    source_system   STRING,
    payload_json    STRING,
    event_ts        TIMESTAMP,  -- parsed from source
    ingest_ts       TIMESTAMP   -- pipeline current_timestamp()
)
USING iceberg
COMMENT 'UC-2 standardized events (managed Apache Iceberg)';

-- 4. PREVIEW PROBE (run once, then delete the row) -------------------------------------
--    If this INSERT errors, managed-Iceberg writes are not enabled here.
CREATE TABLE IF NOT EXISTS customer_rtm.streaming._iceberg_probe (id INT) USING iceberg;
INSERT INTO customer_rtm.streaming._iceberg_probe VALUES (1);
SELECT 'managed iceberg WRITE ok' AS probe, count(*) AS rows FROM customer_rtm.streaming._iceberg_probe;
-- cleanup: DROP TABLE customer_rtm.streaming._iceberg_probe;

-- 5. Confirm the target is really Iceberg ---------------------------------------------
DESCRIBE EXTENDED customer_rtm.streaming.events_std;  -- expect Provider: iceberg

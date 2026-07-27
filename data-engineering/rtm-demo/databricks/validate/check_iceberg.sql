-- UC-2 validation: confirm rows are landing in the managed Iceberg table and it IS Iceberg.

-- 1. Provider must be Iceberg (not delta) -------------------------------------------------
DESCRIBE EXTENDED customer_rtm.streaming.events_std;   -- look for  Provider: iceberg

-- 2. Rows landing + freshness -------------------------------------------------------------
SELECT
  count(*)               AS row_count,
  min(ingest_ts)         AS first_ingest,
  max(ingest_ts)         AS last_ingest,
  count(DISTINCT user_id) AS distinct_users
FROM customer_rtm.streaming.events_std;

-- 3. Standardization worked: event_type should be UPPER + trimmed, no leading/trailing space
SELECT event_type, count(*) AS n
FROM customer_rtm.streaming.events_std
GROUP BY event_type
ORDER BY n DESC;
-- expect values like LOGIN / LOGOUT / PAGE_VIEW / ADD_TO_CART / CHECKOUT (no stray spaces/case)

-- 4. Iceberg history / snapshots (proves it's a real Iceberg table with metadata) ---------
DESCRIBE HISTORY customer_rtm.streaming.events_std;

-- 5. No null keys / parse leakage ---------------------------------------------------------
SELECT
  sum(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) AS null_event_ids,
  sum(CASE WHEN event_type RLIKE '^\\s|\\s$' THEN 1 ELSE 0 END) AS untrimmed_types
FROM customer_rtm.streaming.events_std;
-- both should be 0

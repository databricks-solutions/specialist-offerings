-- =============================================================================
-- Unity Catalog Audit Logs: Security and Compliance Tracking
-- =============================================================================
--
-- WHAT THIS DOES:
-- - Tracks all access to data (who, what, when, where)
-- - Provides audit trail for compliance (SOX, GDPR, HIPAA)
-- - Enables security monitoring and anomaly detection
--
-- REPLACES IN INFORMATICA:
-- - Session logs and statistics
-- - Repository audit tables
-- - Custom audit logging
--
-- AUDIT LOG CAPTURES:
-- - Data access (SELECT queries)
-- - Data modifications (INSERT, UPDATE, DELETE)
-- - Schema changes (CREATE, ALTER, DROP)
-- - Permission changes (GRANT, REVOKE)
-- - Authentication events (login, logout)
--
-- SYSTEM TABLES FOR AUDIT:
-- - system.access.audit: Comprehensive audit log
-- - system.access.table_lineage: Data flow tracking
-- - system.access.column_lineage: Column-level lineage
-- - system.billing.usage: Cost and usage tracking
-- =============================================================================

-- =============================================================================
-- Query Audit Logs for Compliance
-- =============================================================================

-- Who accessed sensitive table in last 30 days?
SELECT
  event_date,
  event_time,
  user_identity.email as user_email,
  action_name,
  request_params.full_name_arg as table_name,
  source_ip_address,
  user_agent,
  response.status_code
FROM system.access.audit
WHERE request_params.full_name_arg = 'main.gold.customer_pii'
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY event_time DESC;


-- =============================================================================
-- Data Access Patterns: Who is accessing what?
-- =============================================================================

-- Top users by query count (last 7 days)
SELECT
  user_identity.email as user_email,
  COUNT(*) as query_count,
  COUNT(DISTINCT request_params.full_name_arg) as tables_accessed
FROM system.access.audit
WHERE action_name IN ('getTable', 'commandSubmit')
  AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY user_identity.email
ORDER BY query_count DESC
LIMIT 20;


-- Most accessed tables
SELECT
  request_params.full_name_arg as table_name,
  COUNT(*) as access_count,
  COUNT(DISTINCT user_identity.email) as unique_users,
  MAX(event_time) as last_accessed
FROM system.access.audit
WHERE action_name = 'getTable'
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND request_params.full_name_arg IS NOT NULL
GROUP BY request_params.full_name_arg
ORDER BY access_count DESC
LIMIT 20;


-- =============================================================================
-- Security Monitoring: Detect Anomalies
-- =============================================================================

-- Failed access attempts (potential security issues)
SELECT
  event_date,
  event_time,
  user_identity.email,
  action_name,
  request_params.full_name_arg as resource,
  response.error_message,
  source_ip_address
FROM system.access.audit
WHERE response.status_code != '200'
  AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY event_time DESC;


-- Unusual access patterns (off-hours activity)
SELECT
  user_identity.email,
  HOUR(event_time) as access_hour,
  COUNT(*) as query_count
FROM system.access.audit
WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
  AND (HOUR(event_time) < 6 OR HOUR(event_time) > 22)  -- Off-hours
GROUP BY user_identity.email, HOUR(event_time)
HAVING COUNT(*) > 10
ORDER BY query_count DESC;


-- =============================================================================
-- Schema Change Tracking
-- =============================================================================

-- Recent schema changes (CREATE, ALTER, DROP)
SELECT
  event_date,
  event_time,
  user_identity.email,
  action_name,
  request_params.full_name_arg as object_name,
  request_params
FROM system.access.audit
WHERE action_name IN ('createTable', 'alterTable', 'dropTable',
                      'createSchema', 'dropSchema', 'createCatalog')
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY event_time DESC;

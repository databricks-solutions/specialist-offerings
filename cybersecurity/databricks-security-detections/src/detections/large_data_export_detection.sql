-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Large Data Export Detection Rule (System Tables)
-- MAGIC 
-- MAGIC **Rule ID**: DATA-001  
-- MAGIC **Author**: Data Protection Team  
-- MAGIC **Version**: 2.0.0  
-- MAGIC **Last Updated**: 2024-01-14
-- MAGIC ## Overview
-- MAGIC This detection rule identifies potential data exfiltration by monitoring for unusually large data exports, downloads, or transfers by users
-- MAGIC 
-- MAGIC ## Detection Logic
-- MAGIC - Monitors system.access.audit for large data exports, downloads, or transfers
-- MAGIC 
-- MAGIC ## MITRE ATT&CK Mapping
-- MAGIC - **Tactic**: TA0010 (Exfiltration)
-- MAGIC - **Technique**: T1041 (Exfiltration Over C2 Channel)
-- MAGIC 
-- MAGIC ## Data Sources
-- MAGIC - system.access.audit (Primary)


-- COMMAND ----------

use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

-- DBTITLE 1,Create MV
CREATE OR REFRESH MATERIALIZED VIEW sec_mv_data_export_detection
COMMENT 'Unified indicators of data export activity outside of Databricks across SQL Editor, Filesystem, Notebooks, and Dashboards.'
---- set a schedule to refresh for mv
-- SCHEDULE EVERY 1 DAY
AS
-- SQL Editor: result downloads from the SQL Editor (excludes dashboards)
SELECT
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  service_name as source,
  action_name,
  CAST(request_params.queryId AS STRING) AS artifact,
  event_time
FROM system.access.audit
WHERE service_name in ('databrickssql')
  AND action_name in ('downloadQueryResult')
  AND COALESCE(response.status_code, 200) = 200
  AND event_time >= current_timestamp() - INTERVAL 168 HOUR

UNION ALL

-- Filesystem: file reads (exposes transferred bytes)
SELECT
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  'filesystem' AS source,
  action_name,
  CAST(request_params.path AS STRING) AS artifact,
  event_time
FROM system.access.audit
WHERE service_name = 'filesystem'
  AND action_name = 'filesGet'
  AND COALESCE(response.status_code, 200) = 200
  AND event_time >= current_timestamp() - INTERVAL 168 HOUR

UNION ALL

-- Notebooks: large results downloads from notebook runs
SELECT
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  'notebook' AS source,
  a.action_name,
  CAST(request_params.notebookFullPath AS STRING) AS artifact,
  event_time
FROM system.access.audit a
WHERE service_name = 'notebook'
  AND action_name in ('downloadLargeResults', 'downloadPreviewResults')
  AND COALESCE(response.status_code, 200) = 200
  AND event_time >= current_timestamp() - INTERVAL 168 HOUR

UNION ALL

-- Dashboards: snapshot/export triggers (successful only)
SELECT
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  'dashboards' AS source,
  a.action_name,
  CAST(a.request_params['dashboard_id'] AS STRING) AS artifact,
  event_time
FROM system.access.audit a
WHERE a.service_name = 'dashboards'
  AND a.action_name IN ('triggerDashboardSnapshot')
  AND a.response['status_code'] = 200
  AND event_time >= current_timestamp() - INTERVAL 168 HOUR;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Export Risk Assessment & Recommendations

-- COMMAND ----------

-- AI-powered risk assessment and recommendations for data export patterns

CREATE OR REFRESH MATERIALIZED VIEW sec_mv_ai_data_export_analysis AS
with actor_activity as
(
  SELECT
  workspace_id,
  actor,
  source,
  action_name,
  artifact,
  MIN(event_time) as first_activity,
  MAX(event_time) as last_activity,
  count(*) as export_count
FROM
  sec_mv_data_export_detection
--WHERE event_time >= current_timestamp() - INTERVAL 168 HOUR
group by
  all
)
select *,
  -- AI-powered comprehensive analysis for data export patterns
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    'Assess data export security risk and provide recommendations: ' || 'Actor=' || actor
    || ', Source='
    || source
    || ', Action='
    || action_name
    || ', Artifact='
    || artifact
    || ', First Activity='
    || cast(first_activity as string)
    || ', Last Activity='
    || cast(last_activity as string)
    || ', Numer of Exports='
    || cast(export_count as string)
    || '. Consider: source sensitivity, time patterns, user role, data volume. '
    || 'Provide: 1) Risk Level (HIGH/MEDIUM/LOW), 2) Risk Factors, 3) Immediate Actions, 4) Long-term Recommendations.',
    responseFormat =>
      '{
          "type": "json_schema",
          "json_schema": {
            "name": "data_export_analysis",
            "schema": {
              "type": "object",
              "properties": {
                "risk_level": {"type": "string"},
                "risk_factors": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "long_term_recommendations": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
  ) as ai_comprehensive_analysis
FROM actor_activity;

-- COMMAND ----------

SELECT * from sec_mv_ai_data_export_analysis where ai_comprehensive_analysis is not null and actor is not null limit 10;
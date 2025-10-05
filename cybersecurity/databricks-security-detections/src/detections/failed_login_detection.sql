-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Failed Login Detection Rule (System Tables)
-- MAGIC
-- MAGIC **Rule ID**: AUTH-001  
-- MAGIC **Author**: Security Team  
-- MAGIC **Version**: 2.0.0  
-- MAGIC
-- MAGIC ## Overview
-- MAGIC This detection rule identifies multiple failed login attempts from the same source IP or targeting the same user account within a specified time window using Databricks system tables.
-- MAGIC
-- MAGIC ## Detection Logic
-- MAGIC - Monitors system.access.audit for authentication events
-- MAGIC - Groups by source IP and target user
-- MAGIC - Triggers when threshold exceeded within time window
-- MAGIC
-- MAGIC ## Data Sources
-- MAGIC - system.access.audit

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration Variables

-- COMMAND ----------

use catalog identifier(:catalog);
use schema identifier(:schema);


-- COMMAND ----------

CREATE VIEW IF NOT EXISTS sec_v_auth_events AS
select
  event_time, 
  event_date,
  service_name,
  coalesce(user_identity.email,request_params.user) as username,
  request_params.authenticationMethod as authentication_method,
  user_agent,
  action_name,
  response.status_code as response_code, 
  response.error_message as response_message,
  response.result as result,
  source_ip_address as source_ip
from system.access.audit 
where action_name IN ('login')
and response.status_code <> 200;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Failed Login Risk Assessment & Recommendations

-- COMMAND ----------

-- AI-powered risk assessment and recommendations for failed login patterns
CREATE OR REFRESH MATERIALIZED VIEW sec_mv_ai_failed_login_analysis AS
WITH failed_login_patterns AS (
    SELECT 
        'IP_BASED' AS detection_type,
        source_ip AS entity_id,
        source_ip AS entity_name,
        'source_ip' AS entity_type,
        COUNT(*) AS failed_attempts,
        COUNT(DISTINCT username) AS unique_entities,
        COLLECT_LIST(DISTINCT username) AS related_entities,
        COLLECT_LIST(DISTINCT user_agent) AS user_agents,
        MIN(event_time) AS first_attempt,
        MAX(event_time) AS last_attempt,
        current_timestamp() AS detection_timestamp
    FROM sec_v_auth_events
    WHERE source_ip IS NOT NULL
    AND event_time >= current_timestamp() - INTERVAL 168 HOURS
    GROUP BY source_ip
    
    UNION ALL
    
    SELECT 
        'USER_BASED' AS detection_type,
        username AS entity_id,
        username AS entity_name,
        'user' AS entity_type,
        COUNT(*) AS failed_attempts,
        COUNT(DISTINCT source_ip) AS unique_entities,
        COLLECT_LIST(DISTINCT source_ip) AS related_entities,
        COLLECT_LIST(DISTINCT user_agent) AS user_agents,
        MIN(event_time) AS first_attempt,
        MAX(event_time) AS last_attempt,
        current_timestamp() AS detection_timestamp
    FROM sec_v_auth_events
    WHERE username IS NOT NULL
    AND event_time >= current_timestamp() - INTERVAL 168 HOURS
    GROUP BY username
)
SELECT 
    detection_type,
    entity_id,
    entity_name,
    entity_type,
    failed_attempts,
    unique_entities,
    related_entities,
    user_agents,
    first_attempt,
    last_attempt,
    detection_timestamp,
    -- AI-powered comprehensive analysis
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Analyze this failed login pattern and provide a comprehensive security assessment. ' ||
        'Context: Type=' || detection_type ||
        ', Entity=' || entity_name ||
        ', Failed Attempts=' || CAST(failed_attempts AS STRING) ||
        ', Unique Entities=' || CAST(unique_entities AS STRING) ||
        ', Time Window=' || CAST(TIMESTAMPDIFF(HOUR, first_attempt, last_attempt) AS STRING) || ' hours' ||
        '. Provide: 1) Risk Level (HIGH/MEDIUM/LOW), 2) Risk Factors, 3) Immediate Actions, 4) Long-term Recommendations.',
        responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "failed_login_analysis",
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
FROM failed_login_patterns;

-- COMMAND ----------

SELECT * from sec_mv_ai_failed_login_analysis where ai_comprehensive_analysis is not null and entity_id is not null limit 10;
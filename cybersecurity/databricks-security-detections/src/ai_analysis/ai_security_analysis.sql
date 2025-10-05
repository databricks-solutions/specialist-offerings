-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AI-Powered Security Analysis
-- MAGIC
-- MAGIC **Purpose**: Centralized AI analysis for security detection rules  
-- MAGIC **Author**: Security Team  
-- MAGIC **Version**: 2.0.0  
-- MAGIC
-- MAGIC ## Overview
-- MAGIC This notebook contains all AI-powered analysis functions for security detection rules.
-- MAGIC It provides risk assessments and security insights using Databricks AI models.
-- MAGIC
-- MAGIC ## Usage
-- MAGIC - Call these views from detection notebooks
-- MAGIC - Provides consistent AI analysis across all security rules
-- MAGIC - Centralized model configuration and prompt management

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration

-- COMMAND ----------

use catalog identifier(:catalog);
use schema identifier(:schema);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Security Posture Summary with AI Insights

-- COMMAND ----------

-- AI-powered security posture summary
CREATE OR REFRESH MATERIALIZED VIEW sec_mv_ai_security_posture_summary AS
WITH security_events AS (
    SELECT 
        'FAILED_LOGINS' as event_type,
        COUNT(*) as event_count,
        MAX(event_time) as last_event
    FROM sec_v_auth_events
    WHERE event_time >= current_timestamp() - INTERVAL 168 HOURS
    
    UNION ALL
    
    SELECT 
        'DATA_EXPORTS' as event_type,
        COUNT(*) as event_count,
        MAX(event_time) as last_event
    FROM sec_mv_data_export_detection
    --WHERE event_time >= current_timestamp() - INTERVAL 168 HOURS
    
    UNION ALL
    
    SELECT 
        'PRIVILEGE_ESCALATIONS' as event_type,
        COUNT(*) as event_count,
        MAX(actor_last_activity) as last_event
    FROM sec_mv_ai_privilege_escalation_consolidated
)
SELECT 
    'Security Posture Analysis' as analysis_type,
    current_timestamp() as analysis_timestamp,
    -- AI-powered security posture assessment
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Analyze this security posture based on events in the last 7 days: ' ||
        'Failed Logins: ' || COALESCE(CAST(MAX(CASE WHEN event_type = 'FAILED_LOGINS' THEN event_count END) AS STRING), '0') ||
        ', Data Exports: ' || COALESCE(CAST(MAX(CASE WHEN event_type = 'DATA_EXPORTS' THEN event_count END) AS STRING), '0') ||
        ', Privilege Escalations: ' || COALESCE(CAST(MAX(CASE WHEN event_type = 'PRIVILEGE_ESCALATIONS' THEN event_count END) AS STRING), '0') ||
        '. Provide: 1) Overall Risk Level, 2) Key Risk Areas, 3) Immediate Actions, 4) Strategic Recommendations, 5) Compliance Considerations.',
        responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "security_posture_summary",
            "schema": {
              "type": "object",
              "properties": {
                "overall_risk_level": {"type": "string"},
                "key_risk_areas": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "strategic_recommendations": {"type": "array", "items": {"type": "string"}},
                "compliance_considerations": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
    ) as ai_security_posture_assessment
FROM security_events;


-- COMMAND ----------

select * from sec_mv_ai_security_posture_summary
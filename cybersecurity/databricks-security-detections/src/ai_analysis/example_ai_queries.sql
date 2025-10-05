-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AI Analysis Example Queries
-- MAGIC
-- MAGIC This notebook demonstrates how to query the AI analysis views and materialized views with their structured output.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Failed Login Analysis

-- COMMAND ----------

-- Get risk level and actions for failed logins
SELECT 
    entity_name,
    failed_attempts,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions,
    get_json_object(ai_comprehensive_analysis, '$.risk_factors') as risk_factors,
    get_json_object(ai_comprehensive_analysis, '$.long_term_recommendations') as long_term_recommendations
FROM sec_mv_ai_failed_login_analysis
WHERE first_attempt >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Data Export Analysis

-- COMMAND ----------

-- High-risk data exports with detailed analysis
SELECT 
    actor,
    source,
    artifact,
    export_count,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.risk_factors') as risk_factors,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions,
    get_json_object(ai_comprehensive_analysis, '$.long_term_recommendations') as long_term_recommendations
FROM sec_mv_ai_data_export_analysis
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
  AND last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Privilege Escalation Analysis

-- COMMAND ----------

-- Critical privilege escalations requiring immediate attention
SELECT 
    escalation_type,
    actor,
    target_user,
    actor_activity_count,
    actor_unique_entities,
    actor_first_activity,
    actor_last_activity,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions,
    get_json_object(ai_comprehensive_analysis, '$.investigation_steps') as investigation_steps
FROM sec_mv_ai_privilege_escalation_consolidated
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('CRITICAL', 'HIGH')
  AND actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS
ORDER BY actor_last_activity DESC;

-- COMMAND ----------

-- Actor frequency analysis for privilege escalations
SELECT 
    actor,
    escalation_type,
    COUNT(*) as total_events,
    AVG(actor_activity_count) as avg_actor_activity,
    MAX(actor_unique_entities) as max_unique_entities,
    MIN(actor_first_activity) as earliest_activity,
    MAX(actor_last_activity) as latest_activity,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level
FROM sec_mv_ai_privilege_escalation_consolidated
WHERE actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS
GROUP BY actor, escalation_type, get_json_object(ai_comprehensive_analysis, '$.risk_level')
HAVING COUNT(*) > 1
ORDER BY total_events DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Security Posture Summary

-- COMMAND ----------

-- Overall security posture assessment
SELECT 
    analysis_type,
    analysis_timestamp,
    get_json_object(ai_security_posture_assessment, '$.overall_risk_level') as overall_risk_level,
    get_json_object(ai_security_posture_assessment, '$.key_risk_areas') as key_risk_areas,
    get_json_object(ai_security_posture_assessment, '$.immediate_actions') as immediate_actions,
    get_json_object(ai_security_posture_assessment, '$.strategic_recommendations') as strategic_recommendations,
    get_json_object(ai_security_posture_assessment, '$.compliance_considerations') as compliance_considerations
FROM sec_mv_ai_security_posture_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Working with Arrays

-- COMMAND ----------

-- Explode risk factors for detailed analysis
SELECT 
    actor,
    source,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    risk_factor.value as individual_risk_factor
FROM sec_mv_ai_data_export_analysis
LATERAL VIEW explode(from_json(get_json_object(ai_comprehensive_analysis, '$.risk_factors'), 'array<string>')) as risk_factor
WHERE last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- Explode investigation steps from privilege escalation analysis
SELECT 
    escalation_type,
    actor,
    target_user,
    investigation_step.value as individual_investigation_step
FROM sec_mv_ai_privilege_escalation_consolidated
LATERAL VIEW explode(from_json(get_json_object(ai_comprehensive_analysis, '$.investigation_steps'), 'array<string>')) as investigation_step
WHERE actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS;

-- COMMAND ----------

-- Explode strategic recommendations from security posture
SELECT 
    analysis_type,
    strategic_recommendation.value as individual_strategic_recommendation
FROM sec_mv_ai_security_posture_summary
LATERAL VIEW explode(from_json(get_json_object(ai_security_posture_assessment, '$.strategic_recommendations'), 'array<string>')) as strategic_recommendation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Filtering by Risk Level

-- COMMAND ----------

-- Count events by risk level across all detection types
SELECT 
    'FAILED_LOGINS' as detection_type,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    COUNT(*) as event_count
FROM sec_mv_ai_failed_login_analysis
WHERE first_attempt >= current_timestamp() - INTERVAL 168 HOURS
GROUP BY get_json_object(ai_comprehensive_analysis, '$.risk_level')

UNION ALL

SELECT 
    'DATA_EXPORTS' as detection_type,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    COUNT(*) as event_count
FROM sec_mv_ai_data_export_analysis
WHERE last_activity >= current_timestamp() - INTERVAL 168 HOURS
GROUP BY get_json_object(ai_comprehensive_analysis, '$.risk_level')

UNION ALL

SELECT 
    'PRIVILEGE_ESCALATIONS' as detection_type,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    COUNT(*) as event_count
FROM sec_mv_ai_privilege_escalation_consolidated
WHERE actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS
GROUP BY get_json_object(ai_comprehensive_analysis, '$.risk_level')

ORDER BY detection_type, 
    CASE risk_level
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Actor Frequency Analysis

-- COMMAND ----------

-- High-frequency actors across different security events
SELECT 
    'DATA_EXPORTS' as event_type,
    actor,
    export_count as activity_count,
    source as unique_source,
    artifact as unique_artifact,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level
FROM sec_mv_ai_data_export_analysis
WHERE export_count > 5
  AND last_activity >= current_timestamp() - INTERVAL 168 HOURS

UNION ALL

SELECT 
    'PRIVILEGE_ESCALATIONS' as event_type,
    actor,
    actor_activity_count as activity_count,
    target_user as unique_entity,
    NULL as unique_artifact,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level
FROM sec_mv_ai_privilege_escalation_consolidated
WHERE actor_activity_count > 3
  AND actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS

ORDER BY event_type, activity_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Compliance Reporting

-- COMMAND ----------

-- Compliance considerations for audit reports
SELECT 
    escalation_type,
    actor,
    target_user,
    actor_activity_count,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.long_term_controls') as long_term_controls
FROM sec_mv_ai_privilege_escalation_consolidated
WHERE actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS
ORDER BY actor_last_activity DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Advanced Analytics

-- COMMAND ----------

-- Cross-correlation analysis between different detection types
WITH failed_login_ips AS (
    SELECT DISTINCT entity_id
    FROM sec_mv_ai_failed_login_analysis
    WHERE first_attempt >= current_timestamp() - INTERVAL 168 HOURS
    AND entity_type = 'source_ip'
    GROUP BY entity_id
    HAVING failed_attempts > 10
),
privilege_escalations AS (
    SELECT DISTINCT actor
    FROM sec_mv_ai_privilege_escalation_consolidated
    WHERE actor_last_activity >= current_timestamp() - INTERVAL 168 HOURS
    AND get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
)
SELECT 
    'CROSS_CORRELATION' as analysis_type,
    COUNT(DISTINCT f.entity_id) as suspicious_ips,
    COUNT(DISTINCT p.actor) as high_risk_actors,
    'Investigate IPs with high failed login attempts that also have privilege escalation events' as recommendation
FROM failed_login_ips f
CROSS JOIN privilege_escalations p;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Performance Monitoring

-- COMMAND ----------

-- Monitor AI analysis performance and response times
SELECT 
    'AI_ANALYSIS_PERFORMANCE' as metric,
    COUNT(*) as total_analyses,
    AVG(LENGTH(CAST(ai_comprehensive_analysis AS STRING))) as avg_response_length,
    MIN(first_attempt) as earliest_event,
    MAX(last_attempt) as latest_event
FROM sec_mv_ai_failed_login_analysis
WHERE first_attempt >= current_timestamp() - INTERVAL 24 HOURS

UNION ALL

SELECT 
    'DATA_EXPORT_ANALYSIS' as metric,
    COUNT(*) as total_analyses,
    AVG(LENGTH(CAST(ai_comprehensive_analysis AS STRING))) as avg_response_length,
    MIN(first_activity) as earliest_event,
    MAX(last_activity) as latest_event
FROM sec_mv_ai_data_export_analysis
WHERE last_activity >= current_timestamp() - INTERVAL 24 HOURS

UNION ALL

SELECT 
    'PRIVILEGE_ESCALATION_ANALYSIS' as metric,
    COUNT(*) as total_analyses,
    AVG(LENGTH(CAST(ai_comprehensive_analysis AS STRING))) as avg_response_length,
    MIN(actor_first_activity) as earliest_event,
    MAX(actor_last_activity) as latest_event
FROM sec_mv_ai_privilege_escalation_consolidated
WHERE actor_last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Specific Privilege Escalation Types

-- COMMAND ----------

-- Account admin assignments analysis
SELECT 
    actor,
    target_user,
    admin_assignments_count,
    unique_targets,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions,
    get_json_object(ai_comprehensive_analysis, '$.investigation_steps') as investigation_steps
FROM sec_v_ai_account_admin_analysis
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('CRITICAL', 'HIGH')
  AND last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- Workspace ACL changes analysis
SELECT 
    actor,
    target_user,
    new_acl,
    acl_changes_count,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions
FROM sec_v_ai_workspace_acl_analysis
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
  AND last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- Sensitive group additions analysis
SELECT 
    actor,
    target_user,
    group_name,
    group_additions_count,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions
FROM sec_v_ai_sensitive_group_analysis
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
  AND last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- Unity Catalog permissions analysis
SELECT 
    actor,
    target_principal,
    securable_type,
    securable_full_name,
    permissions,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions
FROM sec_v_ai_uc_permission_analysis
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
  AND last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- COMMAND ----------

-- Account settings changes analysis
SELECT 
    actor,
    key_type,
    key_name,
    settings,
    setting_changes_count,
    get_json_object(ai_comprehensive_analysis, '$.risk_level') as risk_level,
    get_json_object(ai_comprehensive_analysis, '$.immediate_actions') as immediate_actions
FROM sec_v_ai_account_setting_analysis
WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
  AND last_activity >= current_timestamp() - INTERVAL 24 HOURS;

-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Privilege Escalation Detection Rule (System Tables)
-- MAGIC
-- MAGIC **Rule ID**: AUTH-002  
-- MAGIC **Author**: Security Team  
-- MAGIC **Version**: 2.0.0  
-- MAGIC
-- MAGIC ## Overview
-- MAGIC This detection rule identifies potential privilege escalation attempts by monitoring for unusual elevation of user permissions, role changes, and administrative actions performed by non-administrative users using Databricks system tables.
-- MAGIC
-- MAGIC ## Detection Logic
-- MAGIC - Monitors system.access.audit for permission/role changes
-- MAGIC - Identifies users gaining elevated privileges
-- MAGIC - Detects high-risk administrative actions
-- MAGIC
-- MAGIC ## MITRE ATT&CK Mapping
-- MAGIC - **Tactic**: TA0004 (Privilege Escalation)
-- MAGIC - **Technique**: T1548 (Abuse Elevation Control Mechanism)
-- MAGIC
-- MAGIC ## Data Sources
-- MAGIC - system.access.audit

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration Parameters

-- COMMAND ----------

use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Privileges views

-- COMMAND ----------

-- DBTITLE 1,Account Admin Granted
CREATE VIEW IF NOT EXISTS sec_v_account_admin_assignments AS
SELECT
  event_time,
  service_name,
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  request_params['targetUserName']     AS target_user,
  action_name,
  'ACCOUNT_ADMIN_GRANTED' AS indicator,
  'Critical' as severity
FROM system.access.audit
WHERE service_name = 'accounts'
  AND action_name in ('setAccountAdmin', 'setAdmin');


-- COMMAND ----------

-- DBTITLE 1,Workspace Acl Changed
CREATE VIEW IF NOT EXISTS sec_v_workspace_acl_assignments AS
SELECT
  event_time,
  service_name,
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  request_params['targetUserId']     AS target_user,
  action_name,
  request_params['aclPermissionSet']     AS new_acl,
  'WORKSPACE_ACL_CHANGED' AS indicator,
  'Medium' as severity
FROM system.access.audit
WHERE service_name = 'accounts'
  AND action_name = 'changeDatabricksWorkspaceAcl';


-- COMMAND ----------

-- DBTITLE 1,Added to sensitive group
CREATE VIEW IF NOT EXISTS sec_v_sensitive_group_additions AS
WITH sensitive_groups AS (
  SELECT explode(array('admins','account_admins','metastore_admins','security-admins')) AS group_name
)
SELECT
  a.event_time,
  a.service_name,
  a.workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  a.request_params['targetUserName']   AS target_user,
  a.request_params['targetGroupName']  AS group_name,
  a.action_name,
  'SENSITIVE_GROUP_ADDED' AS indicator,
  'Medium' as severity
FROM system.access.audit a
JOIN sensitive_groups sg
  ON sg.group_name = a.request_params['targetGroupName']
WHERE a.service_name = 'accounts'
  AND a.action_name IN ('addPrincipalToGroup','addPrincipalsToGroup');


-- COMMAND ----------

-- DBTITLE 1,DBSQL ACL changes
CREATE VIEW IF NOT EXISTS sec_v_workspace_db_sql_acl_changes AS
SELECT
  event_time,
  service_name,
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  request_params['targetUserId'] as target_principal,
  action_name,
  request_params['aclPermissionSet']     AS new_acl,
  'WORKSPACE_OR_DBSQL_ACL_CHANGE' AS indicator,
  'Medium' as severity
FROM system.access.audit
WHERE service_name = 'accounts'
  AND action_name IN ('changeDatabricksSqlAcl');

-- COMMAND ----------

-- DBTITLE 1,Unity Catalog - sensitive data permissions
CREATE VIEW IF NOT EXISTS sec_v_uc_permission_escalations AS

WITH sensitive_tables AS
(SELECT
  concat(catalog_name,'.',schema_name,'.', table_name) as securable_full_name
FROM
  system.information_schema.table_tags where tag_value like '%pii%'
)

SELECT
  event_time, service_name, workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  request_params['securable_type']                     AS securable_type,
  request_params['securable_full_name']                AS securable_full_name,
  action_name,
  'UC_PERMISSION_CHANGE'                               AS escalation_type,
  changes_item['principal']                            AS target_principal,
  changes_item['add']                                  AS permissions
FROM system.access.audit a
left join sensitive_tables t ON t.securable_full_name = request_params['securable_full_name']
LATERAL VIEW explode(from_json(request_params['changes'], 'array<struct<principal:string, add:string>>')) AS changes_item
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
  AND (
    request_params['changes'] LIKE '%OWNERSHIP%' OR
    request_params['changes'] LIKE '%ALL_PRIVILEGES%' OR
    request_params['changes'] LIKE '%MANAGE%'
  )
  and (request_params['securable_full_name']  like '%pii%' or request_params['securable_full_name']  = 'system' or t.securable_full_name is not null)
  and request_params['securable_type']   in ('CATALOG', 'SCHEMA', 'TABLE') ;


-- COMMAND ----------

-- DBTITLE 1,Account-level settings changed
CREATE VIEW IF NOT EXISTS sec_v_account_setting_changes AS
SELECT
  event_time,
  service_name,
  workspace_id,
  COALESCE(user_identity.email, identity_metadata.run_by) AS actor,
  action_name,
  request_params['settingKeyTypeName']             AS key_type,
  request_params['settingKeyName']                 AS key_name,
  request_params['settingValueForAudit']           AS settings
FROM system.access.audit
WHERE service_name = 'accounts'
  AND action_name IN ('setSetting','deleteSetting');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Privilege Escalation Risk Assessment & Recommendations

-- COMMAND ----------

-- DBTITLE 1,account admin
-- AI-powered analysis for account admin assignments with actor frequency
CREATE OR REPLACE VIEW sec_v_ai_account_admin_analysis AS
WITH actor_activity AS (
    SELECT 
    service_name,
    workspace_id,
    actor,
    target_user,
    action_name,
        COUNT(*) as admin_assignments_count,
        COUNT(DISTINCT target_user) as unique_targets,
        MIN(event_time) as first_activity,
        MAX(event_time) as last_activity
    FROM sec_v_account_admin_assignments
    WHERE event_time >= current_timestamp() - INTERVAL 168 HOUR
    GROUP BY all
)
SELECT 
    *,
    -- AI-powered comprehensive analysis
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Analyze this account admin assignment for security risks: ' ||
        'Actor=' || actor ||
        ', Target User=' || target_user ||
        ', Action=' || action_name ||
        ', First Activity=' || CAST(first_activity AS STRING) ||
        ', Last Activity=' || CAST(last_activity AS STRING) ||
        ', Actor History: ' || CAST(COALESCE(act.admin_assignments_count, 1) AS STRING) || ' total assignments, ' || 
        CAST(COALESCE(act.unique_targets, 1) AS STRING) || ' unique targets in last 7 days' ||
        'Provide: 1) Risk Level (CRITICAL/HIGH/MEDIUM), 2) Risk Factors, 3) Immediate Actions Required, 4) Investigation Steps, 5) Long-term Controls.',
        responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "account_admin_analysis",
            "schema": {
              "type": "object",
              "properties": {
                "risk_level": {"type": "string"},
                "risk_factors": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "investigation_steps": {"type": "array", "items": {"type": "string"}},
                "long_term_controls": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
    ) as ai_comprehensive_analysis
FROM actor_activity act

-- COMMAND ----------

-- DBTITLE 1,workspace acl
-- AI-powered analysis for workspace ACL changes with actor frequency
CREATE OR REPLACE VIEW sec_v_ai_workspace_acl_analysis AS
WITH actor_activity AS (
    SELECT 
        service_name,
        workspace_id,
        actor,
        target_user,
        action_name,
        new_acl,
        COUNT(*) as acl_changes_count,
        COUNT(DISTINCT target_user) as unique_targets,
        MIN(event_time) as first_activity,
        MAX(event_time) as last_activity
    FROM sec_v_workspace_acl_assignments
    WHERE event_time >= current_timestamp() - INTERVAL 168 HOUR
    GROUP BY all
)
SELECT 
   *,
    -- AI-powered comprehensive analysis
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Analyze this workspace ACL change for security risks: ' ||
        'Actor=' ||actor ||
        ', Target User=' ||target_user ||
        ', New ACL=' ||new_acl ||
        ', Action=' ||action_name ||
        ', First Activity=' || CAST(first_activity AS STRING) ||
        ', Last Activity=' || CAST(last_activity AS STRING) ||
        ', Actor History: ' || CAST(COALESCE(act.acl_changes_count, 1) AS STRING) || ' total ACL changes, ' || 
        CAST(COALESCE(act.unique_targets, 1) AS STRING) || ' unique targets in last 7 days' ||
        '. Provide: 1) Risk Level (HIGH/MEDIUM/LOW), 2) Risk Factors, 3) Immediate Actions, 4) Investigation Steps, 5) Long-term Controls.',
        responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "workspace_acl_analysis",
            "schema": {
              "type": "object",
              "properties": {
                "risk_level": {"type": "string"},
                "risk_factors": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "investigation_steps": {"type": "array", "items": {"type": "string"}},
                "long_term_controls": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
    ) as ai_comprehensive_analysis
FROM actor_activity act;

-- COMMAND ----------

-- DBTITLE 1,sensitive groups
-- AI-powered analysis for sensitive group additions with actor frequency
CREATE OR REPLACE VIEW sec_v_ai_sensitive_group_analysis AS
WITH actor_activity AS (
    SELECT 
        service_name,
        workspace_id,
        actor,
        target_user,
        group_name,
        action_name,
        COUNT(*) as group_additions_count,
        COUNT(DISTINCT target_user) as unique_targets,
        COUNT(DISTINCT group_name) as unique_groups,
        MIN(event_time) as first_activity,
        MAX(event_time) as last_activity
    FROM sec_v_sensitive_group_additions
    WHERE event_time >= current_timestamp() - INTERVAL 168 HOUR
    GROUP BY all
)
SELECT 
    *,
    -- AI-powered comprehensive analysis
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Analyze this sensitive group addition for security risks: ' ||
        'Actor=' || actor ||
        ', Target User=' || target_user ||
        ', Group=' || group_name ||
        ', Action=' || action_name ||
        ', First Activity=' || CAST(first_activity AS STRING) ||
        ', Last Activity=' || CAST(last_activity AS STRING) ||
        ', Actor History: ' || CAST(COALESCE(act.group_additions_count, 1) AS STRING) || ' total group additions, ' || 
        CAST(COALESCE(act.unique_targets, 1) AS STRING) || ' unique targets, ' ||
        CAST(COALESCE(act.unique_groups, 1) AS STRING) || ' unique groups in last 7 days' ||
        '. Provide: 1) Risk Level (HIGH/MEDIUM/LOW), 2) Risk Factors, 3) Immediate Actions, 4) Investigation Steps, 5) Long-term Controls.',
        responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "sensitive_group_analysis",
            "schema": {
              "type": "object",
              "properties": {
                "risk_level": {"type": "string"},
                "risk_factors": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "investigation_steps": {"type": "array", "items": {"type": "string"}},
                "long_term_controls": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
    ) as ai_comprehensive_analysis
FROM actor_activity act;

-- COMMAND ----------

-- DBTITLE 1,uc permissions
-- AI-powered analysis for Unity Catalog permission escalations with actor frequency
CREATE OR REPLACE VIEW sec_v_ai_uc_permission_analysis AS
WITH actor_activity AS (
    SELECT 
      service_name,
      workspace_id,
      actor,
      securable_type,
      securable_full_name,
      action_name,
      escalation_type,
      target_principal,
      permissions,
        COUNT(*) as permission_changes_count,
        COUNT(DISTINCT target_principal) as unique_targets,
        COUNT(DISTINCT securable_full_name) as unique_securables,
        MIN(event_time) as first_activity,
        MAX(event_time) as last_activity
    FROM sec_v_uc_permission_escalations
    WHERE event_time >= current_timestamp() - INTERVAL 168 HOUR
    GROUP BY all
)
SELECT 
   *,
    -- AI-powered comprehensive analysis
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        'Analyze this Unity Catalog permission escalation for security risks: ' ||
        'Actor=' ||actor ||
        ', Target Principal=' ||target_principal ||
        ', Securable=' ||securable_full_name ||
        ', Type=' ||securable_type ||
        ', Permissions=' ||permissions ||
        ', First Activity=' || CAST(first_activity AS STRING) ||
        ', Last Activity=' || CAST(last_activity AS STRING) ||
        ', Actor History: ' || CAST(COALESCE(act.permission_changes_count, 1) AS STRING) || ' total permission changes, ' || 
        CAST(COALESCE(act.unique_targets, 1) AS STRING) || ' unique targets, ' ||
        CAST(COALESCE(act.unique_securables, 1) AS STRING) || ' unique securables in last 7 days' ||
        '. This involves sensitive data permissions. ' ||
        'Provide: 1) Risk Level (HIGH/MEDIUM/LOW), 2) Risk Factors, 3) Immediate Actions, 4) Investigation Steps, 5) Long-term Controls.',
        responseFormat => '{
          "type": "json_schema",
          "json_schema": {
            "name": "uc_permission_analysis",
            "schema": {
              "type": "object",
              "properties": {
                "risk_level": {"type": "string"},
                "risk_factors": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "investigation_steps": {"type": "array", "items": {"type": "string"}},
                "long_term_controls": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
    ) as ai_comprehensive_analysis
FROM actor_activity act;

-- COMMAND ----------

-- DBTITLE 1,account settings
-- AI-powered analysis for account setting changes with actor frequency
CREATE OR REPLACE VIEW sec_v_ai_account_setting_analysis AS
WITH actor_activity AS (
  SELECT
    service_name,
    workspace_id,
    actor,
    action_name,
    key_type,
    key_name,
    settings,
    COUNT(*) as setting_changes_count,
    COUNT(DISTINCT key_name) as unique_keys,
    MIN(event_time) as first_activity,
    MAX(event_time) as last_activity
  FROM
    sec_v_account_setting_changes
  WHERE
    event_time >= current_timestamp() - INTERVAL 168 HOUR
  GROUP BY
    all
)
SELECT
  *,
  -- AI-powered comprehensive analysis
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    'Analyze this account setting change for security risks: ' || 'Actor=' || actor || ', Key Type='
    || key_type
    || ', Key Name='
    || key_name
    || ', Settings='
    || settings
    || ', Action='
    || action_name
    || ', First Activity='
    || CAST(first_activity AS STRING)
    || ', Last Activity='
    || CAST(last_activity AS STRING)
    || ', Actor History: '
    || CAST(COALESCE(act.setting_changes_count, 1) AS STRING)
    || ' total setting changes, '
    || CAST(COALESCE(act.unique_keys, 1) AS STRING)
    || ' unique keys in last 7 days'
    || '. Provide: 1) Risk Level (HIGH/MEDIUM/LOW), 2) Risk Factors, 3) Immediate Actions, 4) Investigation Steps, 5) Long-term Controls.',
    responseFormat =>
      '{
          "type": "json_schema",
          "json_schema": {
            "name": "account_setting_analysis",
            "schema": {
              "type": "object",
              "properties": {
                "risk_level": {"type": "string"},
                "risk_factors": {"type": "array", "items": {"type": "string"}},
                "immediate_actions": {"type": "array", "items": {"type": "string"}},
                "investigation_steps": {"type": "array", "items": {"type": "string"}},
                "long_term_controls": {"type": "array", "items": {"type": "string"}}
              }
            }
          }
        }'
  ) as ai_comprehensive_analysis
FROM
  actor_activity act


-- COMMAND ----------
-- DBTITLE 1,consolidated
-- Comprehensive view combining all privilege escalation analysis with actor frequency
CREATE OR REFRESH MATERIALIZED VIEW sec_mv_ai_privilege_escalation_consolidated AS
SELECT 
    'ACCOUNT_ADMIN' as escalation_type,
    actor,
    target_user,
    admin_assignments_count as actor_activity_count,
    unique_targets as actor_unique_entities,
    first_activity as actor_first_activity,
    last_activity as actor_last_activity,
    ai_comprehensive_analysis
FROM sec_v_ai_account_admin_analysis
UNION ALL
SELECT 
    'WORKSPACE_ACL' as escalation_type,
    actor,
    target_user,
    acl_changes_count as actor_activity_count,
    unique_targets as actor_unique_entities,
    first_activity as actor_first_activity,
    last_activity as actor_last_activity,
    ai_comprehensive_analysis
FROM sec_v_ai_workspace_acl_analysis
UNION ALL
SELECT 
    'SENSITIVE_GROUP' as escalation_type,
    actor,
    target_user,
    group_additions_count as actor_activity_count,
    unique_targets as actor_unique_entities,
    first_activity,
    last_activity,
    ai_comprehensive_analysis
FROM sec_v_ai_sensitive_group_analysis
UNION ALL
SELECT 
    'UC_PERMISSIONS' as escalation_type,
    actor,
    target_principal as affected_entity,
    permission_changes_count as actor_activity_count,
    unique_targets as actor_unique_entities,
    first_activity,
    last_activity,
    ai_comprehensive_analysis
FROM sec_v_ai_uc_permission_analysis
UNION ALL
SELECT 
    'ACCOUNT_SETTINGS' as escalation_type,
    actor,
    key_type as affected_entity,
    setting_changes_count as actor_activity_count,
    unique_keys as actor_unique_entities,
    first_activity,
    last_activity,
    ai_comprehensive_analysis
FROM sec_v_ai_account_setting_analysis;


-- COMMAND ----------

SELECT * from sec_mv_ai_privilege_escalation_consolidated where ai_comprehensive_analysis is not null and actor is not null limit 10;
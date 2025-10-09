-- Databricks notebook source
-- MAGIC %md
# Databricks Security Detection Alerts 

generated from rules.yml

-- COMMAND ----------
use catalog identifier(:catalog);
use schema identifier(:schema);
-- COMMAND ----------


-- Alert for failed login attempts
SELECT create_alert(
    display_name => 'failed_login_alert',
    query_text => format_string('SELECT COUNT(*) AS value
 FROM %s.%s.sec_v_auth_events
 WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
', :catalog, :schema),
    warehouse_id => :warehouse_id,
    comparison_operator => 'GREATER_THAN',
    threshold_value => 0,
    empty_result_state => 'UNKNOWN',
    user_email => :user_email,
    notify_on_ok => true,
    retrigger_seconds => 0,
    cron_schedule => '0 0 10 1/7 * ?',
    timezone_id => 'UTC',
    pause_status => 'PAUSED'
    ) as alert;


-- COMMAND ----------


-- Alert for account admin assignments
SELECT create_alert(
    display_name => 'account_admin_assignment',
    query_text => format_string('SELECT COUNT(*) AS value
   FROM %s.%s.sec_v_account_admin_assignments
   WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
', :catalog, :schema),
    warehouse_id => :warehouse_id,
    comparison_operator => 'GREATER_THAN',
    threshold_value => 5,
    empty_result_state => 'UNKNOWN',
    user_email => :user_email,
    notify_on_ok => true,
    retrigger_seconds => 0,
    cron_schedule => '0 0 10 1/7 * ?',
    timezone_id => 'UTC',
    pause_status => 'PAUSED'
    ) as alert;


-- COMMAND ----------


-- Alert for large data exports
SELECT create_alert(
    display_name => 'data_export_alert',
    query_text => format_string('SELECT count(*) AS value
 FROM %s.%s.sec_mv_data_export_detection
 WHERE event_time >= current_timestamp() - INTERVAL 168 HOURS
', :catalog, :schema),
    warehouse_id => :warehouse_id,
    comparison_operator => 'GREATER_THAN',
    threshold_value => 10,
    empty_result_state => 'UNKNOWN',
    user_email => :user_email,
    notify_on_ok => true,
    retrigger_seconds => 0,
    cron_schedule => '0 0 10 1/7 * ?',
    timezone_id => 'UTC',
    pause_status => 'PAUSED'
    ) as alert;


-- COMMAND ----------


-- Alert for Unity Catalog permission escalations
SELECT create_alert(
    display_name => 'uc_permission_escalation',
    query_text => format_string('SELECT COUNT(*) AS value
   FROM %s.%s.sec_v_uc_permission_escalations
   WHERE event_time >= current_timestamp() - INTERVAL 720 HOURS
', :catalog, :schema),
    warehouse_id => :warehouse_id,
    comparison_operator => 'GREATER_THAN',
    threshold_value => 5,
    empty_result_state => 'UNKNOWN',
    user_email => :user_email,
    notify_on_ok => true,
    retrigger_seconds => 0,
    cron_schedule => '0 0 10 1/7 * ?',
    timezone_id => 'UTC',
    pause_status => 'PAUSED'
    ) as alert;


-- COMMAND ----------


-- Alert for data exports based on AI analysis
SELECT create_alert(
    display_name => 'data_export_ai_alert',
    query_text => format_string('SELECT sum(export_count) AS value
 FROM %s.%s.sec_mv_ai_data_export_analysis
 WHERE get_json_object(ai_comprehensive_analysis, "$.risk_level") IN ("HIGH", "CRITICAL")
', :catalog, :schema),
    warehouse_id => :warehouse_id,
    comparison_operator => 'GREATER_THAN',
    threshold_value => 10,
    empty_result_state => 'UNKNOWN',
    user_email => :user_email,
    notify_on_ok => true,
    retrigger_seconds => 0,
    cron_schedule => '0 0 10 1/7 * ?',
    timezone_id => 'UTC',
    pause_status => 'PAUSED'
    ) as alert;


-- COMMAND ----------

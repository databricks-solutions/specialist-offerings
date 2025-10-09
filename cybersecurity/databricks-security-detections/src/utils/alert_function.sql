-- Databricks notebook source
use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time, os, json, random, string
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC w = WorkspaceClient()
-- MAGIC
-- MAGIC # create token with time to live (TTL) of 1 week
-- MAGIC token = w.tokens.create(comment=f"sdk-{time.time_ns()}", lifetime_seconds=604800).token_value
-- MAGIC
-- MAGIC # Get the host parameter from the notebook context
-- MAGIC host = dbutils.widgets.get("host")
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC CREATE CONNECTION IF NOT EXISTS databricks_rest_api TYPE HTTP
-- MAGIC OPTIONS (
-- MAGIC   host '{host}',
-- MAGIC   base_path '/api/',
-- MAGIC   bearer_token '{token}'
-- MAGIC )
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Create alert function
CREATE OR REPLACE FUNCTION create_alert(
  -- Required parameters
  display_name STRING,
  query_text STRING,                 -- e.g. 'select count(*) as ct from my_table'
  warehouse_id STRING,

  -- Alert configuration
  comparison_operator STRING DEFAULT 'GREATER_THAN',  -- GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, EQUAL, NOT_EQUAL
  threshold_value DOUBLE DEFAULT 0.0,
  empty_result_state STRING DEFAULT 'UNKNOWN',

  -- Notification settings
  user_email STRING DEFAULT NULL,
  notify_on_ok BOOLEAN DEFAULT TRUE,
  retrigger_seconds INT DEFAULT 0,

  -- Schedule settings
  cron_schedule STRING DEFAULT '0 0 10 1/7 * ?',
  timezone_id STRING DEFAULT 'UTC',
  pause_status STRING DEFAULT 'PAUSED'
)
COMMENT 'Creates a Databricks alert using parameters, building JSON via to_json(named_struct(...))'
RETURN
SELECT
  http_request(
    conn   => 'databricks_rest_api',
    method => 'POST',
    path   => '2.0/alerts',
    json   =>
      to_json(
        named_struct(
          'display_name',  display_name,
          'query_text',    query_text,
          'warehouse_id',  warehouse_id,

          -- evaluation block
          'evaluation', named_struct(
            'comparison_operator', comparison_operator,
            'empty_result_state',  empty_result_state,
            'source', named_struct(
              'aggregation', 'FIRST',   -- default aggregation
              'display',     'value',   -- default display name
              'name',        'value'    -- default column name
            ),
            'threshold', named_struct(
              'value', named_struct('double_value', threshold_value)
            )
          ),

          -- notification block
          'notification', named_struct(
            'notify_on_ok',      notify_on_ok,
            'retrigger_seconds', retrigger_seconds,
            'subscriptions', array(named_struct('user_email', user_email))
          ),

          -- schedule block
          'schedule', named_struct(
            'pause_status',         pause_status,
            'quartz_cron_schedule', cron_schedule,
            'timezone_id',          timezone_id
          )
        )
      )
  ).text AS resp;



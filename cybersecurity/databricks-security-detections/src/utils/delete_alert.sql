-- Databricks notebook source
use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

-- DBTITLE 1,List alerts
-- set the owner you want
DECLARE or replace owner STRING DEFAULT 'anhhoang.chu@databricks.com';

create or replace temp view alerts_to_delete 
as
WITH r AS (
  SELECT http_request(
    conn=>'databricks_rest_api', method=>'GET', path=>'2.0/alerts'
  ) AS resp
),
parsed AS (
  SELECT from_json(
           resp.text,
           'STRUCT<results: ARRAY<STRUCT<
              id: STRING,
              display_name: STRING,
              owner_user_name: STRING,
              run_as_user_name: STRING,
              create_time: STRING,
              update_time: STRING,
              parent_path: STRING,
              query_text: STRING,
              warehouse_id: STRING,
              lifecycle_state: STRING,
              evaluation: STRUCT<
              comparison_operator: STRING,
              empty_result_state: STRING,
              state: STRING,
              source: STRUCT<name: STRING, display: STRING>,
              threshold: STRUCT<value: STRUCT<double_value: DOUBLE>>,
              notification: STRUCT<notify_on_ok: BOOLEAN, retrigger_seconds: INT>
              >,
              schedule: STRUCT<quartz_cron_schedule: STRING, timezone_id: STRING, pause_status: STRING>
           >>>'
         ) AS payload
  FROM r
),
alerts AS (
  SELECT explode(payload.results) AS a FROM parsed
)
SELECT
  a.id,
  a.display_name,
  a.owner_user_name,
  a.create_time,
  a.update_time,
  a.schedule.quartz_cron_schedule AS cron,
  a.schedule.pause_status         AS paused,
  a.evaluation.comparison_operator AS op,
  a.evaluation.threshold.value.double_value AS threshold
FROM alerts
WHERE a.owner_user_name = owner;

select * from alerts_to_delete;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC
-- MAGIC alerts_to_delete = spark.table("alerts_to_delete").collect()
-- MAGIC
-- MAGIC for row in alerts_to_delete:
-- MAGIC     resp = spark.sql(f"""
-- MAGIC         SELECT http_request(
-- MAGIC             conn   => 'databricks_rest_api',
-- MAGIC             method => 'DELETE',
-- MAGIC             path   => '2.0/alerts/{row.id}'
-- MAGIC         ) AS response
-- MAGIC     """).collect()[0].response
-- MAGIC

-- COMMAND ----------

select * from alerts_to_delete;
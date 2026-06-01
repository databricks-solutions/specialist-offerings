# EMR Step to Databricks Task — Detailed Conversion Examples

## 1. Spark Submit Step → spark_python_task

### Before: EMR Step (JSON)

```json
{
  "Name": "Run ETL Job",
  "ActionOnFailure": "CONTINUE",
  "HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
      "spark-submit",
      "--deploy-mode", "cluster",
      "--master", "yarn",
      "--conf", "spark.executor.memory=8g",
      "--conf", "spark.executor.cores=4",
      "--conf", "spark.dynamicAllocation.enabled=true",
      "--py-files", "s3://my-bucket/libs/utils.zip",
      "s3://my-bucket/scripts/etl_job.py",
      "--input", "s3://my-bucket/raw/",
      "--output", "s3://my-bucket/processed/",
      "--date", "2024-01-15"
    ]
  }
}
```

### After: Databricks Task (YAML — DABs)

```yaml
tasks:
  - task_key: run_etl_job
    spark_python_task:
      python_file: /Workspace/Repos/etl/etl_job.py
      parameters:
        - "--input"
        - "s3://my-bucket/raw/"
        - "--output"
        - "s3://my-bucket/processed/"
        - "--date"
        - "2024-01-15"
    libraries:
      - pypi:
          package: "utils"  # Or upload to workspace/volumes
    new_cluster:
      spark_version: "15.4.x-scala2.12"
      node_type_id: "m5.2xlarge"
      num_workers: 4
      spark_conf:
        spark.executor.memory: "8g"
        spark.executor.cores: "4"
        # Note: dynamic allocation removed — use autoscale instead
      autoscale:
        min_workers: 2
        max_workers: 8
```

### Parameter Mapping

| EMR spark-submit arg | Databricks equivalent |
|---|---|
| `--master yarn` | Not needed (Databricks manages) |
| `--deploy-mode cluster` | Not needed (always cluster mode) |
| `--conf spark.X=Y` | `new_cluster.spark_conf` |
| `--py-files` | `libraries` section or upload to Workspace |
| `--jars` | `libraries` with `jar` type |
| `--packages` | `libraries` with `maven` type |
| Script path (s3://) | `python_file` in Workspace or Volumes |
| Script arguments | `parameters` list |

---

## 2. Custom JAR Step → spark_jar_task

### Before: EMR Step (JSON)

```json
{
  "Name": "Run Custom Transform",
  "ActionOnFailure": "TERMINATE_CLUSTER",
  "HadoopJarStep": {
    "Jar": "s3://my-bucket/jars/data-transform-1.0.jar",
    "MainClass": "com.company.DataTransform",
    "Args": ["--env", "production", "--table", "events"],
    "Properties": [
      { "Key": "spark.executor.memory", "Value": "16g" }
    ]
  }
}
```

### After: Databricks Task (YAML — DABs)

```yaml
tasks:
  - task_key: run_custom_transform
    spark_jar_task:
      main_class_name: "com.company.DataTransform"
      parameters:
        - "--env"
        - "production"
        - "--table"
        - "events"
    libraries:
      - jar: "dbfs:/FileStore/jars/data-transform-1.0.jar"
        # Or use Unity Catalog Volumes: /Volumes/catalog/schema/jars/data-transform-1.0.jar
    new_cluster:
      spark_version: "15.4.x-scala2.12"
      node_type_id: "r5.4xlarge"
      num_workers: 6
      spark_conf:
        spark.executor.memory: "16g"
    # TERMINATE_CLUSTER equivalent: job cluster auto-terminates after task
```

---

## 3. Hive Script Step → sql_task

### Before: EMR Step (JSON)

```json
{
  "Name": "Run Hive Aggregation",
  "ActionOnFailure": "CANCEL_AND_WAIT",
  "HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
      "hive-script",
      "--run-hive-script",
      "--args",
      "-f", "s3://my-bucket/scripts/aggregate.hql",
      "-d", "INPUT_TABLE=raw_events",
      "-d", "OUTPUT_TABLE=daily_aggregates",
      "-d", "PROCESS_DATE=2024-01-15"
    ]
  }
}
```

**Original Hive script (aggregate.hql):**
```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${OUTPUT_TABLE} PARTITION (dt='${PROCESS_DATE}')
SELECT
  user_id,
  COUNT(*) as event_count,
  SUM(amount) as total_amount
FROM ${INPUT_TABLE}
WHERE dt = '${PROCESS_DATE}'
GROUP BY user_id;
```

### After: Databricks sql_task (YAML — DABs)

```yaml
tasks:
  - task_key: run_hive_aggregation
    sql_task:
      file:
        path: /Workspace/Repos/sql/aggregate.sql
      warehouse_id: "abc123def456"
      parameters:
        INPUT_TABLE: "catalog.schema.raw_events"
        OUTPUT_TABLE: "catalog.schema.daily_aggregates"
        PROCESS_DATE: "2024-01-15"
```

**Converted SQL (aggregate.sql):**
```sql
-- Dynamic partitioning not needed — Delta handles it
-- Unity Catalog paths replace Hive metastore references

INSERT INTO {{OUTPUT_TABLE}}
SELECT
  user_id,
  COUNT(*) AS event_count,
  SUM(amount) AS total_amount,
  '{{PROCESS_DATE}}' AS dt
FROM {{INPUT_TABLE}}
WHERE dt = '{{PROCESS_DATE}}'
GROUP BY user_id;
```

---

## 4. Multi-Step Flow → Multi-Task Job with depends_on

### Before: EMR Job Flow with Steps (JSON)

```json
{
  "Name": "Daily ETL Pipeline",
  "Steps": [
    {
      "Name": "Step 1 - Ingest Raw Data",
      "ActionOnFailure": "TERMINATE_CLUSTER",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "s3://bucket/scripts/ingest.py"]
      }
    },
    {
      "Name": "Step 2 - Clean and Validate",
      "ActionOnFailure": "TERMINATE_CLUSTER",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "s3://bucket/scripts/clean.py"]
      }
    },
    {
      "Name": "Step 3 - Build Aggregates",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "s3://bucket/scripts/aggregate.py"]
      }
    },
    {
      "Name": "Step 4 - Export to Redshift",
      "ActionOnFailure": "CONTINUE",
      "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "s3://bucket/scripts/export.py"]
      }
    }
  ]
}
```

### After: Databricks Multi-Task Job (YAML — DABs)

```yaml
resources:
  jobs:
    daily_etl_pipeline:
      name: "Daily ETL Pipeline"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"  # Daily at 6 AM UTC
        timezone_id: "UTC"

      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "m5.2xlarge"
            autoscale:
              min_workers: 2
              max_workers: 8

      tasks:
        - task_key: ingest_raw_data
          spark_python_task:
            python_file: /Workspace/Repos/etl/ingest.py
          job_cluster_key: etl_cluster
          # ActionOnFailure=TERMINATE → no retry, fail the job

        - task_key: clean_and_validate
          depends_on:
            - task_key: ingest_raw_data
          spark_python_task:
            python_file: /Workspace/Repos/etl/clean.py
          job_cluster_key: etl_cluster

        - task_key: build_aggregates
          depends_on:
            - task_key: clean_and_validate
          spark_python_task:
            python_file: /Workspace/Repos/etl/aggregate.py
          job_cluster_key: etl_cluster
          # ActionOnFailure=CONTINUE → retry or allow downstream to run
          retry_on_timeout: true
          max_retries: 1

        - task_key: export_to_warehouse
          depends_on:
            - task_key: build_aggregates
              outcome: "success"  # Only run if aggregates succeed
          spark_python_task:
            python_file: /Workspace/Repos/etl/export.py
          job_cluster_key: etl_cluster
          max_retries: 2
          min_retry_interval_millis: 60000

      email_notifications:
        on_failure:
          - "team@company.com"

      webhook_notifications:
        on_failure:
          - id: "webhook-id-for-pagerduty"
```

---

## 5. ActionOnFailure → Retry Policy + Notifications

### EMR ActionOnFailure Options

| EMR ActionOnFailure | Meaning | Databricks Equivalent |
|---|---|---|
| `TERMINATE_CLUSTER` | Stop everything, kill cluster | Task fails → job fails (default behavior); job cluster auto-terminates |
| `TERMINATE_JOB_FLOW` | Same as TERMINATE_CLUSTER | Same as above |
| `CANCEL_AND_WAIT` | Cancel remaining steps, keep cluster alive | Use `depends_on` with outcome conditions; all-purpose cluster stays alive |
| `CONTINUE` | Skip failure, run next step | Set `depends_on` without outcome filter, or use `run_if: "AT_LEAST_ONE_FAILED"` on cleanup tasks |

### Retry Configuration Example

```yaml
tasks:
  - task_key: resilient_task
    spark_python_task:
      python_file: /Workspace/Repos/etl/transform.py
    # Retry policy (replaces CONTINUE with retry logic)
    max_retries: 3
    min_retry_interval_millis: 30000   # 30 seconds between retries
    retry_on_timeout: true
    timeout_seconds: 3600              # 1 hour timeout per attempt

  - task_key: cleanup_task
    depends_on:
      - task_key: resilient_task
    run_if: "ALL_DONE"  # Runs regardless of upstream success/failure
    spark_python_task:
      python_file: /Workspace/Repos/etl/cleanup.py
```

### run_if Options

| Value | Behavior |
|---|---|
| `ALL_SUCCESS` | Default — run only if all upstream tasks succeed |
| `AT_LEAST_ONE_SUCCESS` | Run if any upstream task succeeds |
| `NONE_FAILED` | Run if no upstream tasks failed (skipped counts as OK) |
| `ALL_DONE` | Run regardless — useful for cleanup/notification tasks |
| `AT_LEAST_ONE_FAILED` | Run only on failure — useful for error handling |

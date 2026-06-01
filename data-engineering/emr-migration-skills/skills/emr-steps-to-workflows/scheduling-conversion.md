# Scheduling Conversion: EMR to Databricks

## Cron Format Conversion

### Standard Cron (5-field) vs Quartz Cron (6-field)

EMR scheduling (via CloudWatch Events, EventBridge, or Airflow) typically uses **standard 5-field cron** or **EventBridge rate expressions**. Databricks uses **Quartz cron format** with 6 fields (includes seconds).

```
Standard cron (5 fields):  MIN HOUR DOM MON DOW
Quartz cron (6 fields):    SEC MIN HOUR DOM MON DOW
```

### Conversion Rule

Prepend `0` (for seconds) to any standard 5-field cron expression.

### Common Schedule Conversions

| Schedule | Standard Cron / EventBridge | Databricks Quartz Cron |
|---|---|---|
| Every hour | `0 * * * *` | `0 0 * * * ?` |
| Daily at 6 AM UTC | `0 6 * * *` | `0 0 6 * * ?` |
| Daily at midnight | `0 0 * * *` | `0 0 0 * * ?` |
| Weekdays at 8 AM | `0 8 * * 1-5` | `0 0 8 ? * MON-FRI` |
| Every 15 minutes | `*/15 * * * *` | `0 */15 * * * ?` |
| Every 6 hours | `0 */6 * * *` | `0 0 */6 * * ?` |
| First of month at 1 AM | `0 1 1 * *` | `0 0 1 1 * ?` |
| Sunday at 3 AM | `0 3 * * 0` | `0 0 3 ? * SUN` |
| Every 30 minutes M-F | `*/30 * * * 1-5` | `0 */30 * ? * MON-FRI` |

### Quartz Cron Rules

- **6 fields required**: `SEC MIN HOUR DOM MON DOW`
- **`?` (no specific value)**: Required in either DOM or DOW — you cannot specify both. Use `?` for the one you do not need.
- **Day-of-week values**: `SUN`=1, `MON`=2, ..., `SAT`=7 (or use names)
- **Seconds**: Databricks ignores sub-minute precision for job scheduling, but the field is required. Always use `0`.

### DABs YAML Example

```yaml
resources:
  jobs:
    my_job:
      name: "My Scheduled Job"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"  # Daily at 6 AM
        timezone_id: "America/New_York"         # Always specify timezone
        pause_status: "UNPAUSED"                # PAUSED or UNPAUSED
```

### EventBridge Rate Expression Conversion

| EventBridge Rate | Databricks Equivalent |
|---|---|
| `rate(1 hour)` | `quartz_cron_expression: "0 0 * * * ?"` |
| `rate(5 minutes)` | `quartz_cron_expression: "0 */5 * * * ?"` |
| `rate(1 day)` | `quartz_cron_expression: "0 0 0 * * ?"` |

---

## Event-Based Triggers

### S3 Events to File Arrival Triggers

**EMR Pattern**: S3 event -> EventBridge / Lambda -> Start EMR Job Flow

```python
# AWS Lambda triggered by S3 event
import boto3

def handler(event, context):
    emr = boto3.client('emr')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    emr.run_job_flow(
        Name='triggered-etl',
        Steps=[{
            'Name': 'Process File',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 's3://bucket/scripts/process.py', f's3://{bucket}/{key}']
            }
        }],
        # ... cluster config
    )
```

**Databricks Pattern**: File arrival trigger

```yaml
resources:
  jobs:
    file_triggered_etl:
      name: "File Arrival ETL"
      trigger:
        file_arrival:
          url: "s3://my-bucket/incoming/"         # Watch this location
          min_time_between_triggers_seconds: 60   # Minimum interval
          wait_after_last_change_seconds: 30      # Wait for file stability

      tasks:
        - task_key: process_file
          spark_python_task:
            python_file: /Workspace/Repos/etl/process.py
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "m5.xlarge"
            num_workers: 2
```

### SQS/SNS Trigger to Webhook Trigger

**EMR Pattern**: SQS message -> Lambda -> Start EMR Step

**Databricks Pattern**: Use the Jobs API `POST /api/2.1/jobs/run-now` from any webhook source.

```bash
# Trigger a Databricks job via API (from Lambda, EventBridge, or any webhook)
curl -X POST "https://<workspace>/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -d '{"job_id": 12345, "python_params": ["s3://bucket/incoming/file.parquet"]}'
```

---

## Dependency Chains

### Sequential Steps to DAG with depends_on

**EMR**: Steps execute sequentially within a job flow (no parallelism).

**Databricks**: Tasks form a DAG — parallel by default, sequential only when `depends_on` is specified.

```yaml
tasks:
  # No dependencies — runs immediately
  - task_key: ingest_source_a
    spark_python_task:
      python_file: /Workspace/Repos/etl/ingest_a.py

  # No dependencies — runs in parallel with ingest_source_a
  - task_key: ingest_source_b
    spark_python_task:
      python_file: /Workspace/Repos/etl/ingest_b.py

  # Depends on both ingests completing
  - task_key: join_sources
    depends_on:
      - task_key: ingest_source_a
      - task_key: ingest_source_b
    spark_python_task:
      python_file: /Workspace/Repos/etl/join.py

  # Sequential chain
  - task_key: export
    depends_on:
      - task_key: join_sources
    spark_python_task:
      python_file: /Workspace/Repos/etl/export.py
```

### Conditional Execution

```yaml
tasks:
  - task_key: main_etl
    spark_python_task:
      python_file: /Workspace/Repos/etl/main.py

  - task_key: success_notification
    depends_on:
      - task_key: main_etl
    run_if: "ALL_SUCCESS"
    notebook_task:
      notebook_path: /Workspace/Repos/etl/notify_success

  - task_key: failure_handler
    depends_on:
      - task_key: main_etl
    run_if: "AT_LEAST_ONE_FAILED"
    notebook_task:
      notebook_path: /Workspace/Repos/etl/handle_failure

  - task_key: cleanup
    depends_on:
      - task_key: success_notification
      - task_key: failure_handler
    run_if: "ALL_DONE"
    notebook_task:
      notebook_path: /Workspace/Repos/etl/cleanup
```

---

## Time Windows and SLA Configuration

### EMR Pattern: CloudWatch Alarms on Step Duration

```python
# CloudWatch alarm if EMR step exceeds 2 hours
cloudwatch.put_metric_alarm(
    AlarmName='emr-step-sla',
    MetricName='StepDuration',
    Threshold=7200,
    ComparisonOperator='GreaterThanThreshold',
)
```

### Databricks Pattern: Timeout + Notifications

```yaml
resources:
  jobs:
    sla_monitored_job:
      name: "SLA Monitored ETL"
      timeout_seconds: 7200  # Job-level: 2-hour SLA

      tasks:
        - task_key: critical_etl
          spark_python_task:
            python_file: /Workspace/Repos/etl/critical.py
          timeout_seconds: 3600  # Task-level: 1-hour timeout

      email_notifications:
        on_duration_warning_threshold_exceeded:
          - "oncall@company.com"

      health:
        rules:
          - metric: "RUN_DURATION_SECONDS"
            op: "GREATER_THAN"
            value: 5400  # Warn if exceeding 90 minutes

      webhook_notifications:
        on_failure:
          - id: "pagerduty-webhook-id"
```

### Databricks System Tables for SLA Monitoring

```sql
-- Query job run durations from system tables
SELECT
  job_id,
  run_id,
  result_state,
  TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes
FROM system.lakeflow.job_run_timeline
WHERE job_id = 12345
  AND start_time > CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY start_time DESC;

-- Identify SLA breaches
SELECT
  job_id,
  run_id,
  TIMESTAMPDIFF(MINUTE, start_time, end_time) AS duration_minutes
FROM system.lakeflow.job_run_timeline
WHERE job_id = 12345
  AND TIMESTAMPDIFF(MINUTE, start_time, end_time) > 120  -- 2-hour SLA
  AND start_time > CURRENT_DATE - INTERVAL 30 DAYS;
```

---

## Retry and Failure Handling Comparison

| Feature | EMR | Databricks |
|---|---|---|
| Retry on failure | Not built-in (implement in Lambda/Step Functions) | `max_retries` per task |
| Retry interval | Custom implementation | `min_retry_interval_millis` |
| Retry on timeout | Custom implementation | `retry_on_timeout: true` |
| Failure notification | CloudWatch Events to SNS | Email, webhook, PagerDuty native |
| Partial retry | Re-run entire job flow | Re-run from failed task (`repair_run` API) |
| Job-level timeout | Not built-in | `timeout_seconds` on job and task |
| Concurrent run control | Custom implementation | `max_concurrent_runs` |

### Repair Run (Resume from Failure)

A key Databricks advantage: if a task in a multi-task job fails, you can repair (re-run) just the failed task and its downstream dependencies without re-running everything.

```bash
# Repair a failed run — re-runs only failed tasks and their dependents
databricks jobs repair-run --run-id 98765 --rerun-tasks '["failed_task_key"]'
```

```yaml
# In DABs, this is an operational action, not a config item.
# But you can configure retry policies to auto-handle:
tasks:
  - task_key: flaky_task
    max_retries: 3
    min_retry_interval_millis: 60000  # Wait 1 minute between retries
    retry_on_timeout: true
```

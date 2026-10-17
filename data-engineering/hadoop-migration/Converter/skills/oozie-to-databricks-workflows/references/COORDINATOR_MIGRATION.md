# Oozie Coordinator → Databricks Triggers

## Cron-Based Coordinator

```xml
<!-- Oozie Coordinator: daily at midnight -->
<coordinator-app name="daily-etl"
    frequency="${coord:days(1)}"
    start="2024-01-01T00:00Z"
    end="2025-12-31T23:59Z"
    timezone="UTC">
    <action>
        <workflow>
            <app-path>/user/oozie/workflows/etl-daily</app-path>
            <configuration>
                <property>
                    <name>date</name>
                    <value>${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}</value>
                </property>
            </configuration>
        </workflow>
    </action>
</coordinator-app>
```

```json
// Databricks Job with schedule
{
  "name": "daily-etl",
  "schedule": {
    "quartz_cron_expression": "0 0 0 * * ?",
    "timezone_id": "UTC"
  },
  "parameters": [
    {"name": "date", "default": ""}
  ],
  "tasks": [...]
}
// Note: Date parameter auto-set by trigger or use dbutils.widgets
```

## Frequency Mapping

| Oozie Frequency | Cron Expression | Notes |
|---|---|---|
| `${coord:minutes(5)}` | `0 */5 * * * ?` | Every 5 minutes |
| `${coord:minutes(30)}` | `0 */30 * * * ?` | Every 30 minutes |
| `${coord:hours(1)}` | `0 0 * * * ?` | Hourly |
| `${coord:hours(6)}` | `0 0 */6 * * ?` | Every 6 hours |
| `${coord:days(1)}` | `0 0 0 * * ?` | Daily at midnight |
| `${coord:days(7)}` | `0 0 0 ? * MON` | Weekly on Monday |
| `${coord:months(1)}` | `0 0 0 1 * ?` | First of month |
| Custom: `10 * * * *` | `0 10 * * * ?` | Note: Quartz adds seconds |

## Data-Availability Coordinator

```xml
<!-- Oozie: Wait for input data before running -->
<coordinator-app name="data-dependent-etl"
    frequency="${coord:days(1)}"
    start="2024-01-01T00:00Z" end="2025-12-31T23:59Z" timezone="UTC">
    <datasets>
        <dataset name="input" frequency="${coord:days(1)}"
            initial-instance="2024-01-01T00:00Z" timezone="UTC">
            <uri-template>/data/raw/${YEAR}/${MONTH}/${DAY}</uri-template>
        </dataset>
    </datasets>
    <input-events>
        <data-in name="input_data" dataset="input">
            <instance>${coord:current(0)}</instance>
        </data-in>
    </input-events>
    <action>
        <workflow>
            <app-path>/user/oozie/workflows/process</app-path>
        </workflow>
    </action>
</coordinator-app>
```

### Databricks: File Arrival Trigger

```json
{
  "name": "data-dependent-etl",
  "trigger": {
    "file_arrival": {
      "url": "s3://bucket/data/raw/",
      "min_time_between_triggers_seconds": 86400,
      "wait_after_last_change_seconds": 120
    }
  },
  "tasks": [...]
}
```

### Databricks: Alternative — Cron + Check in Notebook

```json
{
  "name": "data-dependent-etl",
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "UTC"
  },
  "tasks": [
    {
      "task_key": "check_data",
      "notebook_task": {
        "notebook_path": "/Workspace/workflows/check_data_available"
      }
    },
    {
      "task_key": "process",
      "depends_on": [{"task_key": "check_data"}],
      "notebook_task": {
        "notebook_path": "/Workspace/workflows/process"
      }
    }
  ]
}
```

## Bundle → Multiple Jobs

```xml
<!-- Oozie Bundle: groups related coordinators -->
<bundle-app name="analytics-bundle">
    <coordinator name="ingest-coord">
        <app-path>/user/oozie/coordinators/ingest</app-path>
    </coordinator>
    <coordinator name="transform-coord">
        <app-path>/user/oozie/coordinators/transform</app-path>
    </coordinator>
    <coordinator name="report-coord">
        <app-path>/user/oozie/coordinators/report</app-path>
    </coordinator>
</bundle-app>
```

```
// Databricks: Create separate Jobs, link with run_job_task if needed
// Or combine into one multi-task Job with dependencies
// Bundles don't have a direct equivalent — just create multiple Jobs
```

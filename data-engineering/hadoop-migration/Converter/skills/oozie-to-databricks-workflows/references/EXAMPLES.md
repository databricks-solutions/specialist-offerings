# Examples: Oozie → Databricks Workflows

## Example 1: Simple Linear Workflow

### Before (Oozie workflow.xml)
```xml
<workflow-app xmlns="uri:oozie:workflow:0.5" name="daily-etl">
    <start to="ingest"/>
    <action name="ingest">
        <sqoop xmlns="uri:oozie:sqoop-action:0.4">
            <arg>import</arg>
            <arg>--connect</arg><arg>jdbc:mysql://host/db</arg>
            <arg>--table</arg><arg>orders</arg>
            <arg>--target-dir</arg><arg>/data/raw/orders/${date}</arg>
            <arg>--num-mappers</arg><arg>4</arg>
        </sqoop>
        <ok to="transform"/>
        <error to="kill"/>
    </action>
    <action name="transform">
        <spark xmlns="uri:oozie:spark-action:0.2">
            <master>yarn</master>
            <mode>cluster</mode>
            <class>com.example.TransformOrders</class>
            <jar>lib/transform.jar</jar>
            <arg>--date</arg><arg>${date}</arg>
        </spark>
        <ok to="load"/>
        <error to="kill"/>
    </action>
    <action name="load">
        <hive2 xmlns="uri:oozie:hive2-action:0.2">
            <jdbc-url>jdbc:hive2://hiveserver:10000</jdbc-url>
            <script>scripts/load_warehouse.hql</script>
            <param>date=${date}</param>
        </hive2>
        <ok to="end"/>
        <error to="kill"/>
    </action>
    <kill name="kill"><message>Failed: ${wf:errorMessage(wf:lastErrorNode())}</message></kill>
    <end name="end"/>
</workflow-app>
```

### After (Databricks Job JSON)
```json
{
  "name": "daily-etl",
  "parameters": [
    {"name": "date", "default": ""}
  ],
  "email_notifications": {
    "on_failure": ["data-team@company.com"]
  },
  "tasks": [
    {
      "task_key": "ingest",
      "notebook_task": {
        "notebook_path": "/Workspace/workflows/daily-etl/ingest_orders",
        "base_parameters": {"date": "{{job.parameters.date}}"}
      },
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2
      }
    },
    {
      "task_key": "transform",
      "depends_on": [{"task_key": "ingest"}],
      "spark_jar_task": {
        "main_class_name": "com.example.TransformOrders",
        "parameters": ["--date", "{{job.parameters.date}}"]
      },
      "libraries": [{"jar": "dbfs:/libs/transform.jar"}],
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale": {"min_workers": 2, "max_workers": 8}
      }
    },
    {
      "task_key": "load",
      "depends_on": [{"task_key": "transform"}],
      "sql_task": {
        "file": {"path": "/Workspace/workflows/daily-etl/load_warehouse.sql"},
        "warehouse_id": "abc123",
        "parameters": {"date": "{{job.parameters.date}}"}
      }
    }
  ]
}
```

## Example 2: Fork/Join with Decision

### Before (Oozie)
```xml
<workflow-app xmlns="uri:oozie:workflow:0.5" name="parallel-processing">
    <start to="validate"/>
    <action name="validate">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>validate.sh</exec>
        </shell>
        <ok to="check_result"/>
        <error to="kill"/>
    </action>
    <decision name="check_result">
        <switch>
            <case to="full_pipeline">${wf:actionData('validate')['mode'] eq 'full'}</case>
            <default to="incremental_only"/>
        </switch>
    </decision>
    <fork name="full_pipeline">
        <path start="process_orders"/>
        <path start="process_customers"/>
        <path start="process_products"/>
    </fork>
    <action name="process_orders"><spark>...</spark><ok to="join1"/><error to="kill"/></action>
    <action name="process_customers"><spark>...</spark><ok to="join1"/><error to="kill"/></action>
    <action name="process_products"><spark>...</spark><ok to="join1"/><error to="kill"/></action>
    <join name="join1" to="aggregate"/>
    <action name="aggregate"><hive>...</hive><ok to="end"/><error to="kill"/></action>
    <action name="incremental_only"><spark>...</spark><ok to="end"/><error to="kill"/></action>
    <kill name="kill"><message>Failed</message></kill>
    <end name="end"/>
</workflow-app>
```

### After (Databricks Job JSON)
```json
{
  "name": "parallel-processing",
  "tasks": [
    {
      "task_key": "validate",
      "notebook_task": {
        "notebook_path": "/Workspace/workflows/parallel/validate"
      }
    },
    {
      "task_key": "process_orders",
      "depends_on": [{"task_key": "validate"}],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{tasks.validate.values.mode}}",
        "right": "full"
      },
      "notebook_task": {"notebook_path": "/Workspace/workflows/parallel/process_orders"}
    },
    {
      "task_key": "process_customers",
      "depends_on": [{"task_key": "validate"}],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{tasks.validate.values.mode}}",
        "right": "full"
      },
      "notebook_task": {"notebook_path": "/Workspace/workflows/parallel/process_customers"}
    },
    {
      "task_key": "process_products",
      "depends_on": [{"task_key": "validate"}],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{tasks.validate.values.mode}}",
        "right": "full"
      },
      "notebook_task": {"notebook_path": "/Workspace/workflows/parallel/process_products"}
    },
    {
      "task_key": "aggregate",
      "depends_on": [
        {"task_key": "process_orders"},
        {"task_key": "process_customers"},
        {"task_key": "process_products"}
      ],
      "sql_task": {
        "file": {"path": "/Workspace/workflows/parallel/aggregate.sql"},
        "warehouse_id": "abc123"
      }
    },
    {
      "task_key": "incremental_only",
      "depends_on": [{"task_key": "validate"}],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "NOT_EQUAL",
        "left": "{{tasks.validate.values.mode}}",
        "right": "full"
      },
      "notebook_task": {"notebook_path": "/Workspace/workflows/parallel/incremental"}
    }
  ]
}
```

## Example 3: Coordinator → Scheduled Job

### Before (Oozie coordinator.xml)
```xml
<coordinator-app name="hourly-ingest"
    frequency="${coord:hours(1)}"
    start="2024-01-01T00:00Z" end="2025-12-31T23:59Z" timezone="America/New_York">
    <action>
        <workflow>
            <app-path>/user/oozie/workflows/ingest</app-path>
            <configuration>
                <property>
                    <name>hour</name>
                    <value>${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH')}</value>
                </property>
            </configuration>
        </workflow>
    </action>
</coordinator-app>
```

### After (Databricks Job)
```json
{
  "name": "hourly-ingest",
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?",
    "timezone_id": "America/New_York"
  },
  "parameters": [
    {"name": "hour", "default": ""}
  ],
  "tasks": [
    {
      "task_key": "ingest",
      "notebook_task": {
        "notebook_path": "/Workspace/workflows/ingest/main",
        "base_parameters": {
          "hour": "{{job.parameters.hour}}"
        }
      }
    }
  ]
}
```

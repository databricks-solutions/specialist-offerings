# DAG Mapping: Oozie → Databricks Workflows

## Basic Linear Workflow

```xml
<!-- Oozie -->
<start to="step1"/>
<action name="step1">...</action>  <!-- ok to="step2" -->
<action name="step2">...</action>  <!-- ok to="step3" -->
<action name="step3">...</action>  <!-- ok to="end" -->
<end name="end"/>
```

```json
// Databricks Job
{
  "tasks": [
    {"task_key": "step1"},
    {"task_key": "step2", "depends_on": [{"task_key": "step1"}]},
    {"task_key": "step3", "depends_on": [{"task_key": "step2"}]}
  ]
}
```

## Fork/Join (Parallel Execution)

```xml
<!-- Oozie: fork splits into parallel paths, join waits for all -->
<start to="step1"/>
<action name="step1">...</action>  <!-- ok to="fork1" -->
<fork name="fork1">
    <path start="parallel_a"/>
    <path start="parallel_b"/>
    <path start="parallel_c"/>
</fork>
<action name="parallel_a">...</action>  <!-- ok to="join1" -->
<action name="parallel_b">...</action>  <!-- ok to="join1" -->
<action name="parallel_c">...</action>  <!-- ok to="join1" -->
<join name="join1" to="step_final"/>
<action name="step_final">...</action>  <!-- ok to="end" -->
```

```json
// Databricks: Use depends_on to model fork/join
{
  "tasks": [
    {"task_key": "step1"},
    {"task_key": "parallel_a", "depends_on": [{"task_key": "step1"}]},
    {"task_key": "parallel_b", "depends_on": [{"task_key": "step1"}]},
    {"task_key": "parallel_c", "depends_on": [{"task_key": "step1"}]},
    {"task_key": "step_final", "depends_on": [
      {"task_key": "parallel_a"},
      {"task_key": "parallel_b"},
      {"task_key": "parallel_c"}
    ]}
  ]
}
```

## Decision Node (Conditional Branching)

```xml
<!-- Oozie: decision evaluates EL expressions -->
<decision name="check_data">
    <switch>
        <case to="full_load">${fs:fileSize('/data/flag/full_refresh') gt 0}</case>
        <case to="incremental">${wf:actionData('check')['status'] eq 'partial'}</case>
        <default to="skip"/>
    </switch>
</decision>
```

```json
// Databricks: Use run_if conditions on tasks
{
  "tasks": [
    {"task_key": "check_data", "spark_python_task": {"python_file": "check_data.py"}},
    {
      "task_key": "full_load",
      "depends_on": [{"task_key": "check_data"}],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{tasks.check_data.values.load_type}}",
        "right": "full"
      }
    },
    {
      "task_key": "incremental",
      "depends_on": [{"task_key": "check_data"}],
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{tasks.check_data.values.load_type}}",
        "right": "incremental"
      }
    }
  ]
}
```

**Note**: Oozie's decision node requires converting EL expressions to Databricks task value conditions. Complex conditions may need a "router" Python task that sets task values.

## Error Handling

```xml
<!-- Oozie -->
<action name="etl_step">
    <spark>...</spark>
    <ok to="next_step"/>
    <error to="error_handler"/>
</action>
<action name="error_handler">
    <email>
        <to>ops@company.com</to>
        <subject>ETL Failed</subject>
    </email>
    <ok to="kill"/>
    <error to="kill"/>
</action>
```

```json
// Databricks: Use email_notifications and on_failure behavior
{
  "tasks": [{
    "task_key": "etl_step",
    "email_notifications": {
      "on_failure": ["ops@company.com"]
    }
  }],
  "email_notifications": {
    "on_failure": ["ops@company.com"]
  }
}
```

## Sub-Workflow

```xml
<!-- Oozie -->
<action name="run_child">
    <sub-workflow>
        <app-path>/user/oozie/workflows/child-wf</app-path>
        <propagate-configuration/>
        <configuration>
            <property><name>date</name><value>${date}</value></property>
        </configuration>
    </sub-workflow>
    <ok to="end"/>
</action>
```

```json
// Databricks: Use run_job_task to call another job
{
  "task_key": "run_child",
  "run_job_task": {
    "job_id": 12345,
    "job_parameters": {"date": "{{job.parameters.date}}"}
  }
}
```

# Oozie Action → Databricks Task Type Mapping

## Action Type Reference

| Oozie Action | Databricks Task Type | Config Key |
|---|---|---|
| `<spark>` | `spark_jar_task` or `spark_python_task` | Depends on JAR vs Python |
| `<hive>` / `<hive2>` | `sql_task` or `notebook_task` | SQL warehouse or notebook |
| `<sqoop>` | `spark_python_task` (JDBC notebook) | Convert to Spark JDBC code |
| `<shell>` | `spark_python_task` or `notebook_task` | Wrap in subprocess or convert |
| `<map-reduce>` | `spark_jar_task` | Convert MR to Spark |
| `<java>` | `spark_jar_task` | Run as Spark job |
| `<pig>` | `spark_python_task` | Rewrite in PySpark |
| `<distcp>` | `notebook_task` (dbutils.fs.cp) | File copy via Volumes |
| `<fs>` (mkdir/delete/chmod) | `notebook_task` (dbutils.fs) | File operations |
| `<email>` | `email_notifications` | Job-level notification |
| `<sub-workflow>` | `run_job_task` | Call another Databricks Job |

## Detailed Mappings

### `<spark>` Action

```xml
<action name="spark_etl">
    <spark xmlns="uri:oozie:spark-action:0.2">
        <master>yarn</master>
        <mode>cluster</mode>
        <name>ETL Job</name>
        <class>com.example.ETLJob</class>
        <jar>lib/etl.jar</jar>
        <spark-opts>--conf spark.sql.shuffle.partitions=200</spark-opts>
        <arg>--input</arg><arg>/data/raw</arg>
        <arg>--output</arg><arg>/data/processed</arg>
    </spark>
</action>
```

```json
{
  "task_key": "spark_etl",
  "spark_jar_task": {
    "main_class_name": "com.example.ETLJob",
    "parameters": ["--input", "/Volumes/main/raw", "--output", "/Volumes/main/processed"]
  },
  "libraries": [{"jar": "dbfs:/libs/etl.jar"}],
  "new_cluster": {
    "spark_conf": {"spark.sql.shuffle.partitions": "200"}
  }
}
```

### `<hive>` / `<hive2>` Action

```xml
<action name="hive_load">
    <hive xmlns="uri:oozie:hive-action:0.5">
        <script>scripts/load_data.hql</script>
        <param>DATE=${date}</param>
    </hive>
</action>
```

```json
// Option 1: SQL Task (preferred for simple queries)
{
  "task_key": "hive_load",
  "sql_task": {
    "file": {
      "path": "/Workspace/workflows/etl/scripts/load_data.sql"
    },
    "warehouse_id": "abc123def456",
    "parameters": {"date": "{{job.parameters.date}}"}
  }
}

// Option 2: Notebook Task (for complex scripts)
{
  "task_key": "hive_load",
  "notebook_task": {
    "notebook_path": "/Workspace/workflows/etl/load_data",
    "base_parameters": {"date": "{{job.parameters.date}}"}
  }
}
```

### `<sqoop>` Action

```xml
<action name="sqoop_import">
    <sqoop xmlns="uri:oozie:sqoop-action:0.4">
        <arg>import</arg>
        <arg>--connect</arg><arg>jdbc:mysql://host/db</arg>
        <arg>--table</arg><arg>customers</arg>
        <arg>--target-dir</arg><arg>/data/raw/customers</arg>
    </sqoop>
</action>
```

```json
// Convert to notebook that runs Spark JDBC
{
  "task_key": "sqoop_import",
  "notebook_task": {
    "notebook_path": "/Workspace/workflows/etl/jdbc_import_customers",
    "base_parameters": {
      "source_table": "customers",
      "target_table": "main.raw.customers"
    }
  }
}
```

### `<shell>` Action

```xml
<action name="run_cleanup">
    <shell xmlns="uri:oozie:shell-action:0.3">
        <exec>cleanup.sh</exec>
        <argument>${date}</argument>
        <file>cleanup.sh#cleanup.sh</file>
    </shell>
</action>
```

```json
// Convert to Python task or notebook
{
  "task_key": "run_cleanup",
  "notebook_task": {
    "notebook_path": "/Workspace/workflows/etl/cleanup",
    "base_parameters": {"date": "{{job.parameters.date}}"}
  }
}
// Note: Shell logic needs manual conversion to Python/notebook
```

### `<fs>` Action

```xml
<action name="prepare_dirs">
    <fs>
        <delete path='/data/staging/output'/>
        <mkdir path='/data/staging/output'/>
    </fs>
</action>
```

```json
// Convert to notebook with dbutils.fs operations
{
  "task_key": "prepare_dirs",
  "notebook_task": {
    "notebook_path": "/Workspace/workflows/etl/prepare_dirs"
  }
}
// Notebook content:
// dbutils.fs.rm("/Volumes/main/staging/output", recurse=True)
// dbutils.fs.mkdirs("/Volumes/main/staging/output")
```

## EL Expression → Databricks Parameter Mapping

| Oozie EL Expression | Databricks Equivalent |
|---|---|
| `${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}` | `{{job.parameters.date}}` (set via schedule) |
| `${wf:id()}` | `{{job.run_id}}` |
| `${wf:appPath()}` | N/A (use workspace paths) |
| `${wf:actionData('task')['key']}` | `{{tasks.task.values.key}}` |
| `${fs:fileSize('/path')}` | Check in notebook with `dbutils.fs.ls()` |

---
name: convert
description: "Convert Hadoop code to Databricks equivalents. Usage: /convert <type> <file-or-code>"
---

# /convert Command

Route to the appropriate conversion skill based on the first argument.

## Usage

```
/convert hive-ddl <file-or-code>     → hive-ddl-to-uc skill
/convert spark <file-or-code>        → spark-to-databricks skill
/convert hive-sql <file-or-code>     → hive-sql-to-spark-sql skill
/convert sqoop <command-or-file>     → sqoop-to-databricks skill
/convert hbase <code-or-file>        → hbase-to-databricks skill
/convert oozie <workflow.xml>        → oozie-to-databricks-workflows skill
/convert ranger <policy-json>       → ranger-to-uc-policies skill
```

## Routing Instructions

When the user invokes `/convert`:

1. Parse the first argument to determine the conversion type
2. If a file path is provided, read the file contents
3. Route to the appropriate skill listed above
4. If the type is ambiguous or not provided, ask the user which conversion they need
5. Always load the `resources/COMMON_PATTERNS.md` for shared migration context

## Examples

```
/convert hive-ddl "CREATE TABLE sales (id INT, amount DOUBLE) STORED AS ORC"
/convert spark ./src/main/scala/com/example/ETLJob.scala
/convert oozie /user/oozie/workflows/daily-etl/workflow.xml
/convert sqoop "sqoop import --connect jdbc:mysql://host/db --table customers --target-dir /data/raw"
/convert ranger ./ranger_policies_export.json
```

---
name: sqoop-to-databricks
description: "Convert Sqoop import/export commands to Databricks. Triggers on: convert Sqoop, migrate Sqoop, Sqoop to Databricks, Sqoop import to JDBC, Sqoop to Lakehouse Federation"
version: 1.0.0
---

# Sqoop to Databricks Converter

Convert Apache Sqoop import/export commands to Databricks equivalents using JDBC, Lakehouse Federation, or Auto Loader.

## When to Use

- Converting `sqoop import` commands to `spark.read.jdbc` or Lakehouse Federation
- Converting `sqoop export` commands to `df.write.jdbc`
- Migrating incremental import patterns (lastmodified/append) to MERGE INTO or Auto Loader
- Converting Sqoop-generated Hive tables to UC managed tables

## Instructions

When given Sqoop commands to convert:

1. **Read references** for detailed rules:
   - `references/JDBC_PATTERNS.md` — Sqoop import/export → Spark JDBC
   - `references/INCREMENTAL_PATTERNS.md` — Incremental patterns → MERGE INTO / Auto Loader
   - `references/EXAMPLES.md` — Full before/after examples

2. **Apply transformations**:
   a. Parse Sqoop command arguments (--connect, --table, --query, --target-dir, etc.)
   b. Determine if import or export
   c. For imports: convert to `spark.read.format("jdbc")` or Lakehouse Federation query
   d. For exports: convert to `df.write.format("jdbc")`
   e. Map Sqoop parallelism (--num-mappers) to Spark partitioning
   f. Convert --split-by to partitionColumn
   g. Handle --hive-import as write to UC table
   h. Convert incremental patterns to Delta MERGE INTO

3. **Recommend** Lakehouse Federation when the pattern is simple table reads

4. **Output** converted code with setup instructions (secret scopes, etc.)

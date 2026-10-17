---
name: hive-sql-to-spark-sql
description: "Convert HiveQL queries to Databricks SQL / Spark SQL. Triggers on: convert HiveQL, migrate Hive query, Hive SQL to Spark SQL, HiveQL to Databricks SQL, convert Hive script"
version: 1.0.0
---

# HiveQL to Spark SQL / Databricks SQL Converter

Convert Hive Query Language (HiveQL) statements to Databricks-compatible SQL.

## When to Use

- Converting `.hql` script files to Databricks SQL
- Migrating HiveQL DML (INSERT, SELECT) to Spark SQL
- Converting Hive-specific syntax (LATERAL VIEW, TRANSFORM, DISTRIBUTE BY)
- Migrating Hive UDFs to Spark/Databricks equivalents
- Converting SET commands and session variables

## Instructions

When given HiveQL to convert:

1. **Read references** for detailed rules:
   - `references/SYNTAX_RULES.md` — HiveQL syntax differences from Spark SQL
   - `references/UDF_MIGRATION.md` — Hive UDF → Spark/Python UDF
   - `references/EXAMPLES.md` — Full before/after examples

2. **Apply transformations**:
   a. Update table references to 3-level namespace (catalog.schema.table)
   b. Convert Hive-specific SQL syntax to Spark SQL equivalents
   c. Replace `SET` variable assignments with Spark SQL equivalents
   d. Convert TRANSFORM/MAP/REDUCE clauses
   e. Update LATERAL VIEW syntax if needed
   f. Replace Hive UDF calls with built-in Spark SQL functions
   g. Convert INSERT OVERWRITE patterns

3. **Output** converted SQL with comments explaining each change

4. **Flag** any Hive UDFs that need custom implementation

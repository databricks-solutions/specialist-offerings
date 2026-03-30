---
name: spark-to-databricks
description: "Convert open-source Spark applications to Databricks. Triggers on: migrate Spark to Databricks, convert spark-submit, Spark job to Databricks, SparkSession migration, HDFS to DBFS"
version: 1.0.0
---

# Spark to Databricks Converter

Convert open-source Apache Spark applications (Scala, Python, Java) to run on Databricks.

## When to Use

- Migrating `spark-submit` scripts to Databricks Jobs
- Converting `SparkSession.builder` initialization code
- Replacing HDFS paths with DBFS/Unity Catalog Volumes paths
- Migrating Hive metastore references to Unity Catalog
- Converting Spark configuration for Databricks runtime

## Instructions

When given Spark code to convert:

1. **Read references** for detailed rules:
   - `references/SESSION_MIGRATION.md` — SparkSession builder changes
   - `references/SUBMIT_TO_JOB.md` — spark-submit → Databricks Jobs
   - `references/PATH_MIGRATION.md` — HDFS → DBFS/UC Volumes
   - `references/EXAMPLES.md` — Full before/after examples

2. **Apply transformations**:
   a. Simplify SparkSession initialization (remove master, appName in notebooks)
   b. Replace HDFS paths with UC Volumes or DBFS paths
   c. Remove/update Hadoop-specific configurations
   d. Replace Hive metastore config with Unity Catalog
   e. Convert spark-submit parameters to Databricks Job config
   f. Update dependency management (Maven coordinates, wheel files)

3. **Output** converted code with comments explaining changes

4. **Provide** Databricks Job JSON config when converting spark-submit scripts

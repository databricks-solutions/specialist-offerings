---
name: spark-to-databricks
description: "Convert open-source Spark applications to Databricks. Triggers on: migrate Spark to Databricks, convert spark-submit, Spark job to Databricks, SparkSession migration, HiveContext, SQLContext, legacy PySpark, CDH PySpark, Python 2 PySpark, HDFS to DBFS"
version: 1.1.0
---

# Spark to Databricks Converter

Convert open-source Apache Spark applications (Scala, Python, Java) to run on Databricks.

## When to Use

- Migrating `spark-submit` scripts to Databricks Jobs
- Converting `SparkSession.builder` initialization code
- **Legacy CDH-era PySpark** with `HiveContext`, `SparkContext`, or Python 2 syntax
- Replacing HDFS paths with DBFS/Unity Catalog Volumes paths
- Migrating Hive metastore references to Unity Catalog
- Converting Spark configuration for Databricks runtime

## Instructions

When given Spark code to convert:

1. **Detect legacy vs modern** — if the code uses `HiveContext`, `SparkContext`, `SparkConf`, Python 2 syntax, or `hdfs://` paths, treat it as **legacy PySpark** and follow `references/PYSPARK_MIGRATION.md` first.

2. **Read references** for detailed rules:
   - `references/PYSPARK_MIGRATION.md` — **legacy CDH PySpark** (`HiveContext`, Python 2, 2-part table names)
   - `references/SESSION_MIGRATION.md` — SparkSession builder changes
   - `references/PATH_MIGRATION.md` — HDFS → DBFS/UC Volumes
   - `references/SUBMIT_TO_JOB.md` — spark-submit → Databricks Jobs
   - `references/EXAMPLES.md` — Full before/after examples (including legacy cluster-setup jobs)
   - `resources/COMMON_PATTERNS.md` — shared path and auth patterns

3. **Apply transformations** (legacy PySpark order):
   a. Fix Python 2 → 3 syntax
   b. Replace `SparkContext`/`HiveContext`/`SQLContext` with `spark` (notebook) or `SparkSession` (job)
   c. Replace `hdfs://` paths with UC Volumes or managed tables
   d. Upgrade 2-part table names (`db.table`) to 3-part UC names (`catalog.schema.table`)
   e. Move `sqlContext.setConf` / `SparkConf` settings to `spark.conf.set` or job cluster config
   f. Remove YARN/Hadoop/Hive metastore configs
   g. Remove `sc.stop()` in notebooks; review `coalesce(1)` on writes
   h. Convert spark-submit parameters to Databricks Job config when applicable

4. **Output** converted code with a header comment listing:
   - Catalog/schema placeholders used (default: `main`)
   - Each category of change applied
   - Any manual review items (e.g. `coalesce(1)`, large path globs)

5. **Provide** Databricks Job JSON config when converting spark-submit scripts or standalone `.py` files meant for scheduling

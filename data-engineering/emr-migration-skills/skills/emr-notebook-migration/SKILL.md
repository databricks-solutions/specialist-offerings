---
name: emr-notebook-migration
description: "Convert EMR Studio/Zeppelin/Jupyter notebooks to Databricks notebooks. Use when: (1) 'convert EMR notebooks', (2) 'Zeppelin to Databricks', (3) 'Jupyter to Databricks notebook', (4) 'EMR Studio migration', (5) 'notebook format conversion'."
---

# EMR Notebook Migration to Databricks

## Overview

EMR supports multiple notebook environments: Apache Zeppelin (legacy), Jupyter (via EMR Notebooks and EMR Studio), and plain PySpark scripts. Databricks has its own native notebook format that supports Python, Scala, SQL, and R with built-in collaboration, versioning, and visualization capabilities.

## Notebook Format Comparison

| Source | Format | Databricks Target |
|---|---|---|
| Zeppelin | .json (zpln) | .py or .sql notebook |
| Jupyter | .ipynb | Import directly or convert to .py |
| EMR Studio | .ipynb | Import directly |
| PySpark scripts | .py | Import as notebook or run as job |

## Migration Approaches

### 1. Jupyter/EMR Studio .ipynb -- Import Directly

Databricks natively supports importing `.ipynb` files. This is the simplest path.

**Steps:**
1. Download `.ipynb` files from EMR Studio (stored in S3)
2. In Databricks workspace: **Import > File > select .ipynb**
3. Databricks converts cells to its native format
4. Review and fix any code incompatibilities (see below)

### 2. Zeppelin .json -- Convert to .py Format

Zeppelin notebooks use a custom JSON format that Databricks cannot import directly. Convert them first.

**Steps:**
1. Export Zeppelin notebook as `.json` (or `.zpln`)
2. Run converter script to produce `.py` with Databricks magic commands
3. Import the `.py` file into Databricks workspace
4. Review and fix code incompatibilities

**Script reference:** `/Users/kishore.mannava/cursorprojects/umlaut-poc-emr-claude/scripts/notebook_converter.py`

### 3. Plain .py Scripts -- Import or Keep as Script

**Option A: Import as notebook**
- Add `# Databricks notebook source` as the first line
- Use `# COMMAND ----------` to separate cells
- Import into Databricks workspace

**Option B: Keep as script for jobs**
- Upload to Databricks Repos or UC Volumes
- Run via Databricks Jobs (spark_python_task)
- No format conversion needed

## Code Changes Needed After Import

### SparkContext / SparkSession

```python
# EMR (remove these -- Databricks pre-initializes spark)
# REMOVE: from pyspark.sql import SparkSession
# REMOVE: spark = SparkSession.builder.appName("my-app").getOrCreate()
# REMOVE: sc = SparkContext()

# Databricks: spark and sc are pre-initialized
df = spark.read.parquet("s3://bucket/path")
```

### Magic Commands

```python
# Zeppelin: %pyspark  -->  Databricks: %python (or just default)
# Zeppelin: %spark.sql -->  Databricks: %sql
# Zeppelin: %sh        -->  Databricks: %sh
# Zeppelin: %md        -->  Databricks: %md
```

### Dynamic Forms / Widgets

```python
# Zeppelin
# name = z.input("name", "default_value")
# choice = z.select("env", [("dev", "Development"), ("prod", "Production")])

# Databricks
dbutils.widgets.text("name", "default_value", "Name")
name = dbutils.widgets.get("name")
dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
choice = dbutils.widgets.get("env")
```

### Display / Visualization

```python
# Zeppelin
# z.show(df)

# Jupyter
# df.toPandas()  # still works
# df.show()      # still works

# Databricks (PREFERRED)
display(df)  # Rich interactive visualization
```

### S3 Path Updates

```python
# EMR (direct S3 with IAM role)
df = spark.read.parquet("s3://my-bucket/data/")

# Databricks (use UC external locations or mount points)
# Option A: Unity Catalog external location (PREFERRED)
df = spark.read.parquet("s3://my-bucket/data/")  # if external location configured

# Option B: UC Volumes
df = spark.read.parquet("/Volumes/catalog/schema/volume/data/")

# Option C: DBFS mount (legacy, not recommended for new work)
# dbutils.fs.mount("s3://my-bucket", "/mnt/my-bucket")
# df = spark.read.parquet("/mnt/my-bucket/data/")
```

## Visualization Migration

Zeppelin has built-in chart types (bar, line, pie, scatter, area) tied to paragraph output. In Databricks:

- Use `display(df)` to get the visualization tab
- Click the chart icon to switch between table, bar, line, pie, scatter, map
- For advanced visualizations, use Databricks SQL dashboards or matplotlib/plotly
- Zeppelin's `%angular` interpreter has no direct equivalent -- use Databricks widgets or dashboards

## Related Skills

- **emr-spark-code-migration**: For converting Spark API calls and library references

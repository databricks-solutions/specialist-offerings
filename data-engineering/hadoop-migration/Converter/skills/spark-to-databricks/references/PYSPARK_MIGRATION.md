# Legacy PySpark Migration: CDH / Spark 1.x → Databricks

Guide for converting **legacy on-prem PySpark** (CDH 5.x, Spark 1.6–2.x, `HiveContext`, Python 2) to Databricks Runtime. Use this when the Analyzer scores a workload as **medium** or when you see the patterns below.

**Reference workloads:** `clickstream_transform.py`, `session_metrics.py` (cluster-setup fixtures).

---

## Detecting legacy PySpark

| Signal | Example |
|--------|---------|
| `HiveContext` / `SQLContext` | `sqlContext = HiveContext(sc)` |
| `SparkContext` + `SparkConf` init | `sc = SparkContext(conf=conf)` |
| Python 2 syntax | `print "msg" % val`, `except E, e:` |
| `hdfs://` paths | `hdfs:///data/raw/clickstream/` |
| 2-part table names | `retail_analytics.enriched_sessions` |
| `sqlContext.setConf` | `sqlContext.setConf("spark.sql.shuffle.partitions", "10")` |
| `sc.stop()` in `finally` | Cluster teardown pattern |

Modern `SparkSession.builder` jobs with UC paths are covered in `SESSION_MIGRATION.md` and `EXAMPLES.md` Example 1.

---

## Migration checklist (apply in order)

1. **Python 2 → 3** — fix syntax before other changes
2. **Replace session init** — `SparkContext`/`HiveContext` → `SparkSession` (or use pre-init `spark` in notebooks)
3. **Rename context variables** — `sqlContext`/`hiveContext` → `spark`
4. **Update paths** — `hdfs://` → UC Volumes or managed tables (see `PATH_MIGRATION.md`)
5. **Update table names** — `db.table` → `catalog.schema.table`
6. **Move configs** — `sqlContext.setConf` / `SparkConf` → `spark.conf.set` or job cluster config
7. **Remove teardown** — drop `sc.stop()` in notebooks; keep `spark.stop()` only in standalone JAR/wheel jobs if needed
8. **Review anti-patterns** — `coalesce(1)`, `%` string formatting in prints
9. **Package as job** — notebook cell or `spark_python_task` (see `SUBMIT_TO_JOB.md`)

---

## 1. Session initialization

### Before (CDH / Spark 1.6)

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("ClickstreamTransform")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
sqlContext.setConf("spark.sql.shuffle.partitions", "10")
```

### After (Databricks notebook)

```python
# spark is pre-initialized — no imports or builder needed
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

### After (Databricks Python job / wheel)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ClickstreamTransform").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "10")
```

**Rules:**
- Remove `SparkContext`, `SparkConf`, `HiveContext`, `SQLContext` imports
- Remove `.master("yarn")`, `.enableHiveSupport()` — UC is default on Databricks
- Replace every `sqlContext.` / `hiveContext.` call with `spark.`

---

## 2. Python 2 → 3

| Python 2 | Python 3 |
|----------|----------|
| `print "Rows: %d" % cnt` | `print(f"Rows: {cnt}")` |
| `except Exception, e:` | `except Exception as e:` |
| `unicode`, `xrange` | `str`, `range` |
| `#!/usr/bin/env python` | `#!/usr/bin/env python3` (optional) |

Databricks Runtime uses Python 3. Fix syntax errors first — the job will not run until Python 3 is valid.

---

## 3. HDFS paths

```python
# Before
raw = sqlContext.read.json("hdfs:///data/raw/clickstream/dt=*/hour=*/")

# After (UC Volumes — adjust catalog/schema to engagement defaults)
raw = spark.read.json("/Volumes/main/raw/clickstream/dt=*/hour=*/")

# After (managed table — preferred when data is already in Hive/UC)
raw = spark.table("main.raw.clickstream")
```

Glob patterns (`dt=*/hour=*`) work on cloud storage paths and UC Volumes the same as HDFS.

See `PATH_MIGRATION.md` for the full mapping table.

---

## 4. Hive table names → Unity Catalog

CDH Hive uses **2-part** names (`database.table`). Unity Catalog requires **3-part** names (`catalog.schema.table`).

```python
# Before
.saveAsTable("retail_analytics.enriched_sessions")
sqlContext.sql("SELECT * FROM retail_analytics.enriched_sessions")

# After (confirm catalog with customer; default placeholder: main)
.write.mode("overwrite").saveAsTable("main.retail_analytics.enriched_sessions")
spark.sql("SELECT * FROM main.retail_analytics.enriched_sessions")
```

**Catalog placeholder:** Use `main` unless the engagement specifies otherwise. Document the chosen catalog in converted output comments.

**Schema creation:** Ensure the schema exists before `saveAsTable`:

```sql
CREATE SCHEMA IF NOT EXISTS main.retail_analytics;
```

---

## 5. DataFrame API calls (mostly unchanged)

Legacy jobs often use the DataFrame API via `sqlContext` — the transformation logic usually carries over unchanged once `sqlContext` → `spark`:

```python
# Before
transformed = raw_clickstream \
    .withColumn("event_hour", F.hour(F.col("event_timestamp"))) \
    .withColumn("page_category", F.expr("CASE WHEN ... END"))

# After — identical except spark variable name
transformed = raw_clickstream \
    .withColumn("event_hour", F.hour(F.col("event_timestamp"))) \
    .withColumn("page_category", F.expr("CASE WHEN ... END"))
```

`F.expr()`, window functions, `groupBy`/`agg`, and `saveAsTable` work the same on Databricks Runtime.

---

## 6. SQL reads

```python
# Before
enriched = sqlContext.sql("SELECT * FROM retail_analytics.enriched_sessions")

# After
enriched = spark.sql("SELECT * FROM main.retail_analytics.enriched_sessions")

# Or prefer table API
enriched = spark.table("main.retail_analytics.enriched_sessions")
```

---

## 7. Writes and `coalesce(1)`

```python
# Before
daily_agg.coalesce(1).write.mode("overwrite").format("parquet") \
    .saveAsTable("retail_analytics.daily_session_aggregates")

# After
daily_agg.write.mode("overwrite").saveAsTable("main.retail_analytics.daily_session_aggregates")
```

**`coalesce(1)` on write:** Common on Hadoop to produce a single output file. On Databricks:
- Remove for managed tables (Delta/Parquet handles file sizing)
- If a single file is required for downstream, use `coalesce(1)` only on small outputs or use `COPY INTO` / export patterns
- Flag for review — can cause driver OOM on large datasets

---

## 8. Error handling and teardown

```python
# Before
try:
    ...
except Exception as e:
    print("Job failed: %s" % str(e))
    raise
finally:
    sc.stop()

# After (notebook)
try:
    ...
except Exception as e:
    print(f"Job failed: {e}")
    raise
# No sc.stop() — cluster lifecycle is managed by Databricks

# After (Python job task)
try:
    ...
except Exception as e:
    print(f"Job failed: {e}")
    raise
finally:
    spark.stop()  # optional for spark_python_task
```

---

## 9. Notebook vs Job packaging

| Pattern | When to use | Notes |
|---------|-------------|-------|
| **Notebook** | Interactive migration, demos, medium complexity | `%python` cells; `spark` pre-init; good for `clickstream_transform` |
| **`spark_python_task`** | Scheduled production jobs | Upload `.py` to workspace or DBFS; see `SUBMIT_TO_JOB.md` |
| **Wheel package** | Multi-file apps with shared modules | Build wheel, attach as library on job |

For Oozie-chained jobs (`clickstream_transform` → `session_metrics`), convert each script first, then build a Databricks Workflow with task dependencies (see `oozie-to-databricks-workflows` skill).

---

## 10. Configurations to remove

Do not port these from `SparkConf` or `sqlContext.setConf`:

| Config | Reason |
|--------|--------|
| `spark.master` | Databricks manages cluster |
| `spark.yarn.*` | No YARN |
| `hive.metastore.uris` | Unity Catalog |
| `spark.sql.warehouse.dir` | UC managed |
| `spark.hadoop.fs.defaultFS` | Use cloud/UC paths |

Keep tuning configs that affect query performance:

```python
spark.conf.set("spark.sql.shuffle.partitions", "10")  # OK to keep
spark.conf.set("spark.sql.adaptive.enabled", "true")   # recommended on DBR
```

---

## Complexity tier guidance

Aligned with Analyzer `pyspark.yaml` scoring:

| Tier | Approach |
|------|----------|
| **easy** | Apply checklist above; deploy as notebook or job |
| **medium** | Full checklist + path/table renames + Python 3; typical for cluster-setup jobs |
| **hard** | RDD API present — rewrite to DataFrame before or during conversion |
| **very_hard** | DStreams, MLlib RDD, custom InputFormat — design doc required |

When the inventory `complexity_recommended_actions` field references this doc, follow the checklist and note any signals (e.g. `legacy_hive_context`, `hdfs_paths`) in conversion comments.

---

## Default naming convention (placeholder)

Use until customer confirms:

| Hadoop | Unity Catalog |
|--------|---------------|
| `retail_analytics` (database) | `main.retail_analytics` (catalog.schema) |
| `hdfs:///data/raw/` | `/Volumes/main/raw/` |
| `hdfs:///data/processed/` | `main.processed.*` tables or `/Volumes/main/processed/` |

Document overrides in the converted file header comment.

---
name: scala-to-pyspark
description: "Convert Scala Spark code to PySpark. Use when: (1) 'convert Scala to PySpark', (2) 'rewrite Scala Spark in Python', (3) 'migrate Scala job to PySpark', (4) 'Scala Spark to Python', (5) translating case classes, implicits, RDD operations, or UDFs from Scala to PySpark equivalents."
---

# Scala Spark to PySpark Migration

## Overview

This skill converts Scala Spark applications to PySpark. Common motivations:
- Consolidating on Python for a unified data engineering stack
- Leveraging Python ML/AI ecosystem (pandas, scikit-learn, MLflow)
- Reducing hiring friction (Python developers are more available)
- Moving to Databricks where PySpark notebooks are the dominant paradigm

**Target runtime:** Databricks Runtime 16.x (Spark 4.0, Delta 4.0, Python 3.12). All examples in this skill use current DBR 16.x APIs.

## Critical Rules

1. **Preserve logic, not syntax** — don't transliterate line-by-line; use idiomatic Python/PySpark patterns
2. **Prefer DataFrame API over RDD** — if the Scala code uses RDD `.map`/`.flatMap`, convert to DataFrame operations where possible (better Catalyst optimization)
3. **Use StructType for schemas** — Scala `case class` encoder-based schemas become explicit `StructType` definitions
4. **Replace implicits with explicit calls** — Python has no implicit conversions; make everything explicit
5. **Test with identical data** — run both versions against the same input and diff the output before decommissioning Scala jobs
6. **Keep UDF usage minimal** — Scala UDFs that were performant may be slow as Python UDFs; prefer built-in functions or pandas UDFs
7. **Use Python scalar UDFs where possible** — on DBR 16.x / Spark 4.0, Python scalar UDFs run natively in the JVM via Spark Connect, closing the performance gap with Scala UDFs
8. **Leverage VARIANT type** — Spark 4.0 adds native `VARIANT` type for semi-structured data; prefer it over JSON string columns with `from_json`

## Type System Mapping

| Scala Type | PySpark Type | Notes |
|---|---|---|
| `String` | `StringType()` | |
| `Int` | `IntegerType()` | |
| `Long` | `LongType()` | |
| `Double` | `DoubleType()` | |
| `Float` | `FloatType()` | |
| `Boolean` | `BooleanType()` | |
| `java.sql.Timestamp` | `TimestampType()` | |
| `java.sql.Date` | `DateType()` | |
| `Array[T]` | `ArrayType(T())` | |
| `Map[K,V]` | `MapType(K(), V())` | |
| `Option[T]` | nullable field | PySpark columns are nullable by default |
| `Seq[Row]` | `ArrayType(StructType(...))` | Nested structs |
| `BigDecimal` | `DecimalType(precision, scale)` | |
| `VariantVal` (Spark 4.0) | `VariantType()` | Native semi-structured — replaces JSON string columns |
| `YearMonthIntervalType` | `YearMonthIntervalType()` | Spark 3.2+; fully supported on DBR 16.x |
| `DayTimeIntervalType` | `DayTimeIntervalType()` | Spark 3.2+; fully supported on DBR 16.x |

## Databricks-Specific Session and Namespace

### DatabricksSession (Recommended for Databricks Development)

**Scala (SparkSession):**
```scala
val spark = SparkSession.builder()
  .appName("MyApp")
  .getOrCreate()

val df = spark.read.table("my_table")
```

**PySpark on Databricks:**
```python
# In notebooks: `spark` is pre-configured — no setup needed.

# For local development, use DatabricksSession (thin client via Spark Connect):
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("DEFAULT").getOrCreate()

# Always use Unity Catalog 3-level namespace
df = spark.read.table("catalog.schema.table_name")
```

### Unity Catalog 3-Level Namespace

All table references should use the full `catalog.schema.table` path:
```python
# ✅ Correct — fully qualified
df = spark.read.table("main.sales.orders")
spark.sql("SELECT * FROM main.sales.orders")

# ❌ Avoid — ambiguous without USE CATALOG/SCHEMA
df = spark.read.table("orders")
```

## Modern PySpark Patterns (Spark 3.4+ / 4.0)

### withColumns / withColumnsRenamed (Spark 3.4+)

Replace chained `withColumn` / `withColumnRenamed` calls — **better performance and readability:**

**Old pattern (avoid):**
```python
df = (df
    .withColumn("name", upper(col("name")))
    .withColumn("email", lower(col("email")))
    .withColumn("age", col("age").cast("int"))
)
```

**Modern pattern:**
```python
df = df.withColumns({
    "name": upper(col("name")),
    "email": lower(col("email")),
    "age": col("age").cast("int"),
})

# Rename multiple columns at once
df = df.withColumnsRenamed({"customerId": "customer_id", "orderId": "order_id"})
```

### Higher-Order Functions for Complex Types

Replace Scala `.map`/`.filter` on array columns with built-in higher-order functions:

```python
from pyspark.sql.functions import transform, filter, aggregate, exists, forall, col

# transform: apply function to each array element
df.select(transform("scores", lambda x: x * 100).alias("pct_scores"))

# filter: keep elements matching predicate
df.select(filter("tags", lambda x: x != "deprecated").alias("active_tags"))

# aggregate: fold/reduce an array
df.select(aggregate("amounts", lit(0.0), lambda acc, x: acc + x).alias("total"))

# exists / forall: boolean checks on arrays
df.select(exists("scores", lambda x: x > 90).alias("has_high_score"))
df.select(forall("scores", lambda x: x >= 0).alias("all_non_negative"))
```

### Pipe Operator in SQL (Spark 4.0)

Spark 4.0 supports the SQL pipe operator (`|>`) for readable left-to-right query composition:

```python
spark.sql("""
    FROM main.sales.orders
    |> WHERE status = 'COMPLETED'
    |> AGGREGATE SUM(amount) AS total_revenue GROUP BY region
    |> ORDER BY total_revenue DESC
    |> LIMIT 10
""")
```

## Core Conversions

### Case Class to StructType

**Scala:**
```scala
case class Customer(id: Long, name: String, email: String, active: Boolean)

val ds = spark.read.json("path").as[Customer]
ds.filter(_.active).map(c => (c.id, c.name.toUpperCase))
```

**PySpark:**
```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType
from pyspark.sql.functions import col, upper

customer_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("active", BooleanType(), nullable=True),
])

df = spark.read.schema(customer_schema).json("path")
df.filter(col("active")).select("id", upper("name").alias("name"))
```

### Dataset Typed Operations to DataFrame API

**Scala:**
```scala
case class Order(orderId: Long, customerId: Long, amount: Double, status: String)

val orders = spark.read.parquet("orders").as[Order]
val totals = orders
  .filter(_.status == "COMPLETED")
  .groupByKey(_.customerId)
  .mapGroups { (custId, iter) =>
    (custId, iter.map(_.amount).sum)
  }
  .toDF("customer_id", "total_amount")
```

**PySpark:**
```python
from pyspark.sql.functions import col, sum as spark_sum

orders = spark.read.parquet("orders")
totals = (
    orders
    .filter(col("status") == "COMPLETED")
    .groupBy("customerId")
    .agg(spark_sum("amount").alias("total_amount"))
    .select(col("customerId").alias("customer_id"), "total_amount")
)
```

### RDD Operations to DataFrame

**Scala:**
```scala
val rdd = sc.textFile("data.csv")
val parsed = rdd
  .map(_.split(","))
  .filter(_.length >= 3)
  .map(a => (a(0), a(1).toDouble, a(2)))

val df = parsed.toDF("name", "value", "category")
```

**PySpark (prefer DataFrame API):**
```python
df = (
    spark.read
    .option("header", "false")
    .csv("data.csv")
    .toDF("name", "value", "category")
    .withColumn("value", col("value").cast("double"))
)
```

If the RDD logic is too complex for DataFrame API:
```python
from pyspark.sql import Row

rdd = sc.textFile("data.csv")
parsed = (
    rdd
    .map(lambda line: line.split(","))
    .filter(lambda a: len(a) >= 3)
    .map(lambda a: Row(name=a[0], value=float(a[1]), category=a[2]))
)
df = spark.createDataFrame(parsed)
```

### Implicit Conversions

**Scala (implicits):**
```scala
import spark.implicits._

val ds = Seq(("Alice", 30), ("Bob", 25)).toDS()
val df = Seq(("Alice", 30), ("Bob", 25)).toDF("name", "age")

// Implicit column reference
ds.filter($"age" > 25)
ds.select('name)   // Symbol syntax
```

**PySpark (explicit):**
```python
from pyspark.sql.functions import col

data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])

df.filter(col("age") > 25)
df.select("name")
```

### UDF Conversion

**Scala UDF:**
```scala
import org.apache.spark.sql.functions.udf

val normalizePhone = udf((phone: String) => {
  Option(phone).map(_.replaceAll("[^0-9]", "")).filter(_.length >= 10).getOrElse(null)
})

df.withColumn("clean_phone", normalizePhone($"phone"))
```

**PySpark UDF (basic):**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

@udf(returnType=StringType())
def normalize_phone(phone):
    if phone is None:
        return None
    cleaned = re.sub(r"[^0-9]", "", phone)
    return cleaned if len(cleaned) >= 10 else None

df.withColumn("clean_phone", normalize_phone("phone"))
```

**PySpark pandas UDF (better performance — vectorized):**
```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(StringType())
def normalize_phone(phones: pd.Series) -> pd.Series:
    cleaned = phones.str.replace(r"[^0-9]", "", regex=True)
    return cleaned.where(cleaned.str.len() >= 10)

df.withColumn("clean_phone", normalize_phone("phone"))
```

**mapInPandas / applyInPandas (Grouped Map — best for complex row-wise or grouped transforms):**
```python
import pandas as pd

# mapInPandas: partition-level pandas processing (replaces complex RDD .mapPartitions)
def normalize_partition(iterator):
    for pdf in iterator:
        pdf["clean_phone"] = pdf["phone"].str.replace(r"[^0-9]", "", regex=True)
        yield pdf

df.mapInPandas(normalize_partition, schema=output_schema)

# applyInPandas: grouped pandas processing (replaces Scala groupByKey + mapGroups)
def compute_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "department": pdf["department"].iloc[0],
        "avg_salary": pdf["salary"].mean(),
        "headcount": len(pdf),
    }])

df.groupBy("department").applyInPandas(compute_stats, schema=stats_schema)
```

**Spark 4.0 / DBR 16.x:** Python scalar UDFs now run with significantly reduced serialization overhead via Arrow-based data exchange. Pandas UDFs remain preferred for batch operations, but the gap with scalar UDFs has narrowed. Always benchmark both on your workload.

### Pattern Matching to Python Conditionals

**Scala:**
```scala
import org.apache.spark.sql.functions._

val categorized = df.withColumn("tier", col("amount") match {
  // Can't pattern-match on columns — typically done with when/otherwise:
})

// Actual Spark pattern:
val categorized = df.withColumn("tier",
  when(col("amount") > 1000, "premium")
    .when(col("amount") > 100, "standard")
    .otherwise("basic")
)
```

**PySpark:**
```python
from pyspark.sql.functions import when, col

categorized = df.withColumn(
    "tier",
    when(col("amount") > 1000, "premium")
    .when(col("amount") > 100, "standard")
    .otherwise("basic")
)
```

### Broadcast Variables

**Scala:**
```scala
val lookup = Map("US" -> "United States", "UK" -> "United Kingdom")
val broadcastLookup = sc.broadcast(lookup)

val resolved = df.map(row => {
  val country = broadcastLookup.value.getOrElse(row.getString(0), "Unknown")
  (row.getString(0), country)
})
```

**PySpark:**
```python
from pyspark.sql.functions import broadcast, col

# Option 1: Broadcast join (preferred)
lookup_df = spark.createDataFrame(
    [("US", "United States"), ("UK", "United Kingdom")],
    ["code", "country_name"]
)
resolved = df.join(broadcast(lookup_df), df.code == lookup_df.code, "left")

# Option 2: Broadcast variable (for UDFs)
lookup = {"US": "United States", "UK": "United Kingdom"}
broadcast_lookup = sc.broadcast(lookup)

@udf(StringType())
def resolve_country(code):
    return broadcast_lookup.value.get(code, "Unknown")
```

### Accumulators

**Scala:**
```scala
val errorCount = sc.longAccumulator("errors")

df.foreach(row => {
  if (row.isNullAt(0)) errorCount.add(1)
})

println(s"Errors: ${errorCount.value}")
```

**PySpark:**
```python
error_count = sc.accumulator(0)

def count_errors(row):
    if row["col0"] is None:
        error_count.add(1)

df.foreach(count_errors)
print(f"Errors: {error_count.value}")
```

## Build System Migration

| Scala (SBT/Maven) | Python |
|---|---|
| `build.sbt` / `pom.xml` | `pyproject.toml` or `requirements.txt` |
| `libraryDependencies` | `pip install` / `%pip install` in notebooks |
| `sbt assembly` (fat JAR) | Wheel package or script upload |
| `spark-submit --class Main app.jar` | `spark-submit app.py` or Databricks job |
| `src/main/scala/` | `src/` or flat `.py` files |
| `src/test/scala/` | `tests/` with pytest |
| ScalaTest / specs2 | pytest + chispa (DataFrame testing) |
| Scala 2.12/2.13 | Python 3.12 (DBR 16.x) | |
| Spark Connect (Scala client) | Databricks Connect v2 (`databricks-connect==16.*`) | Thin client for remote development |

### SBT Dependencies to Python

**build.sbt (Scala original):**
```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided",
  "io.delta" %% "delta-spark" % "4.0.0",
  "com.typesafe" % "config" % "1.4.3",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test"
)
```

**requirements.txt (PySpark equivalent):**
```
databricks-connect==16.1    # preferred over raw pyspark for Databricks dev
delta-spark==4.0.0
python-decouple==3.8        # config replacement
pytest==8.3.0
chispa==0.10.1              # DataFrame assertions
```

> **Note:** On Databricks clusters (DBR 16.x), PySpark 4.0 and Delta 4.0 are pre-installed. Use `databricks-connect` for local development instead of standalone `pyspark`.

## Common Pitfalls

| Issue | Explanation | Fix |
|---|---|---|
| **Null handling** | Scala `Option` prevents NPE at compile time; Python has no equivalent | Use `col.isNull()` / `col.isNotNull()` checks; use `coalesce()` for defaults |
| **Type safety loss** | Scala Dataset API catches type errors at compile time; PySpark DataFrames don't | Add schema validation early; use `df.schema` assertions in tests |
| **UDF performance** | Scala UDFs run in JVM; Python UDFs serialize to Python worker | Use pandas UDFs (`@pandas_udf`) or built-in functions; on DBR 16.x/Spark 4.0 scalar UDF overhead is reduced via Arrow |
| **Collection operations** | Scala `.map`/`.flatMap`/`.collect` on local collections don't translate 1:1 | Use list comprehensions and standard Python idioms |
| **Tuple access** | Scala `._1`, `._2` on tuples | Python `[0]`, `[1]` or named tuples / dataclasses |
| **String interpolation** | Scala `s"Hello $name"` | Python `f"Hello {name}"` |
| **Companion objects** | Scala `object Foo` for singletons/utilities | Python module-level functions or `@staticmethod` |
| **Trait mixins** | Scala `trait` + `with` | Python abstract base classes or mixins |

## Spark SQL Functions: Identical API

These `org.apache.spark.sql.functions` calls are **identical** in PySpark — just change the import:

```python
# Scala: import org.apache.spark.sql.functions._
# PySpark:
from pyspark.sql.functions import (
    col, lit, when, coalesce,                    # core
    sum, avg, count, min, max, countDistinct,    # aggregation
    year, month, dayofmonth, date_format,        # datetime
    split, trim, lower, upper, regexp_replace,   # string
    explode, array, struct, map_keys,            # complex types
    window, lag, lead, row_number, rank,         # window
    broadcast,                                    # optimization
)
```

The function names and signatures are the same. The main difference is column references: Scala uses `$"col"` or `'col`, PySpark uses `col("col")` or `"col"`.

## Testing Strategy

### chispa for DataFrame Assertions

```python
# pip install chispa
from chispa.dataframe_comparer import assert_df_equality

def test_transformation():
    input_df = spark.createDataFrame([(1, "alice"), (2, "bob")], ["id", "name"])
    expected = spark.createDataFrame([(1, "ALICE"), (2, "BOB")], ["id", "name"])
    result = transform(input_df)
    assert_df_equality(result, expected, ignore_row_order=True)
```

### Dual-Run Validation

Run both Scala and PySpark versions against the same input, then compare:

```python
scala_output = spark.read.parquet("s3://bucket/output/scala/")
pyspark_output = spark.read.parquet("s3://bucket/output/pyspark/")

# Row count
assert scala_output.count() == pyspark_output.count()

# Schema match
assert scala_output.schema == pyspark_output.schema

# Content match (for deterministic jobs)
diff = scala_output.exceptAll(pyspark_output)
assert diff.count() == 0, f"Found {diff.count()} mismatched rows"
```

## Spark 4.0 / DBR 16.x Features to Leverage

When migrating Scala to PySpark, take advantage of new capabilities rather than porting old patterns:

| Feature | What Changed | Migration Impact |
|---|---|---|
| **VARIANT type** | Native semi-structured data type | Replace `from_json`/`to_json` string column patterns with `parse_json()` and VARIANT columns |
| **IDENTIFIER clause** | Parameterized SQL column/table names | Replace string interpolation in SQL with safe `IDENTIFIER(:param)` |
| **Collation support** | Per-column collation for string comparison | Replace custom `lower()`-based comparisons with collated columns |
| **Python UDF improvements** | Arrow-optimized serialization | Scalar UDFs are faster; still prefer built-in functions but gap is smaller |
| **DEFAULT column values** | DDL-level defaults on Delta tables | Replace application-level `coalesce(col, lit(default))` with table DDL defaults |
| **Structured Streaming async progress tracking** | Non-blocking progress | Replace `query.lastProgress` polling with async `StreamingQueryListener` |
| **ANSI mode default** | ANSI SQL compliance is default in Spark 4.0 | Overflow/cast errors throw instead of returning null — add explicit `try_*` functions where needed |
| **Spark Connect** | Thin client architecture | Use `databricks-connect` for local PySpark dev without a full local Spark install |
| **Pipe operator (`\|>`)** | SQL pipe syntax for readable queries | Compose queries left-to-right instead of nested subqueries |
| **withColumns / withColumnsRenamed** | Batch column ops (Spark 3.4+) | Replace chained `.withColumn()` calls with single dict-based call |
| **Liquid clustering** | Replaces Z-ORDER on Delta | Use `CLUSTER BY` in DDL instead of `OPTIMIZE ... ZORDER BY`; auto-managed |
| **Auto Loader (cloudFiles)** | Incremental file ingestion | Replace custom file listing/tracking with `spark.readStream.format("cloudFiles")` |
| **Delta UniForm** | Read Delta tables as Iceberg/Hudi | Enable with `delta.universalFormat.enabledFormats` table property |
| **Type widening** | Widen column types without rewrite | Enable with `delta.enableTypeWidening` — avoids full table rewrites for schema evolution |
| **Deletion vectors** | Soft deletes for faster DML | Enabled by default on DBR 16.x — no code changes needed, improves DELETE/MERGE perf |

### VARIANT Type Example

**Scala (old pattern — JSON string column):**
```scala
import org.apache.spark.sql.functions._

val parsed = df.withColumn("event", from_json($"payload", eventSchema))
  .select($"event.event_type", $"event.user_id", $"event.timestamp")
```

**PySpark (Spark 4.0 — VARIANT):**
```python
from pyspark.sql.functions import col, parse_json, variant_get

# Store as VARIANT — no schema needed at write time
df.withColumn("event", parse_json("payload")).write.format("delta").save("events")

# Query with schema-on-read — two approaches:
# 1. SQL-style dot-notation (in selectExpr / spark.sql)
spark.read.format("delta").load("events") \
    .selectExpr(
        "event:event_type::STRING as event_type",
        "event:user_id::LONG as user_id",
        "event:timestamp::TIMESTAMP as event_ts"
    )

# 2. DataFrame API with variant_get (JSONPath syntax)
spark.read.format("delta").load("events") \
    .select(
        variant_get("event", "$.event_type", "string").alias("event_type"),
        variant_get("event", "$.user_id", "long").alias("user_id"),
    )
```

### ANSI Mode Handling

Spark 4.0 enables ANSI mode by default. Scala code that relied on silent null returns on overflow/cast failure will now throw errors:

```python
# Old behavior (Spark 3.x): returns null on overflow
# New behavior (Spark 4.0): throws ArithmeticException

# Fix: use try_* functions for safe casts
from pyspark.sql.functions import try_cast, try_to_number, try_divide

df.select(
    try_cast(col("str_col"), "integer").alias("safe_int"),
    try_divide(col("a"), col("b")).alias("safe_div"),
)
```

### Liquid Clustering (Replaces Z-ORDER)

**Scala (old pattern — manual Z-ORDER):**
```scala
spark.sql("OPTIMIZE my_table ZORDER BY (customer_id, date)")
```

**PySpark (DBR 16.x — Liquid Clustering, auto-managed):**
```python
# At table creation — define clustering columns
spark.sql("""
    CREATE TABLE main.sales.orders (
        order_id BIGINT, customer_id BIGINT, order_date DATE, amount DOUBLE
    )
    USING DELTA
    CLUSTER BY (customer_id, order_date)
""")

# Liquid clustering is auto-managed — no manual OPTIMIZE ZORDER needed.
# Just run OPTIMIZE and clustering is applied automatically:
spark.sql("OPTIMIZE main.sales.orders")

# Alter clustering columns without rewrite:
spark.sql("ALTER TABLE main.sales.orders CLUSTER BY (region, order_date)")
```

### Auto Loader for File Ingestion

Replace custom file-listing logic with Databricks Auto Loader:

**Scala (old pattern — manual file tracking):**
```scala
val newFiles = getNewFiles(checkpointDir)  // custom tracking logic
spark.read.parquet(newFiles: _*)
```

**PySpark (Auto Loader — incremental, exactly-once):**
```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/orders")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/main/raw_data/landing/orders/")
)

# Write to Delta with auto-managed checkpoint
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/orders")
    .trigger(availableNow=True)
    .toTable("main.bronze.orders")
)
```

### Delta MERGE (Upsert) Pattern

**Scala:**
```scala
import io.delta.tables.DeltaTable

val target = DeltaTable.forName(spark, "main.sales.customers")
target.as("t")
  .merge(updates.as("s"), "t.id = s.id")
  .whenMatched.updateAll()
  .whenNotMatched.insertAll()
  .execute()
```

**PySpark (identical API):**
```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "main.sales.customers")
(target.alias("t")
    .merge(updates.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

### foreachBatch Streaming Pattern

Replace Scala `foreachBatch` with the PySpark equivalent for custom sink logic:

```python
def upsert_to_delta(batch_df, batch_id):
    target = DeltaTable.forName(spark, "main.silver.events")
    (target.alias("t")
        .merge(batch_df.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/events")
    .load("/volumes/main/raw/events/")
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/events_upsert")
    .trigger(availableNow=True)
    .start()
)
```

### Volumes for File Access (Unity Catalog)

Replace S3/ADLS direct paths with Unity Catalog Volumes:

```python
# ✅ Recommended — Unity Catalog managed path
df = spark.read.parquet("/Volumes/main/raw_data/landing/orders/")

# ❌ Avoid — direct cloud storage path (no governance)
df = spark.read.parquet("s3://my-bucket/raw/orders/")
```

## Related Skills

- `emr-spark-code-migration` — EMR-specific runtime/API changes for Databricks
- `emr-steps-to-workflows` — convert job submission from EMR Steps to Databricks Jobs
- `emr-config-migration` — translate Spark/YARN configuration
- `emr-migration-validation` — end-to-end data validation after migration

# Advanced Scala to PySpark Patterns

## Custom Spark Listeners

**Scala:**
```scala
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

class MetricsListener extends SparkListener {
  var totalRecords: Long = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    totalRecords += taskEnd.taskMetrics.outputMetrics.recordsWritten
  }
}

val listener = new MetricsListener()
spark.sparkContext.addSparkListener(listener)
```

**PySpark — use StreamingQueryListener or Databricks system tables:**
```python
# Option 1: StreamingQueryListener (Spark 4.0 — fully supported in PySpark)
from pyspark.sql.streaming.listener import StreamingQueryListener

class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: {event.id}")

    def onQueryProgress(self, event):
        print(f"Rows processed: {event.progress.numInputRows}")

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")

spark.streams.addListener(MetricsListener())

# Option 2: Databricks system tables (DBR 16.x)
spark.sql("""
    SELECT * FROM system.compute.node_timeline
    WHERE cluster_id = 'your-cluster-id'
    ORDER BY start_time DESC
""")

# Option 3: Query Spark UI metrics via REST API
import requests
metrics = requests.get(
    f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}/stages"
).json()
```

## Encoders and Serialization

**Scala (Encoders):**
```scala
import org.apache.spark.sql.Encoders

implicit val enc: Encoder[Customer] = Encoders.product[Customer]
val ds: Dataset[Customer] = spark.read.parquet("path").as[Customer]

// Kryo serialization for complex types
implicit val kryoEnc: Encoder[MyComplexType] = Encoders.kryo[MyComplexType]
```

**PySpark — no Encoder concept.** DataFrames are untyped:
```python
# Just read as DataFrame with schema enforcement
from pyspark.sql.types import StructType, StructField, LongType, StringType

schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])
df = spark.read.schema(schema).parquet("path")

# For complex serialization, use Python pickle (via RDD) — avoid if possible
```

## Scala Generics / Type Parameters

**Scala:**
```scala
def processData[T: Encoder](ds: Dataset[T], transformer: T => T): Dataset[T] = {
  ds.map(transformer)
}
```

**PySpark — use DataFrame API with column expressions:**
```python
from pyspark.sql import DataFrame
from typing import Callable
from pyspark.sql.functions import col

def process_data(df: DataFrame, transformer: Callable[[DataFrame], DataFrame]) -> DataFrame:
    return transformer(df)

# Usage
result = process_data(df, lambda d: d.withColumn("name", upper(col("name"))))
```

## Spark Streaming Conversion

**Scala Structured Streaming:**
```scala
import org.apache.spark.sql.streaming.Trigger

val stream = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("subscribe", "events")
  .load()

val parsed = stream
  .selectExpr("CAST(value AS STRING)")
  .as[String]
  .map(json => parseEvent(json))  // typed Dataset map

val query = parsed.writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints/events")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .outputMode("append")
  .start("/delta/events")
```

**PySpark Structured Streaming:**
```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("payload", StringType()),
])

stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "events")
    .load()
)

parsed = (
    stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json("json_str", event_schema).alias("event"))
    .select("event.*")
)

query = (
    parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events")
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start("/delta/events")
)
```

Key differences:
- No `.as[T]` typed streaming — use `from_json` with explicit schema, or use `parse_json()` with VARIANT on Spark 4.0
- `.map` with typed parsing → `from_json` + `select("event.*")`
- `Trigger.ProcessingTime("30 seconds")` → `trigger(processingTime="30 seconds")`
- `Trigger.AvailableNow()` → `trigger(availableNow=True)`

**Spark 4.0 / DBR 16.x streaming improvement — VARIANT ingestion:**
```python
# Instead of requiring a schema for from_json, use VARIANT:
parsed = (
    stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .selectExpr("parse_json(json_str) as event")
    .selectExpr(
        "event:event_id::STRING as event_id",
        "event:timestamp::LONG as event_ts",
        "event:payload::STRING as payload"
    )
)
```

## Window Functions

**Scala:**
```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec = Window
  .partitionBy("department")
  .orderBy($"salary".desc)

val ranked = df.withColumn("rank", dense_rank().over(windowSpec))
  .withColumn("running_total", sum("salary").over(
    Window.partitionBy("department").orderBy("hire_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  ))
```

**PySpark (identical API, different column syntax):**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, sum as spark_sum, col

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

ranked = (
    df
    .withColumn("rank", dense_rank().over(window_spec))
    .withColumn("running_total", spark_sum("salary").over(
        Window.partitionBy("department").orderBy("hire_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ))
)
```

Note: `sum` conflicts with Python built-in — import as `spark_sum`.

**Spark 4.0:** Window functions also support `GROUPS` frame type and `EXCLUDE` clause in SQL mode:
```python
spark.sql("""
    SELECT *, AVG(salary) OVER (
        PARTITION BY department ORDER BY hire_date
        GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as avg_salary
    FROM employees
""")
```

## Custom Partitioners

**Scala:**
```scala
import org.apache.spark.Partitioner

class CustomerPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Long].toInt % numPartitions
  }
}

rdd.partitionBy(new CustomerPartitioner(10))
```

**PySpark:**
```python
# PySpark doesn't support custom Partitioner classes directly.
# Use repartition with column expressions instead:

df.repartition(10, "customer_id")

# Or for RDDs, use partitionBy with a function:
rdd.partitionBy(10, lambda key: int(key) % 10)
```

## Sealed Traits / ADTs to Python Enums

**Scala:**
```scala
sealed trait OrderStatus
case object Pending extends OrderStatus
case object Shipped extends OrderStatus
case object Delivered extends OrderStatus
case object Cancelled extends OrderStatus

def statusToString(s: OrderStatus): String = s match {
  case Pending   => "PENDING"
  case Shipped   => "SHIPPED"
  case Delivered  => "DELIVERED"
  case Cancelled  => "CANCELLED"
}
```

**Python:**
```python
from enum import Enum

class OrderStatus(str, Enum):
    PENDING = "PENDING"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"

# In Spark context, use string columns with when/otherwise:
from pyspark.sql.functions import when, col

df.withColumn("status_label",
    when(col("status") == "PENDING", "Pending")
    .when(col("status") == "SHIPPED", "Shipped")
    .when(col("status") == "DELIVERED", "Delivered")
    .when(col("status") == "CANCELLED", "Cancelled")
)
```

## Error Handling

**Scala (Try/Either):**
```scala
import scala.util.{Try, Success, Failure}

val results = ds.map { record =>
  Try(transform(record)) match {
    case Success(v) => Right(v)
    case Failure(e) => Left((record, e.getMessage))
  }
}

val successes = results.filter(_.isRight).map(_.right.get)
val failures = results.filter(_.isLeft).map(_.left.get)
```

**PySpark — use column-level error handling or separate DataFrames:**
```python
from pyspark.sql.functions import col, when, lit

# Option 1: Flag errors in a column
result = df.withColumn(
    "is_valid",
    col("amount").isNotNull() & (col("amount") > 0)
)
successes = result.filter(col("is_valid"))
failures = result.filter(~col("is_valid"))

# Option 2: Use try_* functions (Spark 4.0 / DBR 16.x — ANSI mode is now default)
# IMPORTANT: Spark 4.0 enables ANSI mode by default, so casts/arithmetic
# that previously returned null will now throw. Use try_* functions explicitly.
from pyspark.sql.functions import try_cast, try_to_number, try_divide

df.select(
    try_cast(col("amount_str"), "double").alias("parsed_amount"),
    try_divide(col("revenue"), col("units")).alias("unit_price"),
)
```

## mapInPandas and applyInPandas

Replace Scala `.mapPartitions` and `.groupByKey.mapGroups` with pandas-based equivalents:

**Scala (mapPartitions):**
```scala
val result = ds.mapPartitions { iter =>
  val model = loadModel()
  iter.map(row => model.predict(row))
}
```

**PySpark (mapInPandas — partition-level pandas processing):**
```python
import pandas as pd

def predict_partition(iterator):
    model = load_model()  # loaded once per partition
    for pdf in iterator:
        pdf["prediction"] = model.predict(pdf[["feature1", "feature2"]])
        yield pdf

result = df.mapInPandas(predict_partition, schema=output_schema)
```

**Scala (groupByKey + mapGroups):**
```scala
ds.groupByKey(_.department)
  .mapGroups { (dept, iter) =>
    val rows = iter.toSeq
    (dept, rows.map(_.salary).sum / rows.size)
  }
```

**PySpark (applyInPandas — grouped pandas processing):**
```python
def dept_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "department": pdf["department"].iloc[0],
        "avg_salary": pdf["salary"].mean(),
        "headcount": len(pdf),
    }])

stats_schema = "department STRING, avg_salary DOUBLE, headcount LONG"
result = df.groupBy("department").applyInPandas(dept_stats, schema=stats_schema)
```

## Scala for-comprehension to Python List Comprehension / DataFrame Chain

**Scala:**
```scala
val metrics = for {
  col <- numericColumns
  stat <- Seq("mean", "stddev", "min", "max")
} yield df.selectExpr(s"$stat($col) as ${col}_$stat")
```

**PySpark:**
```python
from pyspark.sql.functions import mean, stddev, min as spark_min, max as spark_max
from functools import reduce

stat_fns = {"mean": mean, "stddev": stddev, "min": spark_min, "max": spark_max}

agg_exprs = [
    fn(c).alias(f"{c}_{name}")
    for c in numeric_columns
    for name, fn in stat_fns.items()
]

metrics = df.agg(*agg_exprs)
```

## Implicit Class / Extension Methods to Python Helpers

**Scala (implicit class for DataFrame extensions):**
```scala
implicit class DataFrameOps(df: DataFrame) {
  def dropDuplicatesByKey(keys: String*): DataFrame =
    df.dropDuplicates(keys)

  def withSnakeCaseColumns: DataFrame =
    df.columns.foldLeft(df) { (d, c) =>
      d.withColumnRenamed(c, c.replaceAll("([A-Z])", "_$1").toLowerCase.stripPrefix("_"))
    }
}

// Usage: df.withSnakeCaseColumns
```

**PySpark (plain functions or monkey-patching — prefer functions):**
```python
import re
from pyspark.sql import DataFrame

def to_snake_case(df: DataFrame) -> DataFrame:
    """Rename all columns to snake_case."""
    mapping = {c: re.sub(r"(?<!^)(?=[A-Z])", "_", c).lower() for c in df.columns}
    return df.withColumnsRenamed(mapping)

# Usage
result = to_snake_case(df)

# Or use transform() for method chaining:
result = df.transform(to_snake_case).filter(col("status") == "active")
```

## Photon / Databricks Runtime Optimization Notes

When migrating Scala to PySpark on Databricks, these patterns automatically benefit from Photon (DBR 16.x):

| Pattern | Photon Benefit |
|---|---|
| DataFrame operations (filter, join, agg) | Full native acceleration — no Python overhead |
| SQL queries via `spark.sql()` | Fully accelerated |
| Delta MERGE / UPDATE / DELETE | Accelerated with deletion vectors |
| Pandas UDFs (`@pandas_udf`) | Arrow transfer accelerated; UDF body runs in Python |
| Python scalar UDFs (`@udf`) | Serialization overhead — prefer built-in functions |
| RDD operations | **Not accelerated** — always convert to DataFrame API |

**Key guidance:** Photon accelerates the DataFrame/SQL engine, not Python code. The more you stay in DataFrame API / SQL, the more Photon helps. This is another reason to convert Scala RDD patterns to DataFrame API during migration.

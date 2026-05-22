# Scala/JVM to Python Library Equivalents

## Core Libraries

| Scala / JVM Library | Python Equivalent | Notes |
|---|---|---|
| `com.typesafe:config` | `python-decouple` / `pydantic-settings` | HOCON → `.env` or YAML config |
| `org.json4s` / `circe` / `spray-json` | `json` (stdlib) / `orjson` | Built-in JSON is usually sufficient |
| `com.fasterxml.jackson` | `json` / `pydantic` | Pydantic for schema validation |
| `org.scalatest` | `pytest` | |
| `org.specs2` | `pytest` | |
| `org.scalacheck` | `hypothesis` | Property-based testing |
| `org.mockito` (ScalaMock) | `unittest.mock` / `pytest-mock` | |
| `ch.qos.logback` / `log4j` | `logging` (stdlib) / `structlog` | |
| `com.github.scopt` | `argparse` (stdlib) / `click` / `typer` | CLI argument parsing |
| `org.apache.commons.lang3` | stdlib (`os`, `re`, `datetime`) | Most commons-lang is built into Python |
| `joda-time` | `datetime` (stdlib) / `pendulum` | |
| `com.google.guava` | stdlib (collections, itertools, functools) | |
| `org.apache.http` | `requests` / `httpx` | |
| `io.netty` | `aiohttp` / `httpx[http2]` | Async HTTP |
| `com.amazonaws:aws-java-sdk` | `boto3` | |
| `com.databricks:databricks-sdk-java` | `databricks-sdk` | |
| `org.apache.spark:spark-connect-client` | `databricks-connect==16.*` | Thin client for remote Spark development |

## Spark-Specific Libraries

| JVM Library | Python Equivalent | Notes |
|---|---|---|
| `io.delta:delta-spark` 4.0 (Scala) | `delta-spark` 4.0 (Python) | Same API — `DeltaTable.forPath(spark, path)`; pre-installed on DBR 16.x |
| `spark-xml` | `spark-xml` (same package) | Use `.format("xml")` in both |
| `spark-avro` | Built into PySpark 3.x | `spark.read.format("avro")` |
| `spark-csv` | Built into PySpark 2.x+ | `spark.read.csv()` |
| `spark-excel` | `spark-excel` or `openpyxl` + pandas | |
| `com.crealytics:spark-excel` | `com.crealytics:spark-excel` | Same JAR, configure via `.config("spark.jars.packages", ...)` |
| `com.redislabs:spark-redis` | `spark-redis` (same JAR) | |
| `org.elasticsearch:elasticsearch-spark` | `elasticsearch-spark` (same JAR) | Use `.format("es")` |
| `com.mongodb.spark:mongo-spark-connector` | `mongo-spark-connector` (same JAR) | |
| Custom Scala UDFs in JAR | Rewrite as Python UDFs or pandas UDFs | Or keep JAR and call via `spark.udf.registerJavaFunction()` |

## Keeping JVM Libraries from PySpark

You can still use JVM-based connectors from PySpark without rewriting them:

```python
# In notebook or spark-submit
spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,com.crealytics:spark-excel_2.13:4.0.0_0.21.0") \
    .getOrCreate()

# On Databricks, install via cluster libraries or %pip
# On DBR 16.x, delta-spark 4.0 and pyspark 4.0 are pre-installed — no install needed
# For local dev: pip install databricks-connect==16.1
```

## Calling Existing Scala UDFs from PySpark

If you have a JAR with well-tested Scala UDFs and don't want to rewrite them:

```python
# Register a Java/Scala UDF from a JAR
spark.udf.registerJavaFunction(
    "normalize_phone",                           # name in SQL
    "com.company.udfs.NormalizePhone",           # fully qualified class name
    StringType()                                  # return type
)

# Use in SQL
spark.sql("SELECT normalize_phone(phone) FROM customers")

# Use in DataFrame API
from pyspark.sql.functions import expr
df.withColumn("clean_phone", expr("normalize_phone(phone)"))
```

### Performance Impact of Keeping JVM UDFs

**Keeping Scala UDFs in JARs has no performance penalty** — they execute natively in the JVM with zero Python serialization. This is identical to calling them from Scala.

| Approach | Execution | Serialization | Performance |
|---|---|---|---|
| Scala UDF in JAR (via `registerJavaFunction`) | JVM-native | None | **Best** — same as Scala caller |
| Python scalar `@udf` | Python worker | Per-row pickle (Arrow on Spark 4.0) | Slowest |
| `@pandas_udf` | Python worker | Batch Arrow | Good for batch ops |
| Built-in `pyspark.sql.functions` | JVM-native + Photon | None | **Best** — also Catalyst-optimized |

**When to keep the JAR:**
- UDFs are complex, well-tested, and performance-critical
- You're running on classic compute (not serverless)
- Rewriting would be high-risk with no performance benefit

**When to rewrite in Python:**
- Targeting **serverless compute** — `registerJavaFunction` requires JVM access, which is unavailable on serverless or via Spark Connect (`DatabricksSession`)
- The UDF can be replaced by a **built-in function** — this is the real win (Catalyst + Photon optimization)
- You want to eliminate JAR dependency management entirely

**When to rewrite as built-in functions (best option):**
```python
# ❌ UDF (opaque to optimizer, no Photon)
@udf(StringType())
def normalize_phone(phone):
    if phone is None: return None
    cleaned = re.sub(r"[^0-9]", "", phone)
    return cleaned if len(cleaned) >= 10 else None

# ✅ Built-in functions (Catalyst-optimized, Photon-accelerated)
from pyspark.sql.functions import regexp_replace, length, when

df.withColumn("clean_phone",
    when(
        length(regexp_replace("phone", r"[^0-9]", "")) >= 10,
        regexp_replace("phone", r"[^0-9]", "")
    )
)
```

## Data Validation

| Scala | Python | Notes |
|---|---|---|
| Custom validation with ScalaTest assertions | `great_expectations` | Full data validation framework |
| Schema enforcement via `Encoder[T]` | `pandera` + PySpark | Schema validation for DataFrames |
| Manual column checks | `chispa` | DataFrame comparison in tests |
| Deequ (Amazon) | `pydeequ` or `databricks-dqx` | Data quality at scale |
| Delta Lake `MERGE` (Scala API) | `DeltaTable.forPath().alias().merge()` | Identical Python API on Delta 4.0 |
| Delta Lake `OPTIMIZE` / `ZORDER` | `spark.sql("OPTIMIZE ...")` or liquid clustering | Liquid clustering (DBR 16.x) replaces ZORDER — auto-managed |

## Configuration Management

**Scala (Typesafe Config):**
```scala
import com.typesafe.config.ConfigFactory

val config = ConfigFactory.load()  // reads application.conf
val dbHost = config.getString("database.host")
val batchSize = config.getInt("processing.batch_size")
```

**Python (multiple options):**
```python
# Option 1: python-decouple (reads .env files)
from decouple import config
db_host = config("DATABASE_HOST")
batch_size = config("BATCH_SIZE", cast=int, default=1000)

# Option 2: pydantic-settings (typed, validated config)
from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):
    database_host: str
    batch_size: int = 1000

    class Config:
        env_prefix = ""

cfg = AppConfig()

# Option 3: On Databricks, use widgets or job parameters
db_host = dbutils.widgets.get("database_host")
batch_size = int(dbutils.widgets.get("batch_size"))

# Option 4: Databricks Asset Bundles variables (DBR 16.x / DABs)
# Define in databricks.yml, access via job parameters — no code changes needed
```

## Databricks Connect for Local Development (DBR 16.x)

Replace local Spark installs with `databricks-connect` for thin-client remote execution:

```python
# pip install databricks-connect==16.1

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("DEFAULT").getOrCreate()

# Full DataFrame API works — executes on remote cluster
df = spark.read.table("catalog.schema.table")
df.filter(col("status") == "active").show()
```

This replaces the Scala pattern of running `spark-submit` with a local Spark installation, and is the recommended approach for iterating on PySpark code locally against a Databricks cluster.

## MLflow and Feature Engineering

| Scala / JVM | Python Equivalent | Notes |
|---|---|---|
| MLflow Java client | `mlflow` | `mlflow.spark.log_model()`, `mlflow.pyfunc.log_model()` — pre-installed on DBR 16.x |
| Custom feature pipelines | `databricks-feature-engineering` | Feature tables in Unity Catalog; `FeatureEngineeringClient` for serving |
| Manual model registry | `mlflow.register_model()` | Unity Catalog model registry is default on DBR 16.x |

```python
import mlflow
from databricks.feature_engineering import FeatureEngineeringClient

# Log a PySpark pipeline model
with mlflow.start_run():
    mlflow.spark.log_model(pipeline_model, "model")

# Feature engineering with Unity Catalog
fe = FeatureEngineeringClient()
fe.create_table(
    name="main.features.customer_features",
    primary_keys=["customer_id"],
    df=feature_df,
)
```

## Unity Catalog Volumes for File I/O

Replace direct cloud storage paths with governed Volume paths:

| Old Pattern | New Pattern (DBR 16.x) |
|---|---|
| `s3://bucket/path/file.csv` | `/Volumes/catalog/schema/volume/file.csv` |
| `abfss://container@account.dfs.core.windows.net/path` | `/Volumes/catalog/schema/volume/path` |
| `dbfs:/mnt/mount_name/path` | `/Volumes/catalog/schema/volume/path` |

```python
# Reading from Volumes
df = spark.read.csv("/Volumes/main/raw_data/landing/customers.csv", header=True)

# Writing to Volumes
df.toPandas().to_csv("/Volumes/main/raw_data/exports/report.csv", index=False)

# Listing files in Volumes
files = dbutils.fs.ls("/Volumes/main/raw_data/landing/")
```

## Delta Lake 4.0 Table Properties (DBR 16.x)

Key properties to set when creating new Delta tables:

```python
spark.sql("""
    CREATE TABLE main.silver.orders (
        order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_date DATE
    )
    USING DELTA
    CLUSTER BY (customer_id, order_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',         -- faster DELETE/MERGE
        'delta.enableTypeWidening' = 'true',             -- widen column types without rewrite
        'delta.universalFormat.enabledFormats' = 'iceberg'  -- UniForm: read as Iceberg
    )
""")
```

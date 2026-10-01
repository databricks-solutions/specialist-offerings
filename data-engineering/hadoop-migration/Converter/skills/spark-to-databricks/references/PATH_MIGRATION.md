# Path Migration: HDFS → Databricks

## Path Mapping

| HDFS Path Pattern | Databricks Path | Notes |
|---|---|---|
| `hdfs:///data/raw/` | `/Volumes/<catalog>/<schema>/raw/` | UC Volumes (recommended) |
| `hdfs:///user/hive/warehouse/` | Managed tables via UC | No explicit path |
| `hdfs:///tmp/spark-*` | Local temp (automatic) | Handled by Databricks |
| `hdfs:///jars/*.jar` | `dbfs:/jars/` or UC Volumes | Upload JARs |
| `hdfs:///user/<name>/` | `/Volumes/<catalog>/<schema>/user_data/` | Per-user data |
| `file:///local/path` | `/Volumes/<catalog>/<schema>/local/` | Upload to Volumes |

## Code Patterns

### Reading Data

```python
# Before
df = spark.read.parquet("hdfs:///data/warehouse/events/")
df = spark.read.csv("hdfs:///data/raw/users.csv", header=True)
df = spark.read.json("/data/logs/2024/01/")

# After (UC Volumes)
df = spark.read.parquet("/Volumes/main/warehouse/events/")
df = spark.read.csv("/Volumes/main/raw/users.csv", header=True)
df = spark.read.json("/Volumes/main/logs/2024/01/")

# After (UC Tables — preferred for structured data)
df = spark.table("main.warehouse.events")
```

### Writing Data

```python
# Before
df.write.mode("overwrite").parquet("hdfs:///data/processed/output/")
df.write.saveAsTable("mydb.results")

# After (UC managed table — preferred)
df.write.mode("overwrite").saveAsTable("main.processed.output")

# After (UC Volumes — for file-based output)
df.write.mode("overwrite").parquet("/Volumes/main/processed/output/")
```

### Configuration Files

```python
# Before
config_path = "hdfs:///config/app.properties"
sc.addFile(config_path)

# After
config_path = "/Volumes/main/default/config/app.properties"
# Or use Databricks secrets for sensitive config:
db_host = dbutils.secrets.get("scope", "db-host")
```

### Temporary Storage

```python
# Before
df.write.parquet("hdfs:///tmp/intermediate_results")

# After (use temp views or managed tables)
df.createOrReplaceTempView("intermediate_results")
# Or:
df.write.mode("overwrite").saveAsTable("main.temp.intermediate_results")
```

## Regex Pattern for Automated Replacement

```python
import re

# Pattern to find HDFS paths in code
hdfs_pattern = r'["\'](?:hdfs://[^/]*)?(/(?:data|user|tmp|config|jars|lib)[^"\']*)["\']'

# Replacement mapping function
def replace_hdfs_path(match):
    path = match.group(1)
    if path.startswith("/data/"):
        return f'"/Volumes/main/default{path}"'
    elif path.startswith("/user/hive/warehouse/"):
        return '"main.default.<table>"  # Use spark.table() instead'
    elif path.startswith("/tmp/"):
        return f'"/Volumes/main/default/tmp/{path.split("/tmp/")[-1]}"'
    return f'"/Volumes/main/default{path}"'
```

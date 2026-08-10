# spark-submit → Databricks Jobs

## Command Mapping

### Basic spark-submit

```bash
# Before
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.ETLJob \
  --num-executors 10 \
  --executor-memory 4g \
  --executor-cores 4 \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=200 \
  --jars lib/utils.jar,lib/connectors.jar \
  --files config/app.properties \
  hdfs:///jars/etl-job.jar \
  --input /data/raw --output /data/processed
```

### Databricks Job JSON

```json
{
  "name": "ETL Job",
  "tasks": [
    {
      "task_key": "etl_main",
      "spark_jar_task": {
        "main_class_name": "com.example.ETLJob",
        "parameters": ["--input", "/Volumes/main/default/raw", "--output", "/Volumes/main/default/processed"]
      },
      "libraries": [
        {"jar": "dbfs:/jars/etl-job.jar"},
        {"jar": "dbfs:/jars/utils.jar"},
        {"jar": "dbfs:/jars/connectors.jar"}
      ],
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "autoscale": {
          "min_workers": 2,
          "max_workers": 10
        },
        "spark_conf": {
          "spark.sql.shuffle.partitions": "200"
        }
      }
    }
  ]
}
```

## PySpark Submit

```bash
# Before
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files utils.zip,transforms.py \
  etl_job.py \
  --date 2024-01-01
```

### Databricks Job JSON

```json
{
  "name": "PySpark ETL Job",
  "tasks": [
    {
      "task_key": "pyspark_etl",
      "spark_python_task": {
        "python_file": "dbfs:/scripts/etl_job.py",
        "parameters": ["--date", "2024-01-01"]
      },
      "libraries": [
        {"pypi": {"package": "pandas"}},
        {"whl": "dbfs:/libs/utils-1.0-py3-none-any.whl"}
      ],
      "new_cluster": {
        "spark_version": "15.4.x-python3",
        "node_type_id": "i3.xlarge",
        "autoscale": {
          "min_workers": 2,
          "max_workers": 8
        }
      }
    }
  ]
}
```

## Parameter Mapping

| spark-submit Flag | Databricks Equivalent |
|---|---|
| `--master yarn` | Removed (managed) |
| `--deploy-mode cluster` | Removed (always cluster) |
| `--class` | `spark_jar_task.main_class_name` |
| `--num-executors N` | `autoscale.max_workers` |
| `--executor-memory Xg` | Node type selection |
| `--executor-cores N` | Node type selection |
| `--driver-memory Xg` | Driver node type |
| `--conf key=value` | `spark_conf` in cluster config |
| `--jars` | `libraries[].jar` |
| `--py-files` | `libraries[].whl` or `libraries[].egg` |
| `--files` | Upload to DBFS/Volumes, reference in code |
| `--packages` | `libraries[].maven` |
| App JAR path | `spark_jar_task` + `libraries[].jar` |
| App arguments | `parameters[]` |

## Scheduling

```bash
# Before (cron on edge node)
0 2 * * * spark-submit --master yarn ... etl-job.jar

# After (Databricks Job schedule)
# Add to job JSON:
{
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```

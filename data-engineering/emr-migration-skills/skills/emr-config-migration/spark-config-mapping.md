# Spark Configuration Mapping: EMR to Databricks

## Overview

This file categorizes every common EMR Spark configuration by action: **Keep** (transfer directly), **Remove** (not applicable on Databricks), or **Replace** (substitute with Databricks equivalent).

---

## Category 1: Memory and Cores — KEEP (with caveats)

Databricks auto-tunes these if not set. Only override if you have specific requirements.

| Config | Default Behavior on Databricks | Recommendation |
|---|---|---|
| `spark.executor.memory` | Auto-tuned based on node type | Keep if you tuned for specific workloads; otherwise remove and let Databricks optimize |
| `spark.executor.cores` | Auto-tuned | Same as above |
| `spark.driver.memory` | Auto-tuned based on driver node | Keep if driver runs heavy collect() or broadcast joins |
| `spark.driver.cores` | Auto-tuned | Usually safe to remove |
| `spark.executor.memoryOverhead` | Auto-tuned | Keep only if you had OOM issues on EMR that required tuning |
| `spark.driver.memoryOverhead` | Auto-tuned | Same |
| `spark.memory.fraction` | 0.6 | Keep if you tuned this for specific workloads |
| `spark.memory.storageFraction` | 0.5 | Keep if you tuned this |
| `spark.executor.instances` | Managed by autoscaling | Remove — use autoscale min/max workers instead |

**Example migration:**

```yaml
# EMR spark-defaults.conf
# spark.executor.memory=8g
# spark.executor.cores=4
# spark.executor.instances=10
# spark.driver.memory=4g

# Databricks cluster config
new_cluster:
  node_type_id: "m5.2xlarge"  # 8 vCPU, 32 GB — Databricks auto-tunes memory/cores
  autoscale:
    min_workers: 4
    max_workers: 12
  spark_conf:
    # Only keep if specifically needed:
    # spark.executor.memory: "8g"
    # spark.driver.memory: "4g"
```

---

## Category 2: Dynamic Allocation — REMOVE

Databricks uses its own autoscaling mechanism instead of Spark's dynamic allocation.

| Config | Action | Notes |
|---|---|---|
| `spark.dynamicAllocation.enabled` | Remove | Use Databricks `autoscale` instead |
| `spark.dynamicAllocation.minExecutors` | Remove | Maps to `autoscale.min_workers` |
| `spark.dynamicAllocation.maxExecutors` | Remove | Maps to `autoscale.max_workers` |
| `spark.dynamicAllocation.initialExecutors` | Remove | Databricks starts with min_workers |
| `spark.dynamicAllocation.executorIdleTimeout` | Remove | Databricks manages executor lifecycle |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | Remove | Databricks handles scale-up triggers |
| `spark.shuffle.service.enabled` | Remove | Required for Spark dynamic allocation; not needed on Databricks |

**Migration:**

```yaml
# EMR:
# spark.dynamicAllocation.enabled=true
# spark.dynamicAllocation.minExecutors=2
# spark.dynamicAllocation.maxExecutors=20

# Databricks:
autoscale:
  min_workers: 2
  max_workers: 20
```

---

## Category 3: YARN — REMOVE

All YARN configurations must be removed. Databricks does not use YARN.

| Config | Action |
|---|---|
| `spark.yarn.am.memory` | Remove |
| `spark.yarn.am.cores` | Remove |
| `spark.yarn.executor.memoryOverhead` | Remove |
| `spark.yarn.driver.memoryOverhead` | Remove |
| `spark.yarn.maxAppAttempts` | Remove (use Databricks job retries) |
| `spark.yarn.submit.waitAppCompletion` | Remove |
| `spark.yarn.queue` | Remove (use cluster policies for resource governance) |
| `spark.yarn.jars` | Remove |
| `spark.yarn.archive` | Remove |
| `spark.yarn.dist.files` | Remove (use `libraries` in task config) |
| `spark.yarn.dist.archives` | Remove |
| `spark.yarn.appMasterEnv.*` | Remove |

---

## Category 4: S3 and EMRFS — REPLACE with Unity Catalog

Remove all direct S3 credential and EMRFS configurations. Use Unity Catalog storage credentials and external locations instead.

| Config | Action | Replacement |
|---|---|---|
| `fs.s3.consistent` | Remove | Delta Lake provides ACID consistency |
| `fs.s3.consistent.metadata.tableName` | Remove | DynamoDB table no longer needed |
| `fs.s3a.access.key` | Remove | Use UC storage credentials |
| `fs.s3a.secret.key` | Remove | Use UC storage credentials |
| `fs.s3a.session.token` | Remove | Use UC storage credentials |
| `fs.s3a.endpoint` | Remove | Standard S3 endpoint used |
| `fs.s3a.impl` | Remove | Databricks has its own S3 client |
| `fs.s3a.aws.credentials.provider` | Remove | UC handles auth |
| `fs.s3.enableServerSideEncryption` | Remove | Configure on S3 bucket policy |
| `fs.s3.serverSideEncryptionAlgorithm` | Remove | Configure on S3 bucket policy |
| `fs.s3.canned.acl` | Remove | UC handles permissions |
| `spark.hadoop.fs.s3a.*` | Remove | All S3A Hadoop configs |
| `spark.hadoop.fs.s3n.*` | Remove | Legacy S3N configs |

---

## Category 5: Hive Metastore — REMOVE

Unity Catalog replaces the Hive Metastore. Remove all HMS-related configurations.

| Config | Action | Notes |
|---|---|---|
| `hive.metastore.client.factory.class` | Remove | UC is the metastore |
| `javax.jdo.option.ConnectionURL` | Remove | HMS JDBC connection |
| `javax.jdo.option.ConnectionDriverName` | Remove | HMS JDBC driver |
| `javax.jdo.option.ConnectionUserName` | Remove | HMS credentials |
| `javax.jdo.option.ConnectionPassword` | Remove | HMS credentials |
| `hive.metastore.uris` | Remove | Thrift HMS URI |
| `hive.metastore.warehouse.dir` | Remove | UC manages table locations |
| `spark.sql.hive.metastore.version` | Remove | Not applicable |
| `spark.sql.hive.metastore.jars` | Remove | Not applicable |
| `spark.hadoop.hive.metastore.client.factory.class` | Remove | UC replaces Glue catalog |

**If using AWS Glue as HMS on EMR:**

```python
# EMR (Glue as metastore):
# spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory

# Databricks: No config needed. Tables are in Unity Catalog.
# Access Glue tables via external locations if needed during migration.
```

---

## Category 6: SQL Performance — KEEP

These configs generally transfer directly and are beneficial on Databricks.

| Config | Default in DBR | Action |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | Keep — tune for data size |
| `spark.sql.adaptive.enabled` | true | Keep (already default in DBR) |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Keep |
| `spark.sql.adaptive.skewJoin.enabled` | true | Keep |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Keep — increase for larger dimension tables |
| `spark.sql.parquet.mergeSchema` | false | Keep |
| `spark.sql.parquet.filterPushdown` | true | Keep |
| `spark.sql.orc.filterPushdown` | true | Keep |
| `spark.sql.files.maxPartitionBytes` | 128MB | Keep |
| `spark.sql.files.openCostInBytes` | 4MB | Keep |
| `spark.sql.broadcastTimeout` | 300 | Keep if you have large broadcasts |
| `spark.sql.crossJoin.enabled` | false | Keep |
| `spark.sql.sources.partitionOverwriteMode` | static | Keep — set to "dynamic" if needed |

---

## Category 7: Serialization — KEEP

| Config | Action | Notes |
|---|---|---|
| `spark.serializer` | Keep | `org.apache.spark.serializer.KryoSerializer` recommended |
| `spark.kryo.registrationRequired` | Keep | Keep if set to true for performance |
| `spark.kryo.registrator` | Keep | Keep custom registrators |
| `spark.kryoserializer.buffer.max` | Keep | Increase if serialization errors occur |
| `spark.kryoserializer.buffer` | Keep | Initial buffer size |

---

## Category 8: Network and Shuffle — KEEP

| Config | Action | Notes |
|---|---|---|
| `spark.network.timeout` | Keep | Default 120s; increase for slow networks |
| `spark.rpc.message.maxSize` | Keep | Default 128MB; increase for large broadcasts |
| `spark.shuffle.compress` | Keep | Default true |
| `spark.shuffle.spill.compress` | Keep | Default true |
| `spark.reducer.maxSizeInFlight` | Keep | Default 48MB |
| `spark.shuffle.file.buffer` | Keep | Default 32KB |
| `spark.shuffle.io.maxRetries` | Keep | Default 3 |
| `spark.shuffle.io.retryWait` | Keep | Default 5s |

---

## Category 9: Library and Classpath — REPLACE

| Config | Action | Notes |
|---|---|---|
| `spark.jars` | Replace | Use `libraries` in task/cluster config |
| `spark.jars.packages` | Replace | Use `libraries` with `maven` coordinates |
| `spark.jars.repositories` | Replace | Use cluster library config |
| `spark.driver.extraClassPath` | Replace | Use init scripts or `libraries` |
| `spark.executor.extraClassPath` | Replace | Use init scripts or `libraries` |
| `spark.driver.extraJavaOptions` | Keep | Only non-classpath JVM options |
| `spark.executor.extraJavaOptions` | Keep | Only non-classpath JVM options |
| `spark.pyspark.python` | Replace | Use `spark_env_vars.PYSPARK_PYTHON` |
| `spark.pyspark.driver.python` | Replace | Same |

---

## Category 10: Streaming — KEEP (mostly)

| Config | Action | Notes |
|---|---|---|
| `spark.streaming.blockInterval` | Keep | For DStreams (consider migrating to Structured Streaming) |
| `spark.streaming.backpressure.enabled` | Keep | For DStreams |
| `spark.sql.streaming.checkpointLocation` | Keep | Update path from S3 to DBFS/Volumes |
| `spark.sql.streaming.schemaInference` | Keep | |
| `spark.sql.streaming.stateStore.providerClass` | Remove | Databricks has optimized state store |

---

## Migration Script Template

Use this Python script to categorize and convert EMR configurations:

```python
def categorize_emr_config(config_key: str) -> str:
    """Categorize an EMR Spark config as KEEP, REMOVE, or REPLACE."""

    remove_prefixes = [
        "spark.yarn.", "spark.dynamicAllocation.", "spark.shuffle.service.",
        "fs.s3.", "fs.s3a.", "fs.s3n.", "spark.hadoop.fs.s3",
        "hive.metastore.", "javax.jdo.", "spark.sql.hive.metastore",
        "spark.hadoop.hive.metastore",
    ]

    replace_prefixes = [
        "spark.jars", "spark.driver.extraClassPath", "spark.executor.extraClassPath",
        "spark.pyspark.",
    ]

    for prefix in remove_prefixes:
        if config_key.startswith(prefix):
            return "REMOVE"

    for prefix in replace_prefixes:
        if config_key.startswith(prefix):
            return "REPLACE"

    return "KEEP"
```

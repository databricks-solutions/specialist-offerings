---
name: emr-config-migration
description: "Convert EMR Spark and YARN configurations to Databricks cluster configurations and policies. Use when: (1) 'spark-defaults.conf to Databricks', (2) 'YARN configuration mapping', (3) 'EMR configuration classifications', (4) 'cluster policy from EMR config', (5) 'EMR spark config not working on Databricks'."
---

# EMR Configuration Migration to Databricks

## Overview

EMR uses **configuration classifications** (JSON objects) to configure Spark, YARN, Hadoop, Hive, and other components. Databricks uses **cluster configurations** (spark_conf, cluster policies, init scripts) that are simpler because YARN, HDFS, and EMRFS do not exist.

## Critical Rules

1. **Remove all YARN configurations** — Databricks does not use YARN. YARN settings are ignored or cause errors.
2. **Remove all EMRFS configurations** — `fs.s3.*` and EMRFS consistency settings are not needed. Unity Catalog handles S3 access.
3. **Remove all HDFS configurations** — Databricks does not use HDFS. Use DBFS, Unity Catalog Volumes, or cloud storage.
4. **Keep Spark SQL configurations** — Most `spark.sql.*` configs transfer directly.
5. **Replace S3 credentials** — Remove `fs.s3a.access.key`/`secret.key`. Use Unity Catalog storage credentials instead.
6. **Remove Hive Metastore configs** — Unity Catalog replaces HMS. Remove `hive.metastore.*` settings.
7. **Adjust memory/core configs** — Databricks auto-tunes many memory settings. Over-specifying can hurt performance.

## EMR Classification to Databricks Mapping

| EMR Classification | Databricks Equivalent | Action |
|---|---|---|
| `spark-defaults` | `spark_conf` on cluster | Keep applicable Spark configs |
| `spark-env` | `spark_env_vars` on cluster | Keep env vars, remove YARN-related |
| `spark-hive-site` | Not needed | Unity Catalog replaces HMS |
| `yarn-site` | Not applicable | Remove entirely |
| `yarn-env` | Not applicable | Remove entirely |
| `capacity-scheduler` | Cluster policies | Map scheduling constraints to policies |
| `core-site` | `spark_conf` (partial) | Keep `fs.` configs only if custom filesystems |
| `hdfs-site` | Not applicable | Remove entirely |
| `hive-site` | Not needed | Unity Catalog replaces HMS |
| `emrfs-site` | Not applicable | Remove entirely |
| `hadoop-env` | Not applicable | Remove entirely |
| `mapred-site` | Not applicable | Remove entirely |

## How to Extract EMR Configurations

```bash
# Get configuration from running EMR cluster
aws emr describe-cluster --cluster-id j-XXXXX --query 'Cluster.Configurations'

# Get from CloudFormation/Terraform
# Look for Configurations property in aws_emr_cluster resource

# Get from EMR step/job flow definition
aws emr list-clusters --active
aws emr describe-cluster --cluster-id j-XXXXX
```

## How to Apply on Databricks

### In DABs (databricks.yml)

```yaml
resources:
  jobs:
    my_job:
      tasks:
        - task_key: main_task
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "m5.2xlarge"
            num_workers: 4
            spark_conf:
              # Migrated from EMR spark-defaults
              spark.sql.shuffle.partitions: "200"
              spark.sql.adaptive.enabled: "true"
              spark.serializer: "org.apache.spark.serializer.KryoSerializer"
            spark_env_vars:
              # Migrated from EMR spark-env
              PYSPARK_PYTHON: "/databricks/python3/bin/python3"
```

### In Cluster Policy (for standardization)

```json
{
  "spark_conf.spark.sql.shuffle.partitions": {
    "type": "fixed",
    "value": "200"
  },
  "spark_conf.spark.sql.adaptive.enabled": {
    "type": "fixed",
    "value": "true"
  }
}
```

### Via Cluster API

```bash
databricks clusters create --json '{
  "cluster_name": "migrated-emr-cluster",
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "m5.2xlarge",
  "num_workers": 4,
  "spark_conf": {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true"
  }
}'
```

## Quick Reference: Common EMR Configs and Databricks Fate

| EMR Config | Keep/Remove/Replace | Notes |
|---|---|---|
| `spark.executor.memory` | Keep (but consider auto-tuning) | Databricks auto-tunes if not set |
| `spark.executor.cores` | Keep (but consider auto-tuning) | Databricks auto-tunes if not set |
| `spark.driver.memory` | Keep (but consider auto-tuning) | Databricks auto-tunes if not set |
| `spark.sql.shuffle.partitions` | Keep | AQE may override at runtime |
| `spark.sql.adaptive.enabled` | Keep | Enabled by default in DBR |
| `spark.dynamicAllocation.enabled` | Remove | Use Databricks autoscaling instead |
| `spark.yarn.*` | Remove | No YARN |
| `spark.hadoop.fs.s3.*` | Remove | Use UC storage credentials |
| `spark.serializer` | Keep | Kryo is a good choice |
| `spark.sql.parquet.mergeSchema` | Keep | Transfer directly |
| `hive.metastore.client.factory.class` | Remove | Unity Catalog replaces HMS |

See `spark-config-mapping.md` for the complete categorized mapping.
See `yarn-to-databricks.md` for YARN-to-Databricks compute model comparison.
See `cluster-policies.md` for creating policies from EMR configurations.

## Related Skills

- **emr-infra-migration** — instance types, networking, IAM
- **emr-steps-to-workflows** — convert EMR Steps to Databricks Jobs
- **emr-migration-validation** — validate migrated configurations work correctly

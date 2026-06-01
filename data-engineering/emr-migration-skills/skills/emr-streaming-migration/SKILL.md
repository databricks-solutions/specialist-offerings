---
name: emr-streaming-migration
description: "Migrate EMR Spark Streaming workloads to Databricks Structured Streaming. Use when: (1) 'EMR streaming to Databricks', (2) 'Kinesis streaming migration', (3) 'Spark Streaming to Structured Streaming', (4) 'migrate streaming checkpoint', (5) 'DStream to Structured Streaming'."
---

# EMR Streaming Migration to Databricks

## Overview

EMR commonly runs Spark Streaming (DStreams) or Structured Streaming workloads. Databricks uses Structured Streaming exclusively and additionally offers Delta Live Tables (Lakeflow) for declarative streaming pipelines. DStreams are deprecated since Spark 3.4 and must be migrated to Structured Streaming.

## Critical Rules

1. **DStreams are deprecated** -- you MUST migrate to Structured Streaming API
2. **Checkpoint locations must be reset** -- EMR checkpoints on S3 are NOT compatible with Databricks
3. **Consider Delta Live Tables** for new streaming pipelines instead of raw Structured Streaming
4. **Auto Loader (cloudFiles)** replaces file-based streaming and is far superior to `fileStream`

## Migration Paths

### Path 1: Structured Streaming on EMR to Structured Streaming on Databricks

**Effort: Low** -- Minimal code changes required.

Changes needed:
- Update source connector configurations (broker addresses, credentials)
- Reset checkpoint locations (new path on DBFS/UC Volumes)
- Replace `s3a://` paths with Unity Catalog external locations or Volumes
- Update library dependencies (remove EMR-specific JARs)
- Consider enabling Photon for performance

### Path 2: DStream API to Structured Streaming

**Effort: High** -- Full API rewrite required.

The DStream API (RDD-based) must be rewritten to use the DataFrame-based Structured Streaming API. Key conceptual changes:
- `StreamingContext` becomes `SparkSession.readStream`
- `DStream.foreachRDD()` becomes `writeStream.foreachBatch()`
- `updateStateByKey()` becomes `flatMapGroupsWithState()` or `mapGroupsWithState()`
- `window()` becomes `.groupBy(window())` with watermarks
- Receivers become Source connectors

### Path 3: New Pipelines with Delta Live Tables (Lakeflow)

**Effort: Medium** -- New declarative approach, but simpler code.

For pipelines that do bronze/silver/gold medallion processing, Delta Live Tables provides:
- Declarative pipeline definitions (SQL or Python)
- Automatic dependency management
- Built-in data quality expectations
- Managed compute and scaling
- Visual pipeline monitoring

## Source Connector Mapping

| EMR Source | Databricks Equivalent |
|---|---|
| Kinesis (spark-streaming-kinesis-asl) | Kinesis connector or Kafka (via MSK) |
| Kafka | Kafka (same connector, update broker addresses) |
| S3 (fileStream) | cloudFiles (Auto Loader) -- much better |
| Socket | socket (same) |
| Custom receivers | Custom Source V2 API |

## Checkpoint Considerations

**EMR checkpoints on S3 are NOT compatible with Databricks.** This is because:
- Internal checkpoint format includes cluster-specific metadata
- Serialization formats may differ between runtimes
- Offset tracking is tied to the execution environment

**Migration strategies:**
1. **Reset from latest offset** -- simplest, minimal data loss if sources retain data
2. **Reset from specific offset** -- precise, requires manual offset tracking
3. **Dual-write period** -- run EMR and Databricks in parallel during transition

See `checkpoint-migration.md` for detailed strategies.

## Sink Migration

| EMR Sink | Databricks Equivalent |
|---|---|
| S3 (Parquet/JSON/CSV) | Delta table (PREFERRED) or S3 via external location |
| HDFS | Delta table or UC Volumes |
| Kafka | Kafka (same connector) |
| Custom foreach writer | foreachBatch with Delta merge (PREFERRED) |
| DynamoDB | foreachBatch with DynamoDB SDK |
| Elasticsearch | foreachBatch with ES client |

## Related Skills

- **databricks-spark-structured-streaming**: Comprehensive Structured Streaming guide for Databricks
- **databricks-spark-declarative-pipelines**: Delta Live Tables / Lakeflow SDP guide
- **emr-spark-code-migration**: General Spark code migration patterns

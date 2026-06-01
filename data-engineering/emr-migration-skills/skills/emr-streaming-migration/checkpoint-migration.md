# Checkpoint Handling During Streaming Migration

## Key Fact

**Spark Structured Streaming checkpoints are NOT portable between EMR and Databricks.** This is due to:
- Internal metadata includes cluster-specific information
- Serialization versions may differ between Databricks Runtime and EMR's Spark distribution
- Offset tracking formats for sources like Kinesis/Kafka may differ
- State store formats (in-memory vs RocksDB) are not cross-compatible

You MUST plan for checkpoint reset when migrating streaming workloads.

## Migration Strategy 1: Reset from Latest Offset

**Best for**: Workloads where brief data gaps are acceptable.

**How it works:**
1. Stop the EMR streaming job
2. Start the Databricks streaming job with `startingOffsets = "latest"`
3. New job picks up from the current position in the source

**Pros:**
- Simplest approach
- No coordination needed
- Fastest migration

**Cons:**
- Data produced between EMR stop and Databricks start is lost
- Not suitable for exactly-once requirements

```python
# Databricks -- start from latest
query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "my-topic")
    .option("startingOffsets", "latest")  # Skip to current position
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/my-stream-v2")
    .table("catalog.schema.target_table")
)
```

## Migration Strategy 2: Reset from Specific Offset

**Best for**: Workloads requiring no data loss.

**How it works:**
1. Record the last successfully processed offset from EMR (check EMR checkpoint metadata or application logs)
2. Stop the EMR streaming job
3. Start Databricks job from the recorded offset
4. Use idempotent writes (Delta MERGE) to handle potential duplicates

**Pros:**
- No data loss
- Precise control over starting position

**Cons:**
- Requires manual offset tracking
- May produce duplicates (handle with idempotent sink)

```python
# Kafka: specify exact starting offsets per partition
starting_offsets = '{"my-topic":{"0":12345,"1":67890,"2":11111}}'

query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "my-topic")
    .option("startingOffsets", starting_offsets)
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/my-stream-v2")
    .foreachBatch(upsert_to_delta)  # Idempotent writes
    .start()
)

def upsert_to_delta(batch_df, batch_id):
    """Idempotent upsert to handle potential duplicates during migration."""
    from delta.tables import DeltaTable
    
    target = DeltaTable.forName(spark, "catalog.schema.target_table")
    target.alias("t").merge(
        batch_df.alias("s"),
        "t.event_id = s.event_id"  # Deduplicate on business key
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
```

## Migration Strategy 3: Dual-Write Period

**Best for**: Mission-critical workloads requiring zero downtime and zero data loss.

**How it works:**
1. Start Databricks streaming job while EMR is still running (both read from same source)
2. Both write to their respective sinks
3. Validate Databricks output matches EMR output
4. Once validated, stop EMR job
5. Switch downstream consumers to Databricks output

**Pros:**
- Zero downtime
- Zero data loss
- Built-in validation period
- Easy rollback (just stop Databricks and keep EMR)

**Cons:**
- Double resource cost during overlap period
- Need to handle duplicate processing at the sink
- Source must support multiple consumers (Kafka yes, some Kinesis configs may need adjustment)

```python
# Databricks -- run alongside EMR
# Use "earliest" to catch up, or specific offsets matching EMR's start
query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "my-topic")
    .option("startingOffsets", "earliest")  # Process all available data
    .option("maxOffsetsPerTrigger", 100000)  # Throttle during catchup
    .load()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/my-stream-v2")
    .table("catalog.schema.target_table_databricks")  # Write to separate table during validation
)

# After validation passes, rename table or switch downstream consumers
```

## State Store Migration

For stateful streaming operations (aggregations, joins, deduplication):

### RocksDB State Store

- EMR may use the default in-memory state store or RocksDB
- Databricks uses RocksDB state store by default (configurable)
- State is NOT transferable between platforms
- **Impact**: Stateful operations will "reset" -- running counts, session windows, etc. will start fresh

### Mitigation for Stateful Operations

1. **Aggregations**: Pre-compute final state on EMR, load as initial state on Databricks
2. **Session windows**: Accept gap in sessions during migration, or replay from source
3. **Deduplication**: Use Delta MERGE for idempotent writes to handle replayed data
4. **Running totals**: Snapshot current totals, use as starting point in Databricks logic

## Watermark Considerations

- Watermarks are part of the streaming state and are NOT migrated
- On restart, the watermark starts fresh
- This means late data that would have been dropped on EMR may be re-accepted on Databricks
- This is generally safe (duplicates can be handled) but may affect window aggregation results temporarily

## Exactly-Once Guarantees During Migration

Exactly-once semantics require:
1. **Source**: Replayable (Kafka/Kinesis support this)
2. **Processing**: Idempotent (use deterministic logic)
3. **Sink**: Idempotent (Delta MERGE, or use business key deduplication)

During migration, exact-once is maintained by:
- Using Delta tables as sinks (ACID transactions)
- Implementing MERGE (upsert) logic with business keys
- Ensuring checkpoint location is fresh (no stale state)

## Delta Table as Streaming Sink

Delta tables are the ideal streaming sink during migration because:
- **ACID transactions**: No partial writes
- **Idempotent writes**: MERGE handles duplicates naturally
- **Time travel**: Can query historical state for validation
- **Schema evolution**: `mergeSchema` handles new fields
- **Optimized writes**: Auto-compaction and Z-ordering

```python
# Best practice: Delta sink with foreachBatch for idempotent writes
def write_to_delta(batch_df, batch_id):
    from delta.tables import DeltaTable
    
    if batch_df.isEmpty():
        return
    
    target = DeltaTable.forName(spark, "catalog.schema.events")
    target.alias("t").merge(
        batch_df.alias("s"),
        "t.event_id = s.event_id AND t.event_date = s.event_date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

query = (
    parsed_df.writeStream
    .foreachBatch(write_to_delta)
    .option("checkpointLocation", "/Volumes/catalog/schema/checkpoints/events-v2")
    .trigger(processingTime="30 seconds")
    .start()
)
```

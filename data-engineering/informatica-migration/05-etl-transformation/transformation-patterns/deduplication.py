"""
Deduplication: Remove Duplicate Records
=======================================

WHAT THIS DOES:
- Identifies and removes duplicate records
- Keeps most recent version based on timestamp or sequence
- Essential for Bronze → Silver transformation

REPLACES IN INFORMATICA:
- Aggregator with MAX(timestamp) and FIRST() for other columns
- Sorter + Filter combination
- SQL Transformation with ROW_NUMBER()

COMMON DEDUP SCENARIOS:
1. Source sends full snapshots daily (keep latest)
2. CDC stream has multiple updates for same key
3. Multiple sources feed same table with overlapping data
4. Replay/reprocessing creates duplicates

DEDUP STRATEGIES:
| Strategy        | Use Case                        | Method                    |
|-----------------|--------------------------------|---------------------------|
| Keep Latest     | Most recent wins               | ROW_NUMBER + ORDER BY DESC|
| Keep First      | First arrival wins             | ROW_NUMBER + ORDER BY ASC |
| Keep All        | Track history                  | No dedup, use SCD2        |
| Merge           | Combine attributes             | GROUP BY + COALESCE       |

PERFORMANCE NOTES:
- Window functions cause shuffle - partition wisely
- For very large datasets, consider incremental dedup
- dropDuplicates() is simpler but less flexible
"""

import dlt
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col, max as spark_max

# =============================================================================
# Pattern 1: Window Function (Most Flexible)
# Keep latest record per customer based on ingestion time
# =============================================================================
@dlt.table(
    name="silver_customer_deduped",
    comment="Deduplicated customer records - latest version only"
)
def silver_customer_deduped():
    """
    Removes duplicates keeping the most recently ingested record.
    Uses window function for precise control over tie-breaking.
    """
    # Define window: partition by key, order by timestamp descending
    window_spec = Window \
        .partitionBy("customer_id") \
        .orderBy(desc("_ingestion_time"))

    return (
        dlt.read_stream("bronze_customer")
        .withColumn("row_num", row_number().over(window_spec))
        .where("row_num = 1")  # Keep only the latest
        .drop("row_num")
    )


# =============================================================================
# Pattern 2: dropDuplicates (Simpler, Less Control)
# Good for exact duplicates or when order doesn't matter
# =============================================================================
@dlt.table(name="silver_events_deduped_simple")
def silver_events_deduped_simple():
    """
    Simple deduplication using dropDuplicates.
    Keeps arbitrary record when duplicates exist (non-deterministic).
    """
    return (
        dlt.read_stream("bronze_events")
        .dropDuplicates(["event_id"])  # Keeps one arbitrary record per event_id
    )


# =============================================================================
# Pattern 3: Dedup with Multiple Sort Keys
# Handle ties with secondary sort columns
# =============================================================================
@dlt.table(name="silver_orders_deduped")
def silver_orders_deduped():
    """
    Dedup with multiple sort keys for deterministic results.
    Primary: latest update time
    Secondary: highest sequence number (for same timestamp)
    """
    window_spec = Window \
        .partitionBy("order_id") \
        .orderBy(
            desc("last_updated"),
            desc("sequence_number")
        )

    return (
        dlt.read_stream("bronze_orders")
        .withColumn("rn", row_number().over(window_spec))
        .where("rn = 1")
        .drop("rn")
    )


# =============================================================================
# Pattern 4: Incremental Dedup with Watermark
# For streaming with late-arriving data
# =============================================================================
@dlt.table(name="silver_transactions_deduped_streaming")
def silver_transactions_deduped_streaming():
    """
    Streaming dedup with watermark for handling late data.
    Records arriving after watermark are dropped.
    """
    return (
        dlt.read_stream("bronze_transactions")
        .withWatermark("event_time", "1 hour")  # Allow 1 hour late data
        .dropDuplicates(["transaction_id", "event_time"])
    )

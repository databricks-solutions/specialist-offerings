"""
Delta Live Tables (DLT): Python Pipeline Definition
====================================================

WHAT THIS DOES:
- Defines declarative data pipeline using Python decorators
- Automatically manages dependencies between tables
- Built-in data quality with expectations
- Auto-retry, monitoring, and lineage tracking

REPLACES IN INFORMATICA:
- Mapping Designer (visual ETL design)
- Workflow Manager (orchestration)
- Session Management (execution config)
- Repository (metadata, lineage)

DLT vs TRADITIONAL SPARK:
| Aspect          | Traditional Spark      | DLT                    |
|-----------------|------------------------|------------------------|
| Style           | Imperative (how)       | Declarative (what)     |
| Dependencies    | Manual orchestration   | Automatic detection    |
| Quality Checks  | Custom code            | Built-in expectations  |
| Monitoring      | Custom dashboards      | Automatic UI           |
| Error Handling  | Manual retry logic     | Automatic retries      |
| Lineage         | Manual tracking        | Automatic via Unity    |

KEY DECORATORS:
- @dlt.table: Creates a materialized table
- @dlt.view: Creates a temporary view (not persisted)
- @dlt.expect: Log violations, continue processing
- @dlt.expect_or_drop: Drop bad rows, continue
- @dlt.expect_or_fail: Stop pipeline on violation

STREAMING vs BATCH:
- dlt.read_stream("table"): Incremental/streaming processing
- dlt.read("table"): Full table read (batch)
"""

import dlt
from pyspark.sql.functions import col, current_timestamp, count, sum, max

# =============================================================================
# SILVER LAYER: Cleaned and validated data
# =============================================================================
@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_country", "country IN ('US', 'CA', 'MX')")  # Warn only
def silver_customers():
    """
    Transforms raw customer data from Bronze:
    - Validates email format and customer ID
    - Removes duplicates by customer_id
    - Adds processing timestamp
    """
    return (
        dlt.read_stream("bronze_customers")  # Incremental processing
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email").cast("string"),
            col("country"),
            current_timestamp().alias("processed_timestamp")
        )
        .dropDuplicates(["customer_id"])
    )


# =============================================================================
# GOLD LAYER: Business-level aggregates
# =============================================================================
@dlt.table(
    name="gold_customer_summary",
    comment="Customer lifetime value and order metrics",
    table_properties={"quality": "gold"}
)
def gold_customer_summary():
    """
    Aggregates customer data for analytics:
    - Joins customers with orders
    - Calculates lifetime value, order counts
    - Ready for BI consumption
    """
    return (
        dlt.read("silver_customers").alias("c")
        .join(dlt.read("silver_orders").alias("o"), "customer_id")
        .groupBy("c.customer_id", "c.first_name", "c.last_name", "c.country")
        .agg(
            count("o.order_id").alias("total_orders"),
            sum("o.order_amount").alias("lifetime_value"),
            max("o.order_date").alias("last_order_date")
        )
    )

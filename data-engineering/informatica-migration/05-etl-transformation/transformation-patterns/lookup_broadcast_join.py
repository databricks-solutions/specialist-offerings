"""
Lookup / Enrichment with Broadcast Join
=======================================

WHAT THIS DOES:
- Enriches transaction data with dimension attributes
- Uses broadcast join for small lookup tables (fits in executor memory)
- Much faster than Informatica's row-by-row lookup

REPLACES IN INFORMATICA:
- Lookup Transformation (LKP)
- Connected/Unconnected lookups
- Cached lookup with static cache

INFORMATICA vs SPARK LOOKUP:
| Aspect          | Informatica Lookup     | Spark Broadcast Join    |
|-----------------|------------------------|-------------------------|
| Processing      | Row-by-row             | Batch (all at once)     |
| Cache           | Per-session cache      | Per-executor broadcast  |
| Performance     | Slower for large data  | Much faster             |
| Memory          | Integration Service    | Executor memory         |

JOIN STRATEGIES IN SPARK:
| Strategy        | When to Use                              |
|-----------------|------------------------------------------|
| Broadcast       | Small dimension (< 10MB default)         |
| Sort-Merge      | Large tables, both sides large           |
| Shuffle Hash    | Medium tables, one side smaller          |

BROADCAST JOIN LIMITS:
- Default max broadcast size: 10MB
- Can increase: spark.sql.autoBroadcastJoinThreshold
- Recommended max: ~100MB per table
- If dimension is larger, use regular join

BEST PRACTICES:
- Always broadcast the smaller table
- Pre-filter dimensions before broadcast if possible
- Monitor executor memory usage
"""

import dlt
from pyspark.sql.functions import broadcast, col, coalesce, lit

# =============================================================================
# Pattern 1: Simple Broadcast Join for Dimension Enrichment
# =============================================================================
@dlt.table(
    name="silver_transactions_enriched",
    comment="Transactions enriched with product and customer details"
)
def silver_transactions_enriched():
    """
    Enriches transactions with product attributes.
    Products table is small, so broadcast for performance.
    """
    transactions = dlt.read_stream("bronze_transactions")
    products = dlt.read("silver_dim_product")  # Small dimension table

    return (
        transactions
        .join(
            broadcast(products),  # Broadcast small table
            "product_id",
            "left"  # Keep all transactions even without product match
        )
        .select(
            transactions["*"],
            products["product_name"],
            products["product_category"],
            products["unit_cost"],
            products["supplier_id"]
        )
    )


# =============================================================================
# Pattern 2: Multiple Dimension Lookups
# =============================================================================
@dlt.table(name="silver_orders_fully_enriched")
def silver_orders_fully_enriched():
    """
    Multiple dimension lookups in single transformation.
    Each dimension is broadcast separately.
    """
    orders = dlt.read_stream("bronze_orders")
    customers = dlt.read("silver_dim_customer")
    products = dlt.read("silver_dim_product")
    stores = dlt.read("silver_dim_store")

    return (
        orders
        # Customer lookup
        .join(broadcast(customers), "customer_id", "left")
        # Product lookup
        .join(broadcast(products), "product_id", "left")
        # Store lookup
        .join(broadcast(stores), "store_id", "left")
        .select(
            # Order facts
            orders["order_id"],
            orders["order_date"],
            orders["quantity"],
            orders["amount"],

            # Customer attributes
            customers["customer_name"],
            customers["customer_segment"],
            customers["customer_tier"],

            # Product attributes
            products["product_name"],
            products["category"],

            # Store attributes
            stores["store_name"],
            stores["region"]
        )
    )


# =============================================================================
# Pattern 3: Lookup with Default Values (Unmatched Handling)
# =============================================================================
@dlt.table(name="silver_sales_with_defaults")
def silver_sales_with_defaults():
    """
    Handle unmatched lookups with default values.
    Similar to Informatica's default value on no match.
    """
    sales = dlt.read_stream("bronze_sales")
    products = dlt.read("silver_dim_product")

    return (
        sales
        .join(broadcast(products), "product_id", "left")
        .select(
            sales["*"],
            # Use COALESCE for default values when lookup fails
            coalesce(products["product_name"], lit("Unknown Product")).alias("product_name"),
            coalesce(products["category"], lit("Uncategorized")).alias("category"),
            coalesce(products["unit_cost"], lit(0.0)).alias("unit_cost")
        )
    )


# =============================================================================
# Pattern 4: Filtered Lookup (Pre-filter Dimension)
# =============================================================================
@dlt.table(name="silver_orders_active_products")
def silver_orders_active_products():
    """
    Pre-filter dimension before broadcast for better performance.
    Only lookup active products.
    """
    orders = dlt.read_stream("bronze_orders")

    # Filter dimension before broadcast
    active_products = (
        dlt.read("silver_dim_product")
        .where("is_active = TRUE AND effective_end_date IS NULL")
        .select("product_id", "product_name", "category", "unit_cost")
    )

    return (
        orders
        .join(broadcast(active_products), "product_id", "left")
    )

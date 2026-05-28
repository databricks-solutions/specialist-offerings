"""
DLT Expectations: Comprehensive Data Quality Framework
======================================================

WHAT THIS DOES:
- Enforces data quality rules at pipeline runtime
- Handles violations with configurable actions (log, drop, fail)
- Provides automatic quality metrics and monitoring

REPLACES IN INFORMATICA:
- Filter transformation for bad record rejection
- Router transformation for conditional routing
- Error handling with reject files
- Pre/post-session SQL for validation

EXPECTATION ACTIONS:
| Action         | Syntax                    | Behavior                    |
|----------------|---------------------------|-----------------------------|
| WARN           | @dlt.expect()             | Log violation, keep row     |
| DROP           | @dlt.expect_or_drop()     | Drop bad rows, continue     |
| FAIL           | @dlt.expect_or_fail()     | Stop entire pipeline        |

WHEN TO USE EACH:
- WARN: Data quality monitoring, non-critical fields (phone format)
- DROP: Critical validations where bad data should be excluded
- FAIL: Zero-tolerance scenarios (e.g., null primary keys)

QUALITY METRICS:
- DLT automatically tracks pass/fail rates for each expectation
- View in DLT pipeline UI → Data Quality tab
- Query programmatically via event log
"""

import dlt
from pyspark.sql.functions import col, length, when

@dlt.table(
    name="silver_orders",
    comment="Validated order data with comprehensive quality checks"
)
# =============================================================================
# CRITICAL: Pipeline fails if violated (zero tolerance)
# =============================================================================
@dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")

# =============================================================================
# IMPORTANT: Drop invalid rows but continue pipeline
# =============================================================================
@dlt.expect_or_drop("valid_amount",
    "order_amount > 0 AND order_amount < 1000000")

@dlt.expect_or_drop("valid_date",
    "order_date >= '2020-01-01' AND order_date <= CURRENT_DATE")

@dlt.expect_or_drop("valid_customer",
    "customer_id IS NOT NULL AND customer_id > 0")

@dlt.expect_or_drop("valid_status",
    "status IN ('PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED')")

# =============================================================================
# MONITORING: Log violations but keep rows (for review)
# =============================================================================
@dlt.expect("valid_email",
    "email IS NULL OR email LIKE '%@%.%'")

@dlt.expect("valid_phone",
    "phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) >= 10")

@dlt.expect("reasonable_quantity",
    "quantity > 0 AND quantity < 1000")

@dlt.expect("has_shipping_address",
    "shipping_address IS NOT NULL")

def silver_orders():
    """
    Transforms bronze orders with comprehensive validation.
    Bad rows are dropped or flagged based on severity.
    """
    return (
        dlt.read_stream("bronze_orders")
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_date"),
            col("order_amount"),
            col("quantity"),
            col("status"),
            col("email"),
            col("phone"),
            col("shipping_address")
        )
    )


# =============================================================================
# Complex multi-column expectations
# =============================================================================
@dlt.table(name="silver_transactions_validated")
@dlt.expect_or_drop("valid_transaction", """
    transaction_id IS NOT NULL
    AND amount BETWEEN 0.01 AND 999999.99
    AND transaction_date >= '2020-01-01'
    AND transaction_date <= CURRENT_DATE
    AND status IN ('COMPLETED', 'PENDING', 'FAILED', 'REVERSED')
    AND (currency IS NULL OR currency IN ('USD', 'EUR', 'GBP', 'CAD'))
""")
@dlt.expect("amount_currency_match", """
    NOT (currency = 'USD' AND amount > 500000)
""")  # Business rule: USD transactions capped at 500k
def silver_transactions_validated():
    return dlt.read_stream("bronze_transactions")

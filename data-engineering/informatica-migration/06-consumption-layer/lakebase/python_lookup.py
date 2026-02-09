"""
Python Application: Query Lakebase for Real-Time Lookups
========================================================

WHAT THIS DOES:
- Connects Python applications to Databricks Online Tables
- Provides fast point lookups for web/mobile backends
- Uses Databricks SQL Connector for optimized connectivity

REPLACES IN INFORMATICA:
- JDBC connections to Oracle/SQL Server
- Custom sync jobs to application databases
- PowerExchange connectors to operational systems

USE CASES:
- Web API backends needing customer profile data
- Mobile apps checking inventory availability
- Microservices requiring low-latency data access
- ML model serving with feature lookups

CONNECTION OPTIONS:
1. databricks-sql-connector (recommended for Python)
2. PyODBC with Databricks ODBC driver
3. REST API for simple lookups

PERFORMANCE TIPS:
- Use connection pooling for high-throughput applications
- Batch multiple lookups when possible
- Consider caching for extremely hot data
- Monitor latency and adjust warehouse size if needed
"""

import os
from databricks import sql
from typing import Optional, Dict, Any
import time

# =============================================================================
# Configuration (use environment variables in production)
# =============================================================================
DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


# =============================================================================
# Basic Connection and Lookup
# =============================================================================
def get_customer_profile(customer_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch customer profile by ID from Online Table.
    Expected latency: < 10ms for point lookup.
    """
    connection = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT
                customer_id,
                customer_name,
                customer_tier,
                lifetime_value,
                credit_limit,
                is_active
            FROM main.online.customer_profile
            WHERE customer_id = ?
        """, (customer_id,))

        row = cursor.fetchone()
        if row:
            return {
                "customer_id": row[0],
                "customer_name": row[1],
                "customer_tier": row[2],
                "lifetime_value": float(row[3]) if row[3] else 0.0,
                "credit_limit": float(row[4]) if row[4] else 0.0,
                "is_active": row[5]
            }
        return None

    finally:
        cursor.close()
        connection.close()


# =============================================================================
# Connection Pooling for High-Throughput Applications
# =============================================================================
class LakebaseClient:
    """
    Reusable client with connection pooling.
    Use for applications making many requests.
    """

    def __init__(self):
        self._connection = None

    def _get_connection(self):
        if self._connection is None:
            self._connection = sql.connect(
                server_hostname=DATABRICKS_HOST,
                http_path=DATABRICKS_HTTP_PATH,
                access_token=DATABRICKS_TOKEN
            )
        return self._connection

    def get_customer(self, customer_id: int) -> Optional[Dict]:
        """Fast customer lookup with reused connection."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT * FROM main.online.customer_profile WHERE customer_id = ?",
                (customer_id,)
            )
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None
        finally:
            cursor.close()

    def check_inventory(self, product_id: int, warehouse_id: int) -> int:
        """Check available inventory for a product."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT available_quantity
                FROM main.online.inventory_availability
                WHERE product_id = ? AND warehouse_id = ?
            """, (product_id, warehouse_id))
            row = cursor.fetchone()
            return row[0] if row else 0
        finally:
            cursor.close()

    def batch_get_customers(self, customer_ids: list) -> list:
        """Batch lookup for multiple customers."""
        if not customer_ids:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            placeholders = ",".join(["?"] * len(customer_ids))
            cursor.execute(f"""
                SELECT *
                FROM main.online.customer_profile
                WHERE customer_id IN ({placeholders})
            """, customer_ids)

            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def close(self):
        """Close connection when done."""
        if self._connection:
            self._connection.close()
            self._connection = None


# =============================================================================
# Example Usage
# =============================================================================
if __name__ == "__main__":
    # Simple lookup
    start = time.time()
    customer = get_customer_profile(12345)
    print(f"Customer: {customer}")
    print(f"Latency: {(time.time() - start) * 1000:.2f}ms")

    # Pooled client for multiple lookups
    client = LakebaseClient()
    try:
        # Multiple lookups reuse connection
        for cust_id in [12345, 12346, 12347]:
            profile = client.get_customer(cust_id)
            print(f"Customer {cust_id}: {profile}")

        # Batch lookup (more efficient for multiple IDs)
        customers = client.batch_get_customers([12345, 12346, 12347, 12348])
        print(f"Batch result: {len(customers)} customers")

        # Inventory check
        qty = client.check_inventory(product_id=1001, warehouse_id=5)
        print(f"Available quantity: {qty}")

    finally:
        client.close()

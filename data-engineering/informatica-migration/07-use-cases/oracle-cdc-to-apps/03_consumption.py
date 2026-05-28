"""
USE CASE 3: Oracle CDC → Application Serving
============================================
STEP 3: CONSUMPTION LAYER SETUP

SCENARIO:
Set up application serving from Gold inventory data.
Two options shown:
A) Write back to SQL Server (transition period)
B) Use Lakebase for modern app serving (recommended)

OLD PATTERN:
  Informatica → SQL Server → Applications (JDBC)

NEW PATTERN:
  Gold Tables → Lakebase → Applications (JDBC/REST)

THIS FILE: Configure consumption options
"""

# =============================================================================
# OPTION A: Write to SQL Server (During Migration Transition)
# =============================================================================
# Use this temporarily while migrating applications to Lakebase
# Remove once all apps are updated to use Lakebase directly

def sync_to_sql_server():
    """
    Sync Gold inventory to SQL Server for legacy applications.
    Run as scheduled job during transition period.
    """
    # Read Gold table
    gold_inventory = spark.table("gold_inventory_current")

    record_count = gold_inventory.count()
    print(f"Syncing {record_count} inventory records to SQL Server")

    # Write to SQL Server (full refresh)
    gold_inventory.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlserver://sqlserver-host:1433;databaseName=INVENTORY_DW") \
        .option("dbtable", "dbo.INVENTORY_CURRENT") \
        .option("user", dbutils.secrets.get("db-scope", "sql-user")) \
        .option("password", dbutils.secrets.get("db-scope", "sql-password")) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    print(f"Sync complete: {record_count} records written to SQL Server")


# =============================================================================
# OPTION B: Configure Lakebase (Modern Approach - Recommended)
# =============================================================================
# Run this once to set up Lakebase, then applications connect directly

def setup_lakebase():
    """
    Create Lakebase Online Table from Gold inventory.
    Applications query this for sub-10ms lookups.
    """

    # Create online table for inventory lookups
    spark.sql("""
        CREATE OR REPLACE TABLE main.online.inventory_availability (
            product_id BIGINT,
            warehouse_id BIGINT,
            product_name STRING,
            warehouse_code STRING,
            available_quantity INT,
            stock_status STRING,
            last_updated TIMESTAMP,
            PRIMARY KEY (product_id, warehouse_id)
        )
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'pipelines.onlineTableEnabled' = 'true',
            'pipelines.onlineTableRefreshInterval' = '1 minute'
        )
    """)

    # Initial data load
    spark.sql("""
        INSERT OVERWRITE main.online.inventory_availability
        SELECT
            product_id,
            warehouse_id,
            product_name,
            warehouse_code,
            available_quantity,
            stock_status,
            last_updated
        FROM gold_inventory_current
    """)

    print("Lakebase setup complete!")
    print("Applications can now query: main.online.inventory_availability")
    print("Expected latency: < 10ms for point lookups")


# =============================================================================
# Application Integration Examples
# =============================================================================

def example_inventory_check_api():
    """
    Example: REST API endpoint for inventory availability check.
    Deploy as Azure Function, AWS Lambda, or Flask app.
    """
    from databricks import sql
    import os

    def check_availability(product_id: int, warehouse_id: int) -> dict:
        """Check inventory availability for a product at a warehouse."""

        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_HOST"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        )

        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT
                    product_name,
                    warehouse_code,
                    available_quantity,
                    stock_status,
                    last_updated
                FROM main.online.inventory_availability
                WHERE product_id = ? AND warehouse_id = ?
            """, (product_id, warehouse_id))

            row = cursor.fetchone()

            if row:
                return {
                    "product_id": product_id,
                    "warehouse_id": warehouse_id,
                    "product_name": row[0],
                    "warehouse_code": row[1],
                    "available_quantity": row[2],
                    "stock_status": row[3],
                    "last_updated": str(row[4]),
                    "is_available": row[2] > 0
                }
            else:
                return {
                    "product_id": product_id,
                    "warehouse_id": warehouse_id,
                    "error": "Product/warehouse combination not found",
                    "is_available": False
                }

        finally:
            cursor.close()
            connection.close()

    # Example usage
    # result = check_availability(product_id=12345, warehouse_id=5)
    # print(result)


# =============================================================================
# Run Setup
# =============================================================================
if __name__ == "__main__":
    # Choose your consumption pattern:

    # Option A: Transition period - sync to SQL Server
    # sync_to_sql_server()

    # Option B: Modern approach - use Lakebase (recommended)
    setup_lakebase()

    print("\n" + "=" * 60)
    print("CONSUMPTION LAYER CONFIGURED")
    print("=" * 60)
    print("Applications can now connect to Databricks for inventory data.")
    print("")
    print("Connection options:")
    print("1. Lakebase (recommended): main.online.inventory_availability")
    print("   - Sub-10ms latency for point lookups")
    print("   - Auto-syncs from Gold every 1 minute")
    print("")
    print("2. SQL Warehouse: gold_inventory_current")
    print("   - For complex queries and analytics")
    print("   - Connect via JDBC/ODBC")
    print("=" * 60)

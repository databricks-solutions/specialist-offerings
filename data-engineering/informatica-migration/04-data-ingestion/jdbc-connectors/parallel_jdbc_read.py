"""
JDBC: Parallel Read from Relational Database
=============================================

WHAT THIS DOES:
- Extracts data from Oracle/SQL Server/PostgreSQL/MySQL via JDBC
- Parallelizes read across multiple Spark executors for performance
- Uses partition column to split the work (typically primary key or date)

REPLACES IN INFORMATICA:
- Relational Source Qualifier with SQL override
- Bulk data movement sessions
- PowerExchange for database extraction

WHEN TO USE:
- Initial historical data loads
- Small to medium databases (< 500GB)
- Batch/scheduled extracts (daily, hourly)
- When CDC is not available or needed

WHEN TO USE CDC INSTEAD (Lakeflow Connect):
- Very large databases (> 500GB)
- Need real-time/continuous sync
- Must capture DELETEs
- Want minimal source system impact

KEY OPTIONS:
- partitionColumn: Column to parallelize on (must be numeric or date)
- lowerBound/upperBound: Range for partition column (doesn't filter data!)
- numPartitions: Number of parallel readers (typically 10-50)
- fetchsize: Rows per network round-trip (tune for performance)

PERFORMANCE TIPS:
- Choose partition column with uniform distribution
- Set numPartitions based on cluster size and source capacity
- Use query pushdown with dbtable option for filtering at source
- Monitor source database load during extraction
"""

# JDBC read with parallelism for large tables
df_inventory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//oracle-host:1521/PRODDB") \
    .option("dbtable", "INVENTORY") \
    .option("user", dbutils.secrets.get("db-scope", "oracle-user")) \
    .option("password", dbutils.secrets.get("db-scope", "oracle-password")) \
    .option("partitionColumn", "INV_ID") \      # Must be numeric
    .option("lowerBound", "1") \                 # Min value (doesn't filter!)
    .option("upperBound", "10000000") \          # Max value (doesn't filter!)
    .option("numPartitions", "20") \             # Parallel readers
    .option("fetchsize", "10000") \              # Rows per fetch
    .load()

# Write to Bronze layer
df_inventory.write \
    .mode("overwrite") \
    .saveAsTable("bronze_inventory")

# --- Alternative: Query pushdown for filtered extraction ---
# df_filtered = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", "(SELECT * FROM INVENTORY WHERE STATUS = 'ACTIVE') t") \
#     .option("numPartitions", "10") \
#     .load()

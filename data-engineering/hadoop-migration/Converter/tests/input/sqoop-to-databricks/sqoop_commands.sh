#!/bin/bash
# Sqoop import/export commands for the retail analytics pipeline

# ============================================================
# IMPORT 1: Full import of customers table (Parquet format)
# ============================================================
sqoop import \
  --connect jdbc:mysql://mysql.example.com:3306/retail_db \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/mysql.password \
  --table customers \
  --target-dir /data/staging/customers/full \
  --as-parquetfile \
  --num-mappers 4 \
  --split-by customer_id \
  --compress \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --delete-target-dir

# ============================================================
# IMPORT 2: Incremental import of orders (append mode)
# ============================================================
sqoop import \
  --connect jdbc:mysql://mysql.example.com:3306/retail_db \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/mysql.password \
  --table orders \
  --target-dir /data/staging/orders \
  --as-parquetfile \
  --incremental append \
  --check-column order_id \
  --last-value 1000000 \
  --num-mappers 8 \
  --split-by order_id \
  --compress \
  --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
  --hive-import \
  --hive-database retail_analytics \
  --hive-table stg_orders \
  --hive-overwrite \
  --create-hive-table

# ============================================================
# IMPORT 3: Incremental import with lastmodified mode
# ============================================================
sqoop import \
  --connect jdbc:oracle:thin:@oracle.example.com:1521:RETAILDB \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/oracle.password \
  --table PRODUCT_CATALOG \
  --target-dir /data/staging/products/${DATE_PARTITION} \
  --as-avrodatafile \
  --incremental lastmodified \
  --check-column UPDATED_AT \
  --last-value "2024-01-01 00:00:00" \
  --num-mappers 4 \
  --split-by PRODUCT_ID \
  --map-column-java "PRODUCT_ID=Long,PRICE=java.math.BigDecimal,UPDATED_AT=java.sql.Timestamp" \
  --boundary-query "SELECT MIN(PRODUCT_ID), MAX(PRODUCT_ID) FROM PRODUCT_CATALOG WHERE UPDATED_AT > '2024-01-01'"

# ============================================================
# IMPORT 4: Free-form query import
# ============================================================
sqoop import \
  --connect jdbc:postgresql://postgres.example.com:5432/analytics \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/postgres.password \
  --query "SELECT o.order_id, o.customer_id, o.order_date, o.total_amount, c.email, c.tier FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_date >= '2024-01-01' AND \$CONDITIONS" \
  --target-dir /data/staging/enriched_orders/${DATE_PARTITION} \
  --as-parquetfile \
  --num-mappers 4 \
  --split-by o.order_id \
  --compress

# ============================================================
# IMPORT 5: Import into Hive with partitioning
# ============================================================
sqoop import \
  --connect jdbc:mysql://mysql.example.com:3306/retail_db \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/mysql.password \
  --table transactions \
  --where "txn_date = '${DATE_PARTITION}'" \
  --target-dir /data/staging/transactions/${DATE_PARTITION} \
  --num-mappers 8 \
  --split-by txn_id \
  --as-parquetfile \
  --hive-import \
  --hive-database retail_analytics \
  --hive-table stg_transactions \
  --hive-partition-key txn_date \
  --hive-partition-value ${DATE_PARTITION}

# ============================================================
# EXPORT 1: Export aggregated results back to MySQL
# ============================================================
sqoop export \
  --connect jdbc:mysql://mysql.example.com:3306/reporting_db \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/mysql.password \
  --table daily_revenue_summary \
  --export-dir /data/processed/daily_revenue/${DATE_PARTITION} \
  --input-fields-terminated-by ',' \
  --num-mappers 4 \
  --batch \
  --update-key "report_date,region" \
  --update-mode allowinsert

# ============================================================
# EXPORT 2: Export with staging table
# ============================================================
sqoop export \
  --connect jdbc:mysql://mysql.example.com:3306/reporting_db \
  --username etl_user \
  --password-file hdfs://namenode:8020/user/etl/passwords/mysql.password \
  --table customer_360_view \
  --staging-table customer_360_view_staging \
  --clear-staging-table \
  --export-dir /data/processed/customer_360 \
  --num-mappers 4 \
  --batch \
  --columns "customer_id,name,email,tier,total_orders,total_spend,last_order_date,updated_at"

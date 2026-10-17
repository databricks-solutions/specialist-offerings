# Plan: Reproducible Cloudera CDH Cluster on AWS EC2

> 📋 **Working document — planning/implementation notes, not user reference.**
> For the test-cluster setup you actually run, see [`cluster-setup/README.md`](cluster-setup/README.md).
> This file records design decisions and build progress and may lag the scripts.

## Context

The Hadoop Migration toolkit (Profiler, Converter, Analyzer) needs a live Cloudera cluster to test against. The previous CDH 5.7.0 QuickStart instance on EC2 (`34.237.53.91`) is gone. We need a reproducible setup script that launches a new cluster, bootstraps it with a realistic retail analytics data pipeline (Hive, Sqoop, Spark, HBase, Oozie), and can be re-run whenever the cluster is torn down.

**AWS Account:** `aws-sandbox-field-eng` (332745928618), using **default IAM profile**
**Region:** `us-east-1`
**Source DB:** MySQL inside CDH QuickStart Docker (pre-installed)
**Data Volume:** ~100K rows (medium — realistic profiler metrics)

---

## Deliverables

A new top-level directory `cluster-setup/` with:

```
cluster-setup/
├── launch-cluster.sh          # Main entry point — creates EC2 + bootstraps everything
├── teardown-cluster.sh        # Stops/terminates the EC2 instance
├── bootstrap/
│   ├── 00-system-setup.sh     # Docker install, ports, hostname
│   ├── 01-start-cdh.sh        # Launch CDH QuickStart Docker container
│   ├── 02-mysql-seed.sh       # Create retail_db in MySQL, seed ~100K rows
│   ├── 03-hive-schema.sh      # Create Hive databases, tables, views
│   ├── 04-hdfs-data.sh        # Generate and load clickstream/log files into HDFS
│   ├── 05-sqoop-import.sh     # Sqoop import from MySQL → Hive/HDFS
│   ├── 06-spark-jobs.sh       # Submit PySpark transformation jobs
│   ├── 07-hive-transforms.sh  # Run HiveQL transformation queries
│   ├── 08-hbase-setup.sh      # Create HBase tables, seed data
│   ├── 09-oozie-workflow.sh   # Deploy and kick off Oozie workflow
│   └── 10-verify.sh           # Validate all services and data are up
├── data/
│   ├── mysql-seed.sql          # DDL + INSERT statements for retail_db
│   ├── clickstream-generator.py # Python script to generate ~100K clickstream events
│   └── log-generator.py        # Python script to generate raw log data
├── hive/
│   ├── create-database.hql
│   ├── create-tables.hql       # Based on Converter/tests/input/hive-ddl-to-uc/hive_schema.hql
│   ├── transform-orders.hql
│   └── build-aggregates.hql
├── spark/
│   ├── clickstream_transform.py  # PySpark version of RetailETLJob
│   └── session_metrics.py        # PySpark daily aggregations
├── oozie/
│   ├── workflow.xml              # Adapted from Converter/tests/input/oozie-to-databricks-workflows/workflow.xml
│   ├── coordinator.xml
│   └── job.properties
├── hbase/
│   └── create-tables.hbase      # Based on Converter/tests/input/hbase-to-databricks/create_tables.hbase
└── sqoop/
    └── import-commands.sh        # Sqoop imports from local MySQL → Hive/HDFS
```

---

## Implementation Steps

### Step 1: `launch-cluster.sh` — EC2 Provisioning

Creates and configures an EC2 instance:

- **AMI:** Amazon Linux 2 (latest)
- **Instance type:** `m5.2xlarge` (8 vCPU, 32 GB RAM) — CDH QuickStart needs ~12GB+ for all services
- **Disk:** 80 GB gp3 EBS
- **Security Group:** Create `cloudera-quickstart-sg` with ports: 22 (SSH), 7180 (CM), 8888 (Hue), 8088 (YARN), 50070 (HDFS NameNode), 18088 (Spark HS), 11000 (Oozie), 60010 (HBase)
- **Key Pair:** Use existing `cloudera-test-key` from `~/.ssh/cloudera-test-key.pem`
- **Tags:** `Name=cloudera-quickstart-cdh`, `Project=hadoop-migration`, `Owner=akshay.amin`
- After launch: wait for instance to be running, get public IP
- SCP the entire `cluster-setup/bootstrap/` + `data/` + `hive/` + `spark/` + `oozie/` + `hbase/` + `sqoop/` to the instance
- SSH in and run the bootstrap scripts sequentially
- Print the final connection details (SSH command, CM URL, Hue URL)
- Save instance ID and IP to `cluster-setup/.cluster-info` for teardown

### Step 2: `bootstrap/00-system-setup.sh` — EC2 Host Setup

- Install Docker on Amazon Linux 2 (`amazon-linux-extras install docker`)
- Start Docker service
- Pull `cloudera/quickstart:latest` image
- Set hostname

### Step 3: `bootstrap/01-start-cdh.sh` — Launch CDH Container

```bash
docker run -d \
  --hostname=quickstart.cloudera \
  --privileged=true \
  --name=cloudera \
  -p 7180:7180 -p 8888:8888 -p 8088:8088 -p 50070:50070 \
  -p 18088:18088 -p 11000:11000 -p 60010:60010 \
  -p 10000:10000 -p 2181:2181 -p 8020:8020 \
  -v /data/bootstrap:/bootstrap \
  cloudera/quickstart /usr/bin/docker-quickstart
```

- Wait for Cloudera Manager to come up (poll `http://localhost:7180` until healthy)
- Start Cloudera Manager service inside container: `/home/cloudera/cloudera-manager --express`
- Verify YARN, Hive, HBase, Oozie, Spark services are running

### Step 4: `bootstrap/02-mysql-seed.sh` — Source Database

MySQL comes pre-installed in CDH QuickStart. Create the retail source database:

- **Database:** `retail_db`
- **Tables (matching the Sqoop test inputs):**
  - `customers` (~20K rows) — customer_id, first_name, last_name, email, phone, tier, lifetime_value, address fields, created_date, updated_date, is_active
  - `orders` (~50K rows) — order_id, customer_id, product_id, order_date, quantity, unit_price, discount, total_amount, status, payment_method
  - `transactions` (~30K rows) — txn_id, customer_id, txn_date, amount, source
  - `product_catalog` (~1K rows) — product_id, sku, name, category, subcategory, brand, price, updated_at
- **Database:** `reporting_db`
  - `daily_revenue_summary` (empty target for Sqoop export)
  - `customer_360_view` + `customer_360_view_staging` (empty targets)
- Password file for Sqoop: write to HDFS at `/user/etl/passwords/mysql.password`
- **Source:** `cluster-setup/data/mysql-seed.sql`

### Step 5: `bootstrap/03-hive-schema.sh` — Hive Metastore

Run HiveQL scripts inside the container to create the schema from `Converter/tests/input/hive-ddl-to-uc/hive_schema.hql` (adapted for localhost):

- Create database `retail_analytics`
- Create tables: `raw_clickstream`, `dim_customers`, `fact_orders`, `product_catalog`, `raw_logs`
- Create view: `vw_active_customers`
- Create additional tables needed for transformations: `monthly_revenue`, `customer_monthly_summary`, `enriched_sessions`, `daily_session_aggregates`, `stg_orders`, `stg_transactions`
- Add partitions for `fact_orders` (2024 months 1-12)
- **Source:** `cluster-setup/hive/create-database.hql`, `create-tables.hql`

### Step 6: `bootstrap/04-hdfs-data.sh` — File-Based Data in HDFS

Generate and load:

- **Clickstream JSON files** (~50K events) → `/data/raw/clickstream/{date}/` partitioned by date and hour
  - Generated by `data/clickstream-generator.py` using Python's `random` + `json`
  - Fields: session_id, user_id, event_type, page_url, referrer_url, user_agent, ip_address, event_timestamp, properties
- **Raw log files** (~10K lines) → `/data/raw/logs/{date}/`
  - Generated by `data/log-generator.py`
  - Apache-style access logs
- Create HDFS directory structure:
  ```
  /data/raw/clickstream/
  /data/raw/orders/
  /data/raw/logs/
  /data/staging/customers/
  /data/staging/orders/
  /data/staging/products/
  /data/staging/transactions/
  /data/processed/
  /data/reports/
  /user/etl/passwords/
  /user/etl/workflows/retail-etl/
  ```
- Run `ALTER TABLE raw_clickstream ADD PARTITION` for each generated date partition
- Run `ALTER TABLE raw_logs ADD PARTITION` for each generated date partition

### Step 7: `bootstrap/05-sqoop-import.sh` — Sqoop Imports

Adapted from `Converter/tests/input/sqoop-to-databricks/sqoop_commands.sh` to use localhost MySQL:

- Full import of `customers` → `/data/staging/customers/full` (Parquet)
- Incremental append of `orders` → Hive table `stg_orders`
- Import `transactions` with Hive partitioning → `stg_transactions`
- These generate YARN MapReduce jobs that the Profiler will see

### Step 8: `bootstrap/06-spark-jobs.sh` — PySpark Transformations

PySpark equivalents of the Scala `RetailETLJob`:

- `clickstream_transform.py`:
  - Read raw clickstream JSON from HDFS
  - Filter corrupt records, enrich with page categories
  - Compute session metrics (window functions)
  - Join with dim_customers from Hive
  - Write enriched sessions to Hive table `enriched_sessions`
- `session_metrics.py`:
  - Read enriched sessions
  - Compute daily aggregates by segment
  - Write to Hive table `daily_session_aggregates`
- Submit via `spark-submit --master yarn --deploy-mode client` (cluster mode needs more config on QuickStart)

### Step 9: `bootstrap/07-hive-transforms.sh` — HiveQL Transformations

Run representative HiveQL queries from `Converter/tests/input/hive-sql-to-spark-sql/hive_queries.hql`:

- INSERT OVERWRITE into `monthly_revenue` with GROUPING SETS
- INSERT into `customer_monthly_summary`
- Run the LATERAL VIEW explode query
- Run customer segmentation query
- These generate additional YARN MapReduce jobs

### Step 10: `bootstrap/08-hbase-setup.sh` — HBase Tables

Adapted from `Converter/tests/input/hbase-to-databricks/create_tables.hbase`:

- Create tables: `customers`, `products`, `transactions`, `events`
- Seed each with sample data (~100 rows per table using HBase shell `put` commands)
- Verify with `scan` and `count`

### Step 11: `bootstrap/09-oozie-workflow.sh` — Oozie Pipeline

Adapted from `Converter/tests/input/oozie-to-databricks-workflows/workflow.xml`:

- Upload workflow.xml, coordinator.xml, job.properties to HDFS `/user/etl/workflows/retail-etl/`
- Upload helper scripts (ingest.sh, export_reports.sh) — simplified versions that just touch output files
- Submit the Oozie workflow: `oozie job -config job.properties -run`
- This creates Oozie-launched YARN jobs (visible to profiler as OOZIE_LAUNCHER application types)

### Step 12: `bootstrap/10-verify.sh` — Validation

Check everything is working:
- YARN: `curl http://localhost:8088/ws/v1/cluster/apps` — verify apps exist
- Hive: `beeline -e "SHOW TABLES IN retail_analytics"` — verify tables
- HBase: `echo "status 'simple'" | hbase shell` — verify tables
- HDFS: `hdfs dfs -ls /data/raw/clickstream/` — verify files
- CM: `curl http://localhost:7180/api/version` — verify CM is up
- Oozie: `oozie jobs` — verify workflow ran
- Print summary: service URLs, app counts, table counts

### Step 13: `teardown-cluster.sh`

- Read `.cluster-info` for instance ID
- Terminate EC2 instance
- Optionally delete security group
- Clean up `.cluster-info`

---

## Files to Reuse from Existing Codebase

| Existing File | Reuse As |
|---|---|
| `Converter/tests/input/hive-ddl-to-uc/hive_schema.hql` | Base for `cluster-setup/hive/create-tables.hql` (adapt HDFS paths to localhost) |
| `Converter/tests/input/oozie-to-databricks-workflows/workflow.xml` | Base for `cluster-setup/oozie/workflow.xml` (simplify for QuickStart) |
| `Converter/tests/input/oozie-to-databricks-workflows/coordinator.xml` | Base for `cluster-setup/oozie/coordinator.xml` |
| `Converter/tests/input/sqoop-to-databricks/sqoop_commands.sh` | Base for `cluster-setup/sqoop/import-commands.sh` (change to localhost MySQL) |
| `Converter/tests/input/spark-to-databricks/RetailETLJob.scala` | Rewrite as PySpark in `cluster-setup/spark/clickstream_transform.py` |
| `Converter/tests/input/hive-sql-to-spark-sql/hive_queries.hql` | Base for `cluster-setup/hive/transform-orders.hql` and `build-aggregates.hql` |
| `Converter/tests/input/hbase-to-databricks/create_tables.hbase` | Copy to `cluster-setup/hbase/create-tables.hbase` |
| `Profiler/profiler.conf` | Update with new EC2 IP after launch |

---

## Verification Plan

After `launch-cluster.sh` completes:

1. **SSH into the instance** and verify Docker container is running
2. **Cloudera Manager** at `http://<IP>:7180` — all 13+ services green
3. **Run the Profiler** against the new cluster:
   ```bash
   cd Profiler && ./profiler.sh mySecretKey
   ```
   Verify output has YARN apps (MapReduce from Sqoop + Hive, Spark apps, Oozie launchers)
4. **Run the DuckDB exporter** against profiler output — verify counts exceed the previous baseline (156 YARN apps)
5. **Hue** at `http://<IP>:8888` — browse Hive tables, query data
6. **YARN UI** at `http://<IP>:8088` — see completed applications
7. **Re-run test:** Terminate instance, re-run `launch-cluster.sh`, verify identical result

---

## Estimated File Count

- **New files:** ~25 (scripts, SQL, Python generators, HQL, XML)
- **Modified files:** 1 (`Profiler/profiler.conf` — update IP after launch)

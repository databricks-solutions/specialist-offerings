# Examples: HiveQL → Databricks SQL

## Example 1: ETL Script with SET Commands

### Before (HiveQL)
```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;
SET mapreduce.job.reduces=20;

USE analytics;

INSERT OVERWRITE TABLE daily_summary PARTITION (dt)
SELECT
    customer_id,
    product_category,
    SUM(amount) as total_amount,
    COUNT(*) as transaction_count,
    dt
FROM transactions
WHERE dt = '${hivevar:process_date}'
GROUP BY customer_id, product_category, dt;
```

### After (Databricks SQL)
```sql
-- SET commands removed: dynamic partitions and parallelism are default in Databricks
SET spark.sql.shuffle.partitions = 20;

INSERT OVERWRITE main.analytics.daily_summary
SELECT
    customer_id,
    product_category,
    SUM(amount) as total_amount,
    COUNT(*) as transaction_count,
    dt
FROM main.analytics.transactions
WHERE dt = '${process_date}'
GROUP BY customer_id, product_category, dt;
-- Note: ${hivevar:x} → ${x} (Databricks widgets or job parameters)
```

## Example 2: LATERAL VIEW + UDF

### Before (HiveQL)
```sql
ADD JAR hdfs:///jars/custom-udfs.jar;
CREATE TEMPORARY FUNCTION parse_tags AS 'com.example.ParseTagsUDF';

SELECT
    event_id,
    tag,
    user_id,
    get_json_object(payload, '$.action') as action
FROM events
LATERAL VIEW explode(parse_tags(raw_tags)) t AS tag
WHERE event_date >= '2024-01-01';
```

### After (Databricks SQL)
```sql
-- Replace Java UDF with SQL function
CREATE OR REPLACE FUNCTION parse_tags(raw STRING)
RETURNS ARRAY<STRING>
RETURN split(trim(raw), ',');

SELECT
    event_id,
    tag,
    user_id,
    get_json_object(payload, '$.action') as action
FROM main.default.events
LATERAL VIEW explode(parse_tags(raw_tags)) t AS tag
WHERE event_date >= '2024-01-01';
```

## Example 3: Multi-INSERT with TRANSFORM

### Before (HiveQL)
```sql
FROM (
    SELECT TRANSFORM(line)
    USING 'python /scripts/parse_log.py'
    AS (host STRING, path STRING, status INT, bytes INT, ts STRING)
    FROM raw_apache_logs
) parsed
INSERT OVERWRITE TABLE access_by_host
SELECT host, count(*), sum(bytes) GROUP BY host
INSERT OVERWRITE TABLE access_by_status
SELECT status, count(*) GROUP BY status
INSERT OVERWRITE TABLE access_by_hour
SELECT hour(ts), count(*) GROUP BY hour(ts);
```

### After (Databricks SQL)
```sql
-- Replace TRANSFORM with Python UDF
CREATE OR REPLACE FUNCTION parse_apache_log(line STRING)
RETURNS STRUCT<host: STRING, path: STRING, status: INT, bytes: INT, ts TIMESTAMP>
LANGUAGE PYTHON
AS $$
    import re
    from datetime import datetime
    pattern = r'(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) \S+" (\d+) (\d+)'
    m = re.match(pattern, line)
    if not m:
        return None
    return {
        "host": m.group(1),
        "path": m.group(4),
        "status": int(m.group(5)),
        "bytes": int(m.group(6)),
        "ts": datetime.strptime(m.group(2), "%d/%b/%Y:%H:%M:%S %z")
    }
$$;

-- Split multi-INSERT into separate statements
CREATE OR REPLACE TEMP VIEW parsed_logs AS
SELECT parse_apache_log(line).* FROM main.raw.apache_logs;

INSERT OVERWRITE main.analytics.access_by_host
SELECT host, count(*) as cnt, sum(bytes) as total_bytes
FROM parsed_logs GROUP BY host;

INSERT OVERWRITE main.analytics.access_by_status
SELECT status, count(*) as cnt
FROM parsed_logs GROUP BY status;

INSERT OVERWRITE main.analytics.access_by_hour
SELECT hour(ts) as hr, count(*) as cnt
FROM parsed_logs GROUP BY hour(ts);
```

## Example 4: Complex Hive Script

### Before (HiveQL)
```sql
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=50000000;

CREATE TEMPORARY TABLE staging AS
SELECT
    o.order_id,
    o.customer_id,
    c.segment,
    o.amount,
    o.order_date,
    p.category
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-03-31';

INSERT OVERWRITE TABLE quarterly_report PARTITION (quarter='Q1-2024')
SELECT
    segment,
    category,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_orders,
    SUM(amount) as revenue,
    percentile_approx(amount, 0.5) as median_order
FROM staging
GROUP BY segment, category;

DROP TABLE staging;
```

### After (Databricks SQL)
```sql
-- SET commands removed (auto broadcast join is default, threshold configurable if needed)

CREATE OR REPLACE TEMP VIEW staging AS
SELECT
    o.order_id,
    o.customer_id,
    c.segment,
    o.amount,
    o.order_date,
    p.category
FROM main.sales.orders o
JOIN main.sales.customers c ON o.customer_id = c.id
JOIN main.sales.products p ON o.product_id = p.id
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-03-31';

INSERT OVERWRITE main.analytics.quarterly_report
SELECT
    segment,
    category,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_orders,
    SUM(amount) as revenue,
    percentile_approx(amount, 0.5) as median_order,
    'Q1-2024' as quarter
FROM staging
GROUP BY segment, category;

-- DROP TABLE replaced: temp view is auto-cleaned
```

# HiveQL → Spark SQL Syntax Rules

## Compatible Syntax (No Changes Needed)

These HiveQL constructs work identically in Spark SQL:
- Standard SELECT, WHERE, GROUP BY, HAVING, ORDER BY
- JOIN types (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- Window functions (ROW_NUMBER, RANK, LEAD, LAG, etc.)
- Common aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- CASE/WHEN expressions
- UNION ALL / UNION
- Subqueries (correlated and non-correlated)
- CTE (WITH clause)
- LIKE, RLIKE, IN, BETWEEN, IS NULL

## Syntax That Needs Conversion

### DISTRIBUTE BY / SORT BY / CLUSTER BY

```sql
-- HiveQL
SELECT * FROM events DISTRIBUTE BY user_id SORT BY event_time;

-- Spark SQL (equivalent)
SELECT * FROM events DISTRIBUTE BY user_id SORT BY event_time;
-- Note: These actually work in Spark SQL too, but for Databricks SQL:
SELECT * FROM events ORDER BY user_id, event_time;
-- Or use window functions for precise control
```

### TRANSFORM / MAP / REDUCE (Streaming)

```sql
-- HiveQL (TRANSFORM with external script)
SELECT TRANSFORM(line)
USING 'python parse_log.py'
AS (host STRING, path STRING, status INT)
FROM raw_logs;

-- Databricks SQL: Use Python UDF instead
CREATE OR REPLACE FUNCTION parse_log(line STRING)
RETURNS STRUCT<host: STRING, path: STRING, status: INT>
LANGUAGE PYTHON
AS $$
    parts = line.split(" ")
    return {"host": parts[0], "path": parts[6], "status": int(parts[8])}
$$;

SELECT parse_log(line).* FROM raw_logs;
```

### LATERAL VIEW + EXPLODE

```sql
-- HiveQL
SELECT id, tag
FROM events
LATERAL VIEW explode(tags) t AS tag;

-- Spark SQL (same syntax works, but preferred alternative):
SELECT id, tag
FROM events
LATERAL VIEW explode(tags) t AS tag;

-- Or modern Spark SQL:
SELECT id, explode(tags) AS tag
FROM events;
```

### LATERAL VIEW OUTER

```sql
-- HiveQL
SELECT id, tag
FROM events
LATERAL VIEW OUTER explode(tags) t AS tag;

-- Spark SQL
SELECT id, tag
FROM events
LATERAL VIEW OUTER explode(tags) t AS tag;
-- Works in Spark SQL; for Databricks SQL also consider:
SELECT id, tag
FROM events LEFT JOIN LATERAL explode(tags) AS t(tag);
```

### INSERT OVERWRITE

```sql
-- HiveQL (static partition)
INSERT OVERWRITE TABLE results PARTITION (year=2024, month=1)
SELECT id, value FROM staging WHERE year=2024 AND month=1;

-- Spark SQL / Databricks SQL
INSERT OVERWRITE TABLE main.default.results
PARTITION (year=2024, month=1)
SELECT id, value FROM main.default.staging WHERE year=2024 AND month=1;

-- Or use dynamic partitions (preferred with Delta):
INSERT OVERWRITE main.default.results
SELECT id, value, year, month FROM main.default.staging;
```

### INSERT OVERWRITE DIRECTORY

```sql
-- HiveQL
INSERT OVERWRITE DIRECTORY '/output/results'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM results;

-- Databricks (use DataFrame API or COPY INTO)
-- In a notebook:
-- spark.sql("SELECT * FROM main.default.results") \
--     .write.mode("overwrite").csv("/Volumes/main/default/output/results")
```

### SET Commands

```sql
-- HiveQL
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapreduce.job.reduces=10;
SET hive.exec.parallel=true;

-- Databricks SQL
-- Most Hive SET commands can be removed; defaults are already optimal
SET spark.sql.shuffle.partitions = 10;  -- replaces mapreduce.job.reduces
-- Dynamic partitions enabled by default in Databricks
-- Parallel execution handled by Databricks runtime
```

### SET Variable Mapping

| Hive SET | Databricks Equivalent | Notes |
|----------|----------------------|-------|
| `hive.exec.dynamic.partition=true` | Remove | Default in Databricks |
| `hive.exec.dynamic.partition.mode=nonstrict` | Remove | Default in Databricks |
| `hive.exec.parallel=true` | Remove | Handled by runtime |
| `mapreduce.job.reduces=N` | `spark.sql.shuffle.partitions=N` | Approximate equivalent |
| `hive.auto.convert.join=true` | Remove | Spark handles automatically |
| `hive.mapjoin.smalltable.filesize` | `spark.sql.autoBroadcastJoinThreshold` | Size in bytes |
| `hive.exec.compress.output=true` | Remove | Delta handles compression |
| `hive.merge.mapfiles=true` | Remove | Auto Optimize handles this |
| `hive.merge.mapredfiles=true` | Remove | Auto Optimize handles this |

### TABLESAMPLE

```sql
-- HiveQL
SELECT * FROM large_table TABLESAMPLE(10 PERCENT);
SELECT * FROM large_table TABLESAMPLE(1000 ROWS);

-- Spark SQL
SELECT * FROM main.default.large_table TABLESAMPLE(10 PERCENT);
SELECT * FROM main.default.large_table TABLESAMPLE(1000 ROWS);
-- Both work in Spark SQL
```

### EXPLAIN

```sql
-- HiveQL
EXPLAIN SELECT * FROM events WHERE date > '2024-01-01';

-- Databricks SQL (same syntax, richer output)
EXPLAIN SELECT * FROM main.default.events WHERE date > '2024-01-01';
-- Or for more detail:
EXPLAIN EXTENDED SELECT * FROM main.default.events WHERE date > '2024-01-01';
```

### Multi-INSERT

```sql
-- HiveQL
FROM events
INSERT OVERWRITE TABLE events_summary
SELECT event_type, count(*) GROUP BY event_type
INSERT OVERWRITE TABLE events_daily
SELECT date, count(*) GROUP BY date;

-- Spark SQL (split into separate statements)
INSERT OVERWRITE TABLE main.default.events_summary
SELECT event_type, count(*) FROM main.default.events GROUP BY event_type;

INSERT OVERWRITE TABLE main.default.events_daily
SELECT date, count(*) FROM main.default.events GROUP BY date;
```

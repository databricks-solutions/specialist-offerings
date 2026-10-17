# API Migration: HBase Java API → Lakebase (SQL / JDBC / psycopg2)

## Connection Setup

```java
// Before (HBase)
Configuration conf = HBaseConfiguration.create();
conf.set("hbase.zookeeper.quorum", "zk1,zk2,zk3");
conf.set("hbase.zookeeper.property.clientPort", "2181");
Connection connection = ConnectionFactory.createConnection(conf);
Table table = connection.getTable(TableName.valueOf("users"));
```

```python
# After (Lakebase via psycopg2)
import psycopg2

conn = psycopg2.connect(
    host="<lakebase-host>",
    port=5432,
    dbname="<database>",
    user=dbutils.secrets.get("scope", "lakebase-user"),
    password=dbutils.secrets.get("scope", "lakebase-pass")
)
cursor = conn.cursor()
```

```java
// After (Lakebase via JDBC — for Java apps)
Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://<lakebase-host>:5432/<database>",
    user, password
);
PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users WHERE user_id = ?");
```

```sql
-- After (Databricks SQL — via Lakehouse Federation)
-- No connection code needed; query directly:
SELECT * FROM lakebase_catalog.public.users WHERE user_id = 'user_123';
```

## PUT (Write Single Row) → INSERT / UPSERT

```java
// HBase
Put put = new Put(Bytes.toBytes("user_123"));
put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Alice"));
put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes("alice@example.com"));
table.put(put);
```

```sql
-- Lakebase — Insert
INSERT INTO users (user_id, name, email)
VALUES ('user_123', 'Alice', 'alice@example.com');

-- Lakebase — Upsert (HBase Put overwrites, so this is the exact equivalent)
INSERT INTO users (user_id, name, email)
VALUES ('user_123', 'Alice', 'alice@example.com')
ON CONFLICT (user_id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email;
```

```python
# Lakebase via psycopg2
cursor.execute("""
    INSERT INTO users (user_id, name, email)
    VALUES (%s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email
""", ('user_123', 'Alice', 'alice@example.com'))
conn.commit()
```

## GET (Read Single Row) → SELECT by Primary Key

```java
// HBase
Get get = new Get(Bytes.toBytes("user_123"));
get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
Result result = table.get(get);
String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
```

```sql
-- Lakebase — Point lookup (sub-millisecond on primary key)
SELECT name FROM users WHERE user_id = 'user_123';
```

```python
# Lakebase via psycopg2
cursor.execute("SELECT name FROM users WHERE user_id = %s", ('user_123',))
row = cursor.fetchone()
name = row[0] if row else None
```

## SCAN (Range Scan) → SELECT with WHERE

```java
// HBase — prefix scan
Scan scan = new Scan();
scan.setRowPrefixFilter(Bytes.toBytes("user_"));
scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
scan.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("login_count"));
ResultScanner scanner = table.getScanner(scan);
for (Result result : scanner) {
    String rowKey = Bytes.toString(result.getRow());
    String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
    long logins = Bytes.toLong(result.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("login_count")));
}
```

```sql
-- Lakebase — equivalent prefix scan
SELECT user_id, name, login_count
FROM users
WHERE user_id LIKE 'user_%';
```

```java
// HBase — range scan
Scan scan = new Scan();
scan.withStartRow(Bytes.toBytes("user_100"));
scan.withStopRow(Bytes.toBytes("user_200"));
```

```sql
-- Lakebase — range query
SELECT * FROM users
WHERE user_id >= 'user_100' AND user_id < 'user_200';
```

## DELETE → DELETE

```java
// HBase — delete row
Delete delete = new Delete(Bytes.toBytes("user_123"));
table.delete(delete);

// HBase — delete specific column (cell)
Delete delete = new Delete(Bytes.toBytes("user_123"));
delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("phone"));
table.delete(delete);
```

```sql
-- Lakebase — delete row
DELETE FROM users WHERE user_id = 'user_123';

-- Lakebase — null out a column (equivalent of deleting a cell)
UPDATE users SET phone = NULL WHERE user_id = 'user_123';
```

## SCAN with Filters → SQL WHERE

| HBase Filter | SQL Equivalent |
|---|---|
| `SingleColumnValueFilter(cf, col, =, val)` | `WHERE col = val` |
| `SingleColumnValueFilter(cf, col, >, val)` | `WHERE col > val` |
| `PrefixFilter(prefix)` | `WHERE pk LIKE 'prefix%'` |
| `RowFilter(=, RegexString)` | `WHERE pk ~ 'pattern'` (Postgres regex) |
| `ColumnRangeFilter(min, max)` | SELECT specific columns |
| `PageFilter(N)` | `LIMIT N` |
| `FirstKeyOnlyFilter` | `SELECT DISTINCT ON (pk) pk` |
| `FilterList(AND, [f1, f2])` | `WHERE cond1 AND cond2` |
| `FilterList(OR, [f1, f2])` | `WHERE cond1 OR cond2` |

## Batch Operations → Bulk INSERT / UPSERT

```java
// HBase — batch put
List<Put> puts = new ArrayList<>();
for (Record record : records) {
    Put put = new Put(Bytes.toBytes(record.getId()));
    put.addColumn(...);
    puts.add(put);
}
table.put(puts);
```

```sql
-- Lakebase — bulk upsert
INSERT INTO users (user_id, name, email) VALUES
    ('user_001', 'Alice', 'alice@example.com'),
    ('user_002', 'Bob', 'bob@example.com'),
    ('user_003', 'Carol', 'carol@example.com')
ON CONFLICT (user_id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email;
```

```python
# Lakebase via psycopg2 — bulk insert with executemany
from psycopg2.extras import execute_values

data = [
    ('user_001', 'Alice', 'alice@example.com'),
    ('user_002', 'Bob', 'bob@example.com'),
    ('user_003', 'Carol', 'carol@example.com'),
]
execute_values(cursor, """
    INSERT INTO users (user_id, name, email) VALUES %s
    ON CONFLICT (user_id) DO UPDATE SET
        name = EXCLUDED.name, email = EXCLUDED.email
""", data)
conn.commit()
```

## Increment → UPDATE with Arithmetic

```java
// HBase — atomic increment
Increment inc = new Increment(Bytes.toBytes("user_123"));
inc.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("page_views"), 1);
table.increment(inc);
```

```sql
-- Lakebase — atomic update (Postgres row-level locking guarantees atomicity)
UPDATE users SET page_views = page_views + 1 WHERE user_id = 'user_123';
```

## CheckAndPut → INSERT ... ON CONFLICT / UPDATE ... WHERE

```java
// HBase — conditional write
Put put = new Put(Bytes.toBytes("user_123"));
put.addColumn(b("info"), b("status"), b("active"));
table.checkAndPut(Bytes.toBytes("user_123"), b("info"), b("status"), b("pending"), put);
```

```sql
-- Lakebase — conditional update
UPDATE users SET status = 'active' WHERE user_id = 'user_123' AND status = 'pending';
-- Returns row count = 0 if condition not met (equivalent of checkAndPut returning false)
```

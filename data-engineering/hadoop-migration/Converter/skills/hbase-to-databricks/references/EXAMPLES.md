# Examples: HBase → Lakebase (Managed Postgres)

## Example 1: User Profile Table

### Before (HBase Shell)
```
create 'user_profiles',
  {NAME => 'info', VERSIONS => 1, TTL => 0, COMPRESSION => 'SNAPPY'},
  {NAME => 'metrics', VERSIONS => 1, TTL => 2592000, COMPRESSION => 'SNAPPY'},
  {NAME => 'prefs', VERSIONS => 1, TTL => 0}

# Row key: user_id (e.g., "U00012345")
put 'user_profiles', 'U00012345', 'info:name', 'Alice Johnson'
put 'user_profiles', 'U00012345', 'info:email', 'alice@example.com'
put 'user_profiles', 'U00012345', 'metrics:logins', '142'
put 'user_profiles', 'U00012345', 'metrics:last_seen', '2024-03-15T10:30:00'
put 'user_profiles', 'U00012345', 'prefs:theme', 'dark'
```

### After (Lakebase)
```sql
-- Table DDL
CREATE TABLE user_profiles (
    user_id TEXT PRIMARY KEY,         -- HBase row key
    -- info CF columns
    name TEXT,
    email TEXT,
    -- metrics CF columns (previously had TTL=30 days)
    logins INTEGER DEFAULT 0,
    last_seen TIMESTAMPTZ,
    -- prefs CF columns
    theme TEXT DEFAULT 'light'
);

-- Insert equivalent
INSERT INTO user_profiles (user_id, name, email, logins, last_seen, theme)
VALUES ('U00012345', 'Alice Johnson', 'alice@example.com', 142, '2024-03-15T10:30:00Z', 'dark');

-- Scheduled cleanup for metrics TTL (run daily via Databricks Job)
-- DELETE FROM user_profiles
-- WHERE last_seen < NOW() - INTERVAL '30 days' AND name IS NULL;
```

## Example 2: Time-Series IoT Data

### Before (HBase)
```
# Table: sensor_data
# Row key: device_id + reversed_timestamp (for newest-first scans)
# CF: readings — columns: temperature, humidity, pressure, battery

create 'sensor_data',
  {NAME => 'readings', VERSIONS => 1, TTL => 7776000, COMPRESSION => 'SNAPPY',
   BLOOMFILTER => 'ROW'}
```

```java
// Write sensor reading
long reversedTs = Long.MAX_VALUE - System.currentTimeMillis();
String rowKey = deviceId + "|" + String.format("%019d", reversedTs);
Put put = new Put(Bytes.toBytes(rowKey));
put.addColumn(Bytes.toBytes("readings"), Bytes.toBytes("temperature"), Bytes.toBytes(temp));
put.addColumn(Bytes.toBytes("readings"), Bytes.toBytes("humidity"), Bytes.toBytes(humidity));
table.put(put);

// Read latest 100 readings for a device
Scan scan = new Scan();
scan.setRowPrefixFilter(Bytes.toBytes(deviceId + "|"));
scan.setMaxResultSize(100);
```

### After (Lakebase)
```sql
-- Table design: no reversed timestamps needed
CREATE TABLE sensor_data (
    device_id TEXT NOT NULL,
    reading_time TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    battery DOUBLE PRECISION,
    PRIMARY KEY (device_id, reading_time)
);

-- Descending index for newest-first queries (replaces reversed timestamp hack)
CREATE INDEX idx_sensor_latest ON sensor_data (device_id, reading_time DESC);

-- Write sensor reading
INSERT INTO sensor_data (device_id, reading_time, temperature, humidity)
VALUES ('sensor_001', NOW(), 23.5, 65.2);

-- Read latest 100 readings (sub-ms with index)
SELECT * FROM sensor_data
WHERE device_id = 'sensor_001'
ORDER BY reading_time DESC
LIMIT 100;

-- TTL cleanup (run via scheduled Databricks Job)
DELETE FROM sensor_data WHERE reading_time < NOW() - INTERVAL '90 days';
```

## Example 3: HBase Java Application → Python with psycopg2

### Before (HBase Java)
```java
public class CustomerLookupService {
    private Table table;

    public CustomerLookupService() {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf("customers"));
    }

    public Customer getCustomer(String customerId) {
        Get get = new Get(Bytes.toBytes(customerId));
        Result result = table.get(get);
        return new Customer(
            Bytes.toString(result.getValue(b("info"), b("name"))),
            Bytes.toString(result.getValue(b("info"), b("email"))),
            Bytes.toString(result.getValue(b("info"), b("segment")))
        );
    }

    public List<Customer> getCustomersBySegment(String segment) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.setFilter(new SingleColumnValueFilter(
            b("info"), b("segment"), CompareOp.EQUAL, b(segment)));

        List<Customer> customers = new ArrayList<>();
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            customers.add(parseCustomer(r));
        }
        return customers;
    }
}
```

### After (Python with psycopg2)
```python
import psycopg2
from psycopg2.extras import RealDictCursor


class CustomerLookupService:
    """Customer lookup using Lakebase (managed Postgres)."""

    def __init__(self, conn_params: dict):
        self.conn = psycopg2.connect(**conn_params)

    def get_customer(self, customer_id: str) -> dict | None:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT name, email, segment FROM customers WHERE customer_id = %s",
                (customer_id,)
            )
            return cur.fetchone()

    def get_customers_by_segment(self, segment: str) -> list[dict]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT customer_id, name, email, segment FROM customers WHERE segment = %s",
                (segment,)
            )
            return cur.fetchall()


# Usage
service = CustomerLookupService({
    "host": "<lakebase-host>",
    "port": 5432,
    "dbname": "mydb",
    "user": "user",
    "password": "pass"
})
customer = service.get_customer("CUST001")
premium_customers = service.get_customers_by_segment("premium")
```

```sql
-- Lakebase table DDL
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    segment TEXT
);

-- Index for segment lookups (replacing HBase scan + filter)
CREATE INDEX idx_customers_segment ON customers (segment);
```

### After (Java with JDBC — if keeping Java)
```java
public class CustomerLookupService {
    private final DataSource ds;

    public CustomerLookupService(DataSource ds) {
        this.ds = ds;
    }

    public Customer getCustomer(String customerId) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT name, email, segment FROM customers WHERE customer_id = ?")) {
            stmt.setString(1, customerId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return new Customer(rs.getString("name"), rs.getString("email"), rs.getString("segment"));
            }
            return null;
        }
    }

    public List<Customer> getCustomersBySegment(String segment) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT customer_id, name, email, segment FROM customers WHERE segment = ?")) {
            stmt.setString(1, segment);
            ResultSet rs = stmt.executeQuery();
            List<Customer> customers = new ArrayList<>();
            while (rs.next()) {
                customers.add(new Customer(
                    rs.getString("name"), rs.getString("email"), rs.getString("segment")));
            }
            return customers;
        }
    }
}
```

## Example 4: Batch Upsert (HBase Batch Put → Postgres Bulk Upsert)

### Before (HBase)
```java
List<Put> puts = new ArrayList<>();
for (Map<String, String> record : records) {
    Put put = new Put(Bytes.toBytes(record.get("id")));
    put.addColumn(b("d"), b("name"), b(record.get("name")));
    put.addColumn(b("d"), b("score"), b(record.get("score")));
    puts.add(put);
}
table.put(puts);  // batch write
```

### After (Lakebase)
```python
from psycopg2.extras import execute_values

data = [(r["id"], r["name"], r["score"]) for r in records]
execute_values(cursor, """
    INSERT INTO scores (id, name, score) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        score = EXCLUDED.score
""", data)
conn.commit()
```

## Example 5: Atomic Counter (HBase Increment → Postgres UPDATE)

### Before (HBase)
```java
Increment inc = new Increment(Bytes.toBytes("page_home"));
inc.addColumn(b("counters"), b("views"), 1);
inc.addColumn(b("counters"), b("unique_visitors"), 1);
Result result = table.increment(inc);
```

### After (Lakebase)
```sql
-- Atomic update (row-level locking guarantees atomicity)
UPDATE page_counters
SET views = views + 1, unique_visitors = unique_visitors + 1
WHERE page_id = 'page_home';

-- Table DDL
CREATE TABLE page_counters (
    page_id TEXT PRIMARY KEY,
    views BIGINT DEFAULT 0,
    unique_visitors BIGINT DEFAULT 0
);
```

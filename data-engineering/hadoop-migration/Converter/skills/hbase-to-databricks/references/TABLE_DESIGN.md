# Table Design: HBase → Lakebase (Managed Postgres)

## Core Rule

**HBase row key → Postgres PRIMARY KEY. Everything else → columns.**

## Row Key → Primary Key

### Simple Row Key

```
HBase: row key = user_id (e.g., "user_12345")

Lakebase (Postgres):
CREATE TABLE user_profiles (
    user_id TEXT PRIMARY KEY,
    -- columns from column families...
);
```

### Composite Row Key

```
HBase: row key = "region#date#order_id" (composite, '#' delimited)

Lakebase: Split into separate columns forming a composite primary key
CREATE TABLE orders (
    region TEXT,
    order_date DATE,
    order_id TEXT,
    -- columns from column families...
    PRIMARY KEY (region, order_date, order_id)
);

-- Add index for common lookup patterns
CREATE INDEX idx_orders_date ON orders (order_date);
```

### Salted Row Key

```
HBase: row key = "salt_bucket|user_id" (to avoid hotspotting)

Lakebase: Drop the salt — Postgres handles distribution automatically
CREATE TABLE user_data (
    user_id TEXT PRIMARY KEY,
    -- columns...
);
-- No salting needed — Postgres B-tree index handles this natively
```

### Reversed Timestamp Key

```
HBase: row key = (Long.MAX_VALUE - timestamp) + user_id
  (reversed for newest-first scan order)

Lakebase: Use ORDER BY DESC at query time — no key reversal needed
CREATE TABLE events (
    user_id TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    -- columns...
    PRIMARY KEY (user_id, event_time)
);

-- Create descending index for newest-first queries
CREATE INDEX idx_events_latest ON events (user_id, event_time DESC);

-- Query newest first:
SELECT * FROM events
WHERE user_id = 'user123'
ORDER BY event_time DESC
LIMIT 100;
```

## Column Family → Column Mapping

### Single Column Family

```
HBase:
  Table: user_profiles
  Column Family: info
    Columns: info:name, info:email, info:phone, info:address

Lakebase:
CREATE TABLE user_profiles (
    user_id TEXT PRIMARY KEY,  -- from row key
    name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT
);
```

### Multiple Column Families

```
HBase:
  Table: user_activity
  CF 'profile': profile:name, profile:email, profile:tier
  CF 'metrics': metrics:login_count, metrics:last_login, metrics:page_views
  CF 'preferences': preferences:theme, preferences:language, preferences:notifications

Lakebase — Option 1: Single Table (preferred for low-latency lookups)
CREATE TABLE user_activity (
    user_id TEXT PRIMARY KEY,
    -- profile columns
    name TEXT,
    email TEXT,
    tier TEXT,
    -- metrics columns
    login_count INTEGER,
    last_login TIMESTAMPTZ,
    page_views BIGINT,
    -- preferences columns
    theme TEXT,
    language TEXT,
    notifications BOOLEAN
);

Lakebase — Option 2: Separate Tables (if CFs are accessed independently)
CREATE TABLE user_profiles (user_id TEXT PRIMARY KEY, name TEXT, email TEXT, tier TEXT);
CREATE TABLE user_metrics (user_id TEXT PRIMARY KEY, login_count INTEGER, last_login TIMESTAMPTZ, page_views BIGINT);
CREATE TABLE user_preferences (user_id TEXT PRIMARY KEY, theme TEXT, language TEXT, notifications BOOLEAN);
```

### Wide Column / Dynamic Columns

```
HBase: Arbitrary qualifier names (e.g., cf:attr_1, cf:attr_2, ..., cf:attr_N)

Lakebase — Option 1: JSONB column (best for sparse/dynamic attributes)
CREATE TABLE entity_attributes (
    entity_id TEXT PRIMARY KEY,
    attributes JSONB
);

-- Query specific attribute:
SELECT attributes->>'color' FROM entity_attributes WHERE entity_id = 'E001';

-- Index for JSONB lookups:
CREATE INDEX idx_attrs_gin ON entity_attributes USING GIN (attributes);

Lakebase — Option 2: Key-value rows (better for querying across entities)
CREATE TABLE entity_attributes (
    entity_id TEXT,
    attribute_name TEXT,
    attribute_value TEXT,
    PRIMARY KEY (entity_id, attribute_name)
);
```

## Data Type Mapping

| HBase (byte array) | Postgres Type | Notes |
|---------------------|---------------|-------|
| `Bytes.toBytes(String)` | `TEXT` | Direct mapping |
| `Bytes.toBytes(int)` | `INTEGER` | 4 bytes |
| `Bytes.toBytes(long)` | `BIGINT` | 8 bytes |
| `Bytes.toBytes(float)` | `REAL` | 4 bytes |
| `Bytes.toBytes(double)` | `DOUBLE PRECISION` | 8 bytes |
| `Bytes.toBytes(boolean)` | `BOOLEAN` | Direct |
| Raw bytes | `BYTEA` | Binary data |
| Timestamp (long millis) | `TIMESTAMPTZ` | Convert millis to timestamp |
| JSON string | `JSONB` | Native JSON support with indexing |

## Special HBase Features

### TTL (Time-To-Live)

```
HBase: Column family TTL = 86400 (24 hours auto-expiry)

Lakebase: Scheduled cleanup job
-- Option 1: Scheduled SQL (Databricks Job)
DELETE FROM events WHERE event_time < NOW() - INTERVAL '24 hours';

-- Option 2: Postgres partitioning by time + DROP old partitions
CREATE TABLE events (
    event_id TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    data TEXT
) PARTITION BY RANGE (event_time);

CREATE TABLE events_2026_03 PARTITION OF events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

-- Drop expired partition:
DROP TABLE events_2026_01;
```

### Versions (Multiple Cell Versions)

```
HBase: VERSIONS => 3 (keep 3 versions per cell)

Lakebase: Audit/history table pattern
-- Main table (current state)
CREATE TABLE user_profiles (
    user_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- History table (previous versions)
CREATE TABLE user_profiles_history (
    user_id TEXT,
    name TEXT,
    email TEXT,
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    PRIMARY KEY (user_id, valid_from)
);

-- Trigger to capture changes (or handle in application)
```

### Coprocessors

```
HBase: Observer coprocessors (triggers) and Endpoint coprocessors (aggregation)

Lakebase:
- Observer → Postgres triggers (CREATE TRIGGER)
- Endpoint aggregation → SQL aggregate queries or materialized views
- Server-side filters → SQL WHERE clauses

-- Example: Trigger for audit logging
CREATE OR REPLACE FUNCTION log_profile_change() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO user_profiles_history (user_id, name, email, valid_from, valid_to)
    VALUES (OLD.user_id, OLD.name, OLD.email, OLD.updated_at, NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER profile_audit
    BEFORE UPDATE ON user_profiles
    FOR EACH ROW EXECUTE FUNCTION log_profile_change();
```

## Indexing Strategy

| HBase Access Pattern | Postgres Index |
|---------------------|----------------|
| Point lookup by row key | `PRIMARY KEY` (automatic B-tree) |
| Prefix scan | B-tree index (supports `LIKE 'prefix%'`) |
| Range scan on row key | `PRIMARY KEY` composite (range queries on trailing columns) |
| Secondary attribute lookup | `CREATE INDEX` on the column |
| Full-text search | `GIN` index on `tsvector` or `JSONB` |
| Existence check | Partial index: `CREATE INDEX ... WHERE condition` |

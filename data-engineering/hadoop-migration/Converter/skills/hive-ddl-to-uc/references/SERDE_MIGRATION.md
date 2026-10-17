# SerDe Migration: Hive → Unity Catalog

## Common SerDe Mappings

### CSV/TSV SerDe

```sql
-- Hive (OpenCSVSerde)
CREATE TABLE csv_data (
    col1 STRING,
    col2 STRING,
    col3 STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/data/csv/';

-- Unity Catalog
CREATE TABLE main.default.csv_data (
    col1 STRING,
    col2 STRING,
    col3 STRING
)
USING CSV
OPTIONS (
    header = 'false',
    sep = ',',
    quote = '"',
    escape = '\\'
)
LOCATION 's3://bucket/data/csv/';
-- Or convert to Delta: USING DELTA (load data separately)
```

### JSON SerDe

```sql
-- Hive (JsonSerDe)
CREATE TABLE json_events (
    event_id STRING,
    event_type STRING,
    payload MAP<STRING, STRING>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;

-- Unity Catalog
CREATE TABLE main.default.json_events (
    event_id STRING,
    event_type STRING,
    payload MAP<STRING, STRING>
)
USING JSON;
-- Or: USING DELTA (preferred, load JSON data via COPY INTO or Auto Loader)
```

### Avro SerDe

```sql
-- Hive (AvroSerDe with schema URL)
CREATE TABLE avro_data
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.url'='/schemas/data.avsc')
STORED AS AVRO;

-- Unity Catalog
CREATE TABLE main.default.avro_data
USING AVRO
LOCATION 's3://bucket/data/avro/';
-- Note: Schema is inferred from Avro files; or convert to Delta
```

### RegexSerDe

```sql
-- Hive (RegexSerDe for log parsing)
CREATE TABLE apache_logs (
    host STRING,
    identity STRING,
    user_name STRING,
    request_time STRING,
    request STRING,
    status INT,
    size INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    'input.regex' = '([^ ]*) ([^ ]*) ([^ ]*) \\[([^\\]]*)\\] "([^"]*)" (\\d+) (\\d+)'
);

-- Unity Catalog: No direct RegexSerDe equivalent
-- Strategy: Use Auto Loader with regex parsing or a Python UDF
-- CREATE TABLE main.default.apache_logs USING DELTA;
-- Then: Load via notebook with regex parsing
```

### LazySimpleSerDe (default)

```sql
-- Hive (default SerDe, typically with delimiters)
CREATE TABLE delimited_data (
    id INT,
    name STRING,
    value DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Unity Catalog
CREATE TABLE main.default.delimited_data (
    id INT,
    name STRING,
    value DOUBLE
)
USING CSV
OPTIONS (sep = '\t', header = 'false');
-- Or: USING DELTA (preferred)
```

## SerDe Mapping Summary

| Hive SerDe | UC Format | Notes |
|------------|-----------|-------|
| `LazySimpleSerDe` | `CSV` or `DELTA` | Default Hive SerDe |
| `OpenCSVSerde` | `CSV` | Map SERDEPROPERTIES to OPTIONS |
| `JsonSerDe` | `JSON` or `DELTA` | Prefer Delta |
| `AvroSerDe` | `AVRO` or `DELTA` | Prefer Delta |
| `OrcSerde` | `DELTA` | Always convert |
| `ParquetHiveSerDe` | `PARQUET` or `DELTA` | Prefer Delta |
| `RegexSerDe` | `DELTA` | Requires custom loading logic |
| `ColumnarSerDe` | `DELTA` | RCFile format, convert to Delta |
| Custom Java SerDe | `DELTA` | **Manual review required** |

## Flags for Manual Review

- Custom Java SerDe classes → need equivalent parsing logic in Python/SQL
- `ROW FORMAT SERDE` with complex SERDEPROPERTIES → validate mapping
- `INPUTFORMAT`/`OUTPUTFORMAT` overrides → check compatibility

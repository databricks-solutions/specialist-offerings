# Data Validation Techniques

## 1. Row Count Comparison

The simplest and first check. If row counts do not match, something is fundamentally wrong.

```sql
-- On EMR (Hive/Spark SQL)
SELECT COUNT(*) as row_count FROM source_table WHERE date = '2024-01-01';

-- On Databricks
SELECT COUNT(*) as row_count FROM catalog.schema.target_table WHERE date = '2024-01-01';
```

**Automated comparison (run on Databricks):**

```python
def validate_row_counts(emr_table, dbx_table, partition_col="date", partition_value="2024-01-01"):
    """Compare row counts between EMR output (loaded into Databricks) and Databricks output."""
    emr_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {emr_table} 
        WHERE {partition_col} = '{partition_value}'
    """).collect()[0]["cnt"]
    
    dbx_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {dbx_table} 
        WHERE {partition_col} = '{partition_value}'
    """).collect()[0]["cnt"]
    
    match = emr_count == dbx_count
    print(f"EMR: {emr_count:,} rows | Databricks: {dbx_count:,} rows | Match: {match}")
    return match
```

---

## 2. Checksum Validation

Hash concatenated columns to detect any data differences. Works well for tables with a primary key.

```sql
-- Generate checksum per row (on both EMR and Databricks)
SELECT 
    id,
    md5(concat_ws('|', 
        CAST(id AS STRING),
        COALESCE(CAST(name AS STRING), ''),
        COALESCE(CAST(amount AS STRING), ''),
        COALESCE(CAST(created_at AS STRING), '')
    )) as row_hash
FROM my_table
WHERE date = '2024-01-01'
ORDER BY id;
```

**Partition-level checksum for large tables:**

```sql
-- Aggregate checksum per partition
SELECT 
    date,
    COUNT(*) as row_count,
    md5(CAST(SUM(CAST(conv(substr(md5(concat_ws('|', 
        CAST(id AS STRING), 
        COALESCE(name, ''),
        COALESCE(CAST(amount AS STRING), '')
    )), 1, 8), 16, 10) AS BIGINT)) AS STRING)) as partition_hash
FROM my_table
GROUP BY date
ORDER BY date;
```

**Python comparison:**

```python
def validate_checksums(emr_table, dbx_table, key_cols, check_cols, partition_filter=""):
    """Compare row-level checksums between two tables."""
    cols_expr = ", ".join([f"COALESCE(CAST({c} AS STRING), '')" for c in check_cols])
    where = f"WHERE {partition_filter}" if partition_filter else ""
    key_expr = ", ".join(key_cols)
    
    query = f"""
        SELECT {key_expr}, md5(concat_ws('|', {cols_expr})) as row_hash
        FROM {{table}} {where}
    """
    
    emr_df = spark.sql(query.format(table=emr_table))
    dbx_df = spark.sql(query.format(table=dbx_table))
    
    # Find mismatches
    mismatches = emr_df.alias("e").join(
        dbx_df.alias("d"),
        [emr_df[k] == dbx_df[k] for k in key_cols],
        "full_outer"
    ).filter("e.row_hash != d.row_hash OR e.row_hash IS NULL OR d.row_hash IS NULL")
    
    mismatch_count = mismatches.count()
    print(f"Checksum mismatches: {mismatch_count}")
    if mismatch_count > 0:
        print("Sample mismatches:")
        mismatches.show(10, truncate=False)
    return mismatch_count == 0
```

---

## 3. Schema Comparison

Verify column names, data types, and nullability match between source and target.

```sql
-- On EMR
DESCRIBE TABLE EXTENDED source_table;

-- On Databricks
DESCRIBE TABLE EXTENDED catalog.schema.target_table;
```

**Automated schema comparison:**

```python
def validate_schema(emr_table, dbx_table):
    """Compare schemas between two tables."""
    emr_schema = spark.table(emr_table).schema
    dbx_schema = spark.table(dbx_table).schema
    
    emr_fields = {f.name: (str(f.dataType), f.nullable) for f in emr_schema.fields}
    dbx_fields = {f.name: (str(f.dataType), f.nullable) for f in dbx_schema.fields}
    
    all_cols = set(emr_fields.keys()) | set(dbx_fields.keys())
    issues = []
    
    for col in sorted(all_cols):
        if col not in emr_fields:
            issues.append(f"  EXTRA in Databricks: {col} ({dbx_fields[col][0]})")
        elif col not in dbx_fields:
            issues.append(f"  MISSING in Databricks: {col} ({emr_fields[col][0]})")
        elif emr_fields[col] != dbx_fields[col]:
            issues.append(f"  TYPE MISMATCH: {col} - EMR: {emr_fields[col]} vs DBX: {dbx_fields[col]}")
    
    if issues:
        print("Schema differences found:")
        for issue in issues:
            print(issue)
        return False
    else:
        print("Schemas match perfectly.")
        return True
```

---

## 4. Sample Data Comparison

Compare a random sample of rows field-by-field to catch subtle differences.

```python
def validate_sample(emr_table, dbx_table, key_cols, sample_size=1000):
    """Compare a random sample of rows field-by-field."""
    key_expr = ", ".join(key_cols)
    
    # Get sample keys from EMR table
    sample_keys = spark.sql(f"""
        SELECT {key_expr} FROM {emr_table}
        ORDER BY RAND()
        LIMIT {sample_size}
    """)
    sample_keys.createOrReplaceTempView("sample_keys")
    
    # Get rows from both tables
    join_cond = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])
    emr_sample = spark.sql(f"SELECT t.* FROM {emr_table} t JOIN sample_keys s ON {join_cond}")
    dbx_sample = spark.sql(f"SELECT t.* FROM {dbx_table} t JOIN sample_keys s ON {join_cond}")
    
    # Compare using subtract
    emr_only = emr_sample.subtract(dbx_sample)
    dbx_only = dbx_sample.subtract(emr_sample)
    
    emr_only_count = emr_only.count()
    dbx_only_count = dbx_only.count()
    
    if emr_only_count == 0 and dbx_only_count == 0:
        print(f"Sample of {sample_size} rows: MATCH")
        return True
    else:
        print(f"Sample differences: {emr_only_count} rows only in EMR, {dbx_only_count} rows only in Databricks")
        if emr_only_count > 0:
            print("Sample rows only in EMR:")
            emr_only.show(5, truncate=False)
        if dbx_only_count > 0:
            print("Sample rows only in Databricks:")
            dbx_only.show(5, truncate=False)
        return False
```

---

## 5. Null Analysis

Verify null distributions match -- different handling of nulls is a common migration issue.

```sql
-- Run on both EMR and Databricks
SELECT
    COUNT(*) as total_rows,
    SUM(CASE WHEN col1 IS NULL THEN 1 ELSE 0 END) as col1_nulls,
    SUM(CASE WHEN col2 IS NULL THEN 1 ELSE 0 END) as col2_nulls,
    SUM(CASE WHEN col3 IS NULL THEN 1 ELSE 0 END) as col3_nulls,
    SUM(CASE WHEN col4 IS NULL THEN 1 ELSE 0 END) as col4_nulls
FROM my_table
WHERE date = '2024-01-01';
```

**Automated null analysis:**

```python
def validate_nulls(emr_table, dbx_table, partition_filter=""):
    """Compare null counts per column between tables."""
    where = f"WHERE {partition_filter}" if partition_filter else ""
    columns = spark.table(emr_table).columns
    
    null_exprs = [f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) as `{c}_nulls`" for c in columns]
    select_expr = ", ".join(["COUNT(*) as total"] + null_exprs)
    
    emr_nulls = spark.sql(f"SELECT {select_expr} FROM {emr_table} {where}").collect()[0]
    dbx_nulls = spark.sql(f"SELECT {select_expr} FROM {dbx_table} {where}").collect()[0]
    
    issues = []
    for col in columns:
        emr_val = emr_nulls[f"{col}_nulls"]
        dbx_val = dbx_nulls[f"{col}_nulls"]
        if emr_val != dbx_val:
            issues.append(f"  {col}: EMR={emr_val} nulls, DBX={dbx_val} nulls (diff={dbx_val - emr_val})")
    
    if issues:
        print("Null count differences:")
        for issue in issues:
            print(issue)
        return False
    print("Null counts match for all columns.")
    return True
```

---

## 6. Aggregate Validation

Compare summary statistics on numeric columns to catch systemic differences.

```sql
-- Run on both platforms
SELECT
    COUNT(*) as row_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    COUNT(DISTINCT category) as distinct_categories,
    COUNT(DISTINCT customer_id) as distinct_customers
FROM my_table
WHERE date = '2024-01-01';
```

**Automated aggregate comparison with tolerance for floating-point:**

```python
def validate_aggregates(emr_table, dbx_table, numeric_cols, partition_filter="", tolerance=1e-6):
    """Compare aggregate statistics with floating-point tolerance."""
    where = f"WHERE {partition_filter}" if partition_filter else ""
    
    agg_exprs = []
    for col in numeric_cols:
        agg_exprs.extend([
            f"SUM(CAST(`{col}` AS DOUBLE)) as `{col}_sum`",
            f"AVG(CAST(`{col}` AS DOUBLE)) as `{col}_avg`",
            f"MIN(`{col}`) as `{col}_min`",
            f"MAX(`{col}`) as `{col}_max`"
        ])
    
    select_expr = ", ".join(["COUNT(*) as total"] + agg_exprs)
    
    emr_agg = spark.sql(f"SELECT {select_expr} FROM {emr_table} {where}").collect()[0]
    dbx_agg = spark.sql(f"SELECT {select_expr} FROM {dbx_table} {where}").collect()[0]
    
    issues = []
    for key in emr_agg.asDict().keys():
        emr_val = emr_agg[key]
        dbx_val = dbx_agg[key]
        if emr_val is None and dbx_val is None:
            continue
        if emr_val is None or dbx_val is None:
            issues.append(f"  {key}: EMR={emr_val}, DBX={dbx_val}")
        elif isinstance(emr_val, float):
            if abs(emr_val - dbx_val) > tolerance * max(abs(emr_val), 1):
                issues.append(f"  {key}: EMR={emr_val}, DBX={dbx_val}, diff={abs(emr_val - dbx_val)}")
        elif emr_val != dbx_val:
            issues.append(f"  {key}: EMR={emr_val}, DBX={dbx_val}")
    
    if issues:
        print("Aggregate differences:")
        for issue in issues:
            print(issue)
        return False
    print("All aggregates match within tolerance.")
    return True
```

---

## Full Validation Runner

```python
def run_full_validation(emr_table, dbx_table, key_cols, numeric_cols, partition_filter=""):
    """Run all validation checks and produce a summary report."""
    results = {}
    
    print("=" * 60)
    print(f"VALIDATION: {emr_table} vs {dbx_table}")
    print("=" * 60)
    
    print("\n1. ROW COUNT VALIDATION")
    results["row_count"] = validate_row_counts(emr_table, dbx_table)
    
    print("\n2. SCHEMA VALIDATION")
    results["schema"] = validate_schema(emr_table, dbx_table)
    
    print("\n3. NULL ANALYSIS")
    results["nulls"] = validate_nulls(emr_table, dbx_table, partition_filter)
    
    print("\n4. AGGREGATE VALIDATION")
    results["aggregates"] = validate_aggregates(emr_table, dbx_table, numeric_cols, partition_filter)
    
    print("\n5. CHECKSUM VALIDATION")
    results["checksums"] = validate_checksums(emr_table, dbx_table, key_cols, 
                                               spark.table(emr_table).columns, partition_filter)
    
    print("\n6. SAMPLE COMPARISON")
    results["sample"] = validate_sample(emr_table, dbx_table, key_cols)
    
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    all_pass = True
    for check, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {check}: {status}")
        if not passed:
            all_pass = False
    
    print(f"\nOverall: {'ALL CHECKS PASSED' if all_pass else 'SOME CHECKS FAILED'}")
    return all_pass
```

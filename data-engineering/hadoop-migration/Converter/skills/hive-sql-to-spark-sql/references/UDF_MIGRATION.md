# Hive UDF Migration → Databricks

## Built-in Function Equivalents

Most Hive built-in functions have direct Spark SQL equivalents:

| Hive Function | Spark SQL Equivalent | Notes |
|---------------|---------------------|-------|
| `to_date(string)` | `to_date(string)` | Same |
| `date_add(date, n)` | `date_add(date, n)` | Same |
| `datediff(end, start)` | `datediff(end, start)` | Same |
| `from_unixtime(ts)` | `from_unixtime(ts)` | Same |
| `unix_timestamp(str)` | `unix_timestamp(str)` | Same |
| `get_json_object(json, path)` | `get_json_object(json, path)` | Same |
| `json_tuple(json, k1, k2)` | `json_tuple(json, k1, k2)` | Same |
| `parse_url(url, part)` | `parse_url(url, part)` | Same |
| `regexp_extract(str, re, idx)` | `regexp_extract(str, re, idx)` | Same |
| `regexp_replace(str, re, rep)` | `regexp_replace(str, re, rep)` | Same |
| `split(str, pattern)` | `split(str, pattern)` | Same |
| `collect_set(col)` | `collect_set(col)` | Same |
| `collect_list(col)` | `collect_list(col)` | Same |
| `sort_array(array)` | `sort_array(array)` | Same |
| `size(array_or_map)` | `size(array_or_map)` | Same |
| `xpath(xml, path)` | No direct equivalent | Use Python UDF |
| `xpath_string/int/double` | No direct equivalent | Use Python UDF |

## Custom Hive UDF Migration

### Java Hive UDF → SQL UDF

```java
// Before: Java Hive UDF
public class MaskEmail extends UDF {
    public String evaluate(String email) {
        if (email == null) return null;
        int at = email.indexOf('@');
        if (at <= 1) return email;
        return email.charAt(0) + "***" + email.substring(at);
    }
}
```

```sql
-- After: Databricks SQL UDF
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN email IS NULL THEN NULL
    WHEN INSTR(email, '@') <= 1 THEN email
    ELSE CONCAT(LEFT(email, 1), '***', SUBSTRING(email, INSTR(email, '@')))
END;
```

### Java Hive GenericUDF → Python UDF

```java
// Before: GenericUDF (complex types)
public class ParseUserAgent extends GenericUDF {
    // ... parses user agent strings into struct
}
```

```python
# After: Python UDF in Databricks
CREATE OR REPLACE FUNCTION parse_user_agent(ua STRING)
RETURNS STRUCT<browser: STRING, os: STRING, device: STRING>
LANGUAGE PYTHON
AS $$
    # Simple parser — for production use ua-parser library
    browser = "unknown"
    if "Chrome" in ua: browser = "Chrome"
    elif "Firefox" in ua: browser = "Firefox"
    elif "Safari" in ua: browser = "Safari"

    os_name = "unknown"
    if "Windows" in ua: os_name = "Windows"
    elif "Mac" in ua: os_name = "macOS"
    elif "Linux" in ua: os_name = "Linux"

    device = "desktop"
    if "Mobile" in ua: device = "mobile"
    elif "Tablet" in ua: device = "tablet"

    return {"browser": browser, "os": os_name, "device": device}
$$;
```

### UDAF (User-Defined Aggregate Function)

```java
// Before: Hive UDAF
// Complex Java implementation with init/iterate/merge/terminate

-- After: Use built-in Spark SQL aggregates or Python UDAF
-- Most custom aggregates can be replaced with combinations of:
-- collect_list + transform + aggregate
-- Or use Pandas UDF for complex aggregations
```

### UDTF (User-Defined Table Function)

```sql
-- Hive UDTF usage
SELECT t.word, t.count
FROM documents
LATERAL VIEW my_tokenizer(text) t AS word, count;

-- Databricks: Use explode + transform or Python UDTF
SELECT word, count
FROM documents,
LATERAL TABLE(tokenize(text)) AS t(word, count);

-- Or with Python UDTF (Databricks Runtime 14.0+)
CREATE OR REPLACE FUNCTION tokenize(text STRING)
RETURNS TABLE(word STRING, count INT)
LANGUAGE PYTHON
AS $$
    from collections import Counter
    words = text.lower().split()
    for word, cnt in Counter(words).items():
        yield (word, cnt)
$$;
```

## ADD JAR → Library Dependencies

```sql
-- HiveQL
ADD JAR hdfs:///jars/custom-udf-1.0.jar;
CREATE TEMPORARY FUNCTION my_func AS 'com.example.MyUDF';

-- Databricks: Install library on cluster, then:
-- Option 1: Use cluster-level library installation
-- Option 2: Convert UDF to SQL/Python (preferred)
```

# Complete Magic Command Mapping

## Interpreter / Magic Command Reference

| Zeppelin | Jupyter/EMR Studio | Databricks | Notes |
|---|---|---|---|
| %pyspark | N/A (default kernel) | %python | Default in Databricks Python notebooks |
| %spark | N/A | %scala | Requires Scala-compatible cluster |
| %spark.sql / %sql | %%sql (cell magic) | %sql | Works identically in Databricks |
| %sh | !command or %%bash | %sh | Runs on driver node only |
| %md | Markdown cell | %md | Rendered as markdown |
| %r | %%R | %r | Requires R support on cluster |
| %angular | N/A | N/A | Use Databricks widgets or dashboards |
| z.show() | display() | display() | Databricks display function with rich viz |
| z.input() | N/A | dbutils.widgets.text() | Text input widget |
| z.select() | N/A | dbutils.widgets.dropdown() | Dropdown selection widget |
| z.checkbox() | N/A | dbutils.widgets.multiselect() | Multi-select checkbox widget |
| z.textbox() | N/A | dbutils.widgets.text() | Same as z.input() |
| z.run() | N/A | dbutils.notebook.run() | Run another notebook |

## Detailed Conversion Examples

### Zeppelin %pyspark to Databricks %python

```python
# Zeppelin
%pyspark
from pyspark.sql.functions import col, sum
df = spark.read.parquet("s3://bucket/data")
z.show(df.groupBy("category").agg(sum("amount")))

# Databricks (default cell -- no magic needed if notebook language is Python)
from pyspark.sql.functions import col, sum
df = spark.read.parquet("s3://bucket/data")
display(df.groupBy("category").agg(sum("amount")))
```

### Zeppelin %spark.sql to Databricks %sql

```sql
-- Zeppelin
%spark.sql
SELECT category, SUM(amount) as total
FROM my_table
GROUP BY category
ORDER BY total DESC

-- Databricks
%sql
SELECT category, SUM(amount) as total
FROM my_table
GROUP BY category
ORDER BY total DESC
```

### Zeppelin %sh to Databricks %sh

```bash
# Zeppelin
%sh
ls -la /tmp/
echo "Current user: $(whoami)"

# Databricks (identical)
%sh
ls -la /tmp/
echo "Current user: $(whoami)"
```

### Zeppelin %md to Databricks %md

```markdown
<!-- Zeppelin -->
%md
## Analysis Report
This notebook analyzes **sales data** for Q4.
- Revenue trends
- Top products
- Regional breakdown

<!-- Databricks (identical) -->
%md
## Analysis Report
This notebook analyzes **sales data** for Q4.
- Revenue trends
- Top products
- Regional breakdown
```

### Zeppelin Dynamic Forms to Databricks Widgets

```python
# Zeppelin -- text input
%pyspark
name = z.input("name", "World")
print(f"Hello, {name}!")

# Databricks
dbutils.widgets.text("name", "World", "Name")
name = dbutils.widgets.get("name")
print(f"Hello, {name}!")
```

```python
# Zeppelin -- dropdown select
%pyspark
env = z.select("environment", [
    ("dev", "Development"),
    ("staging", "Staging"),
    ("prod", "Production")
], "dev")
print(f"Environment: {env}")

# Databricks
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
env = dbutils.widgets.get("environment")
print(f"Environment: {env}")
```

```python
# Zeppelin -- checkbox (multi-select)
%pyspark
regions = z.checkbox("regions", [
    ("us-east", "US East"),
    ("us-west", "US West"),
    ("eu", "Europe")
], ["us-east"])
print(f"Selected regions: {regions}")

# Databricks
dbutils.widgets.multiselect("regions", "us-east", ["us-east", "us-west", "eu"], "Regions")
regions = dbutils.widgets.get("regions")  # Returns comma-separated string
print(f"Selected regions: {regions}")
```

### Zeppelin z.run() to Databricks dbutils.notebook.run()

```python
# Zeppelin -- run another paragraph by ID
%pyspark
z.run("20ABC123")

# Zeppelin -- run another notebook
%pyspark
z.run("/path/to/other/notebook")

# Databricks -- run another notebook (with timeout and parameters)
result = dbutils.notebook.run(
    "/path/to/other/notebook",
    timeout_seconds=3600,
    arguments={"param1": "value1", "param2": "value2"}
)
print(f"Notebook returned: {result}")
```

### Zeppelin %angular to Databricks Alternatives

Zeppelin's `%angular` interpreter allows embedding AngularJS code for interactive visualizations. Databricks does not support this directly.

**Alternatives:**
1. **Databricks widgets** -- for simple interactive controls (text, dropdown, multiselect)
2. **Databricks SQL dashboards** -- for interactive dashboards with filters
3. **Plotly/Bokeh** -- for interactive charts in notebooks
4. **Databricks Apps** -- for full custom web applications (FastAPI + React)

```python
# Zeppelin %angular (NOT supported in Databricks)
%angular
<div ng-controller="MyController">
  <input ng-model="query" />
  <button ng-click="search()">Search</button>
</div>

# Databricks alternative: widgets + display
dbutils.widgets.text("query", "", "Search Query")
query = dbutils.widgets.get("query")
results = spark.sql(f"SELECT * FROM table WHERE name LIKE '%{query}%'")
display(results)
```

## Jupyter Cell Magics to Databricks

| Jupyter | Databricks | Notes |
|---|---|---|
| %%sql | %sql | Same behavior |
| %%bash / %%sh | %sh | Same behavior |
| %%python | %python | Default in Python notebooks |
| %%scala | %scala | Cross-language support |
| %%markdown | %md | Rendered markdown |
| %%html | %html | Rendered HTML (Databricks extension) |
| %%timeit | N/A | Use `%timeit` (line magic) or manual timing |
| %%capture | N/A | No direct equivalent |
| !pip install | %pip install | Databricks-specific; installs on cluster |
| %%writefile | N/A | Use `dbutils.fs.put()` or Python file I/O |

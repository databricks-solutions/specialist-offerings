# Hadoop Migration Toolkit

End-to-end toolkit for profiling, analyzing, and converting Hadoop workloads to Databricks. It comprises three modules that work as a pipeline:

1. **Profiler** — Extracts cluster metadata and workload history from Hadoop REST APIs, then exports it to a portable DuckDB database
2. **Analyzer** — Produces a code-level workload inventory with Oozie workflow correlation
3. **Converter** — AI-assisted conversion of Hadoop code to Databricks equivalents (Claude Code plugin)

**Supported Distributions:** CDH 5.x/6.x, HDP 2.x/3.x, HDI 3.x/4.x, CDP 7.x

---

## Quick Start

```bash
# 1. Profile the cluster (on an edge node with curl + jq)
cd Profiler
./profiler.sh <encryption-key>

# 2. Export profiler JSON to a portable DuckDB file
pip install -r requirements.txt
python -m duckdb_exporter export --profiler-output ./Output --output hadoop_profiler.duckdb

# 3. Build a code-level workload inventory
cd ../Analyzer
pip install -r requirements.txt
python -m analyzer analyze --config analyzer.conf.yaml

# 4. Convert workloads to Databricks (requires Claude Code + Converter plugin)
/convert hive-ddl create_tables.sql
/convert spark etl_job.py
/convert oozie workflow.xml
```

---

## Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│     Profiler      │     │     Analyzer      │     │    Converter      │
│  (Bash + Python)  │────▶│     (Python)      │────▶│ (Claude Code)     │
│                   │     │                   │     │                   │
│ • profiler.sh     │     │ • Parse YARN/     │     │ • 7 conversion    │
│ • duckdb_exporter │     │   Spark/Impala    │     │   skills          │
│                   │     │ • Scan Oozie      │     │ • /convert cmd    │
│ Cluster REST APIs │     │ • Correlate jobs  │     │ • Migration notes │
│ → 17 JSON files   │     │   to workflows    │     │                   │
│ → .duckdb (24 tbl)│     │ → JSON + CSV      │     │ → Databricks code │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

---

## Profiler

The Profiler has two parts: a **Bash script** that extracts data from Hadoop REST APIs and a **Python DuckDB Exporter** that transforms the raw JSON into a structured analytical database.

### profiler.sh

A self-contained Bash script (~790 lines) that extracts cluster metadata by calling YARN, Spark History Server, Cloudera Manager (or Ambari), and Impala REST APIs. Requires only `curl` and `jq` on the target host.

**Key features:**
- Zero-install on the target cluster
- Non-intrusive read-only API calls
- Kerberos and SSL support
- Initial + incremental extraction modes
- Produces 17 JSON files organized under `Output/{YARN,SPARK,CM,IMPALA}/`

**Configuration:** Edit `Profiler/profiler.conf` with cluster endpoints, credentials (AES-256 encrypted), and extraction scope. See `Profiler/RUNBOOK.md` for detailed setup instructions.

**Usage:**
```bash
cd Profiler
./profiler.sh <encryption-key>
```

### DuckDB Exporter

Transforms the 17 raw JSON files into a single portable `.duckdb` database with 24 tables (17 base + 7 derived analysis views). Includes job type classification, cost estimation, and per-user/queue/type summaries.

**Usage:**
```bash
cd Profiler
pip install -r requirements.txt

# Export to DuckDB
python -m duckdb_exporter export --profiler-output ./Output --output hadoop_profiler.duckdb

# Validate profiler output
python -m duckdb_exporter validate --profiler-output ./Output

# Query the database
python -m duckdb_exporter query --db hadoop_profiler.duckdb \
    --sql "SELECT job_type, COUNT(*) FROM yarn_analysis_vw GROUP BY job_type"
```

**Requirements:** Python 3.8+, `duckdb>=0.10.0`, `pyyaml>=6.0`

---

## Analyzer

A Python tool that builds a code-level workload inventory by parsing Profiler output and correlating it with Oozie workflow definitions.

The Profiler captures app-level metadata but no code-level details. The Analyzer fills this gap by:
- Parsing YARN, Spark, and Impala data from profiler JSON output
- Connecting to Oozie REST API to retrieve workflow/coordinator definitions
- Correlating YARN apps to Oozie workflows via launcher name patterns
- Extracting code artifacts (JARs, scripts, SQL files) from each Oozie action
- Optionally verifying HDFS paths via WebHDFS

### Supported Oozie Action Types

| Action Type | Extracted Artifacts |
|---|---|
| `<spark>` | JAR path, main class, spark-opts |
| `<hive>` / `<hive2>` | Script path, inline SQL, UDF JARs |
| `<sqoop>` | JDBC URL, table name, target directory |
| `<shell>` | Exec script, staged files |
| `<map-reduce>` | JAR path, mapper/reducer classes |
| `<sub-workflow>` | App-path for recursive analysis |

### Usage

```bash
cd Analyzer
pip install -r requirements.txt

# Full analysis: parse profiler output + scan Oozie + correlate
python -m analyzer analyze --config analyzer.conf.yaml

# Parse profiler output only (no Oozie access needed)
python -m analyzer parse-profiler --config analyzer.conf.yaml

# Scan Oozie workflows independently
python -m analyzer scan-oozie --config analyzer.conf.yaml

# Verify HDFS paths from inventory
python -m analyzer verify-paths --config analyzer.conf.yaml --input inventory.json
```

**Output:** `workload_inventory.json` and `workload_inventory.csv` containing per-workload details (type, user, queue, resource usage, Oozie workflow, code references, dependencies).

**Requirements:** Python 3.8+, `requests>=2.28.0`, `requests-kerberos>=0.14.0`, `pyyaml>=6.0`

---

## Converter

A Claude Code plugin with 7 skills for AI-assisted conversion of Hadoop code to Databricks equivalents. Invoked via the `/convert` slash command.

| Skill | From → To |
|---|---|
| `hive-ddl-to-uc` | Hive DDL → Unity Catalog DDL |
| `spark-to-databricks` | OSS Spark → Databricks Spark |
| `hive-sql-to-spark-sql` | HiveQL → Databricks SQL |
| `sqoop-to-databricks` | Sqoop → JDBC / Lakehouse Federation |
| `hbase-to-databricks` | HBase → Lakebase (managed Postgres) |
| `oozie-to-databricks-workflows` | Oozie XML → Databricks Workflows |
| `ranger-to-uc-policies` | Ranger → UC Grants + Row Filters + Column Masks |

### Usage

```
/convert hive-ddl <DDL-statement-or-file>
/convert spark <spark-code-or-file>
/convert hive-sql <hiveql-or-file>
/convert sqoop <sqoop-command>
/convert hbase <hbase-code>
/convert oozie <workflow.xml>
/convert ranger <policy-json>
```

Each skill can also be triggered by describing the conversion need naturally (e.g., "Convert this Hive CREATE TABLE to Unity Catalog").

**Prerequisite:** Claude Code CLI with the Converter plugin installed.

---

## End-to-End Workflow

| Step | Action | Tool |
|------|--------|------|
| 1 | Run Profiler against Hadoop cluster | `profiler.sh` |
| 2 | Export JSON to DuckDB | `python -m duckdb_exporter export` |
| 3 | Upload .duckdb to Databricks, convert to Delta | Databricks notebook |
| 4 | Run Analyzer with Oozie correlation | `python -m analyzer analyze` |
| 5 | Review workload inventory, prioritize | Manual review of JSON/CSV |
| 6 | Convert each workload to Databricks | `/convert <type> <file>` |
| 7 | Review and test converted code | Manual + Databricks |

---

## File Structure

```
hadoop-migration/
├── Profiler/
│   ├── profiler.sh                  # Cluster data extraction (Bash)
│   ├── profiler.conf                # Cluster configuration
│   ├── RUNBOOK.md                   # Detailed setup and execution guide
│   ├── requirements.txt             # Python dependencies
│   ├── duckdb_exporter.conf.yaml    # DuckDB exporter configuration
│   ├── duckdb_exporter/             # JSON → DuckDB export module
│   │   ├── loaders/                 # YARN, Spark, Impala, CM loaders
│   │   └── transforms/             # Derived analysis tables
│   └── tests/
├── Analyzer/
│   ├── analyzer/                    # Workload analysis module
│   │   ├── parsers/                 # YARN, Spark, Impala parsers
│   │   ├── extractors/             # Oozie action type extractors
│   │   ├── connectors/             # Oozie + WebHDFS clients
│   │   └── reporters/              # JSON + CSV output
│   ├── analyzer.conf.yaml          # Analyzer configuration
│   └── tests/
├── Converter/
│   ├── commands/
│   │   └── convert.md              # /convert slash command
│   ├── skills/                     # 7 conversion skills
│   │   ├── hive-ddl-to-uc/
│   │   ├── spark-to-databricks/
│   │   ├── hive-sql-to-spark-sql/
│   │   ├── sqoop-to-databricks/
│   │   ├── hbase-to-databricks/
│   │   ├── oozie-to-databricks-workflows/
│   │   └── ranger-to-uc-policies/
│   └── resources/
│       └── COMMON_PATTERNS.md
└── HADOOP_MIGRATION_TOOLKIT.md     # Detailed stakeholder walkthrough
```

---

## Running Tests

```bash
# Profiler / DuckDB Exporter tests
cd Profiler && python -m pytest tests/ -v

# Analyzer tests
cd Analyzer && python -m pytest tests/ -v
```

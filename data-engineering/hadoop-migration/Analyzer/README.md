# Hadoop Workload Analyzer

A Python tool that parses profiler output and connects to Oozie REST API to build a code-level inventory of all Hadoop workloads.

## Overview

The existing Profiler (`../Profiler/profiler.sh`) captures app-level metadata from YARN, Spark History Server, Cloudera Manager, and Impala. However, it captures **no code-level details** — no JAR paths, no script locations, no workflow definitions, no Oozie data.

The Analyzer fills this gap by:

1. **Parsing profiler JSON output** — extracts workload metadata from YARN, Spark HS, and Impala dumps
2. **Connecting to Oozie** — retrieves workflow/coordinator definitions and extracts code artifacts from each action
3. **Correlating data** — matches YARN apps to Oozie workflows using launcher name patterns
4. **Verifying paths** — optionally checks that HDFS code paths actually exist via WebHDFS
5. **Generating reports** — outputs structured JSON and/or CSV inventory files

## Installation

```bash
cd src/hadoop/Analyzer
pip install -r requirements.txt
```

## Configuration

Copy and edit the config file:

```bash
cp analyzer.conf.yaml my-config.yaml
# Edit my-config.yaml with your environment details
```

```yaml
profiler_output:
  base_dir: "/path/to/profiler-output/Output"  # auto-detects YARN/, SPARK/, IMPALA/ subdirs

oozie:
  url: "http://oozie-host:11000"
  auth: "simple"          # "simple" | "kerberos"
  kerberos_principal: ""  # only if auth=kerberos
  max_jobs: 5000
  timeout: 30

webhdfs:
  enabled: false
  url: "http://namenode:9870"
  user: "hdfs"

output:
  format: "json"          # "json" | "csv" | "both"
  dir: "./analyzer-output"
```

## Usage

### Full Pipeline (Profiler + Oozie)

```bash
python -m analyzer analyze --config analyzer.conf.yaml
```

### Profiler Output Only (No Oozie)

```bash
python -m analyzer parse-profiler --config analyzer.conf.yaml
```

### Oozie Only (No Profiler)

```bash
python -m analyzer scan-oozie --config analyzer.conf.yaml
```

### Verify HDFS Paths

```bash
python -m analyzer verify-paths --config analyzer.conf.yaml --input inventory.json
```

### Verbose Mode

Add `-v` for debug logging:

```bash
python -m analyzer -v parse-profiler --config analyzer.conf.yaml
```

## Output

### JSON Report

```json
{
  "generated_at": "2026-03-24T10:00:00Z",
  "total_workloads": 47,
  "summary": {
    "by_type": {"spark": 12, "hive": 20, "sqoop": 5, "mapreduce": 10},
    "by_source": {"yarn": 23, "oozie": 40, "impala": 12}
  },
  "inventory": [
    {
      "workload_id": "application_xxx_0005",
      "workload_type": "hive",
      "entry_point": "etl_daily.hql",
      "code_artifacts": [
        {"path": "/user/hive/scripts/etl_daily.hql", "location_type": "hdfs", "artifact_type": "hql"}
      ],
      "oozie_workflow_name": "etl-daily-wf",
      "oozie_app_path": "hdfs:///user/oozie/workflows/etl-daily",
      "source": "oozie+yarn"
    }
  ]
}
```

### CSV Report

One row per workload with columns: workload_id, workload_name, workload_type, user, queue, entry_point, source, tags, code_artifact_paths, dependency_paths, oozie_workflow_name, etc.

## How Oozie Correlation Works

YARN apps launched by Oozie have predictable name patterns:
```
oozie:launcher:T=<type>:W=<wf-name>:A=<action-name>:ID=<wf-id>
```

The Analyzer:
1. Parses YARN app names for this pattern
2. Retrieves the corresponding Oozie workflow definition XML
3. Extracts code artifacts from each action (JARs, scripts, SQL files)
4. Merges YARN metrics with Oozie code-level details

## Supported Oozie Action Types

| Action Type | Extracted Artifacts |
|---|---|
| `<spark>` | JAR path, main class, spark-opts (--jars, --files) |
| `<hive>` / `<hive2>` | Script path, inline SQL, UDF JARs |
| `<sqoop>` | JDBC URL, table name, target directory |
| `<shell>` | Exec script, staged files |
| `<map-reduce>` | JAR path, mapper/reducer classes |
| `<sub-workflow>` | App-path (for recursive analysis) |

## Running Tests

```bash
cd src/hadoop/Analyzer
python -m pytest tests/ -v
```

## Integration with Converter

Use the Analyzer output to feed into the Converter plugin:

1. Run: `python -m analyzer parse-profiler --config analyzer.conf.yaml`
2. Review the inventory JSON
3. For each workload type, use the corresponding `/convert` command in Claude Code

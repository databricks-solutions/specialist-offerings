# Implementation Plan: Analyzer + Converter Modules for Hadoop Migration Toolkit

## Context

The existing Profiler (`src/hadoop/Profiler/profiler.sh`) extracts app-level metadata from YARN, Spark HS, CM, and Impala via REST APIs into JSON files. However, it captures **no code-level details** — no JAR paths, no script locations, no workflow definitions, no Oozie data. Two new modules address this gap:

1. **Analyzer** — Python tool that parses profiler output + connects to Oozie REST API to build a code-level inventory of all workloads
2. **Converter** — Claude Code plugin with 6 modular skills for converting Hadoop code to Databricks equivalents

Both live as new directories parallel to `Profiler/`.

---

## Directory Structure

```
src/hadoop/
├── Profiler/                  # existing — unchanged
├── Analyzer/
│   ├── README.md
│   ├── requirements.txt       # requests, requests-kerberos, PyYAML
│   ├── analyzer.conf.yaml     # YAML config (Oozie URL, profiler output path, etc.)
│   ├── analyzer/
│   │   ├── __init__.py
│   │   ├── __main__.py        # Allow running as python -m analyzer
│   │   ├── cli.py             # argparse: analyze | parse-profiler | scan-oozie | verify-paths
│   │   ├── config.py          # YAML config loader
│   │   ├── models.py          # WorkloadType enum, CodeArtifact, WorkloadInventoryItem dataclasses
│   │   ├── parsers/
│   │   │   ├── __init__.py
│   │   │   ├── yarn_parser.py     # Parse YarnApplicationDump JSON → inventory items
│   │   │   ├── spark_parser.py    # Parse Spark_Applications JSON → inventory items
│   │   │   └── impala_parser.py   # Parse impala_*.json → inventory items (SQL as embedded artifact)
│   │   ├── connectors/
│   │   │   ├── __init__.py
│   │   │   ├── oozie_client.py    # Oozie REST API: list workflows/coordinators, get definitions
│   │   │   └── webhdfs_client.py  # WebHDFS: verify code paths exist on HDFS
│   │   ├── extractors/
│   │   │   ├── __init__.py
│   │   │   ├── spark_extractor.py     # <spark> action → JAR path, main class, spark-opts
│   │   │   ├── hive_extractor.py      # <hive>/<hive2> action → script path, inline SQL, UDF JARs
│   │   │   ├── sqoop_extractor.py     # <sqoop> action → JDBC URL, table, target-dir
│   │   │   ├── shell_extractor.py     # <shell> action → exec script, staged files
│   │   │   ├── mr_extractor.py        # <map-reduce> action → JAR, mapper/reducer classes
│   │   │   └── subworkflow_extractor.py  # <sub-workflow> → app-path for recursive analysis
│   │   ├── inventory.py           # InventoryBuilder: orchestrate parsers + Oozie + correlate
│   │   └── reporters/
│   │       ├── __init__.py
│   │       ├── json_reporter.py   # Structured JSON output with summary stats
│   │       └── csv_reporter.py    # Flattened CSV (one row per workload)
│   └── tests/
│       ├── __init__.py
│       ├── test_yarn_parser.py
│       ├── test_oozie_client.py
│       ├── test_extractors.py
│       ├── test_inventory.py
│       └── fixtures/              # Sample JSON + XML for tests
│           ├── yarn_sample.json
│           ├── spark_sample.json
│           ├── impala_sample.json
│           ├── workflow_spark.xml
│           ├── workflow_hive.xml
│           └── coordinator_sample.xml
│
└── Converter/
    ├── .claude-plugin/
    │   └── plugin.json            # Plugin manifest
    ├── README.md
    ├── resources/
    │   └── COMMON_PATTERNS.md     # Shared: HDFS→cloud paths, auth, config mapping
    ├── commands/
    │   └── convert.md             # /convert slash command for interactive use
    └── skills/
        ├── hive-ddl-to-uc/
        │   ├── SKILL.md
        │   └── references/
        │       ├── NAMESPACE_MAPPING.md
        │       ├── DDL_RULES.md
        │       ├── SERDE_MIGRATION.md
        │       └── EXAMPLES.md
        ├── spark-to-databricks/
        │   ├── SKILL.md
        │   └── references/
        │       ├── SESSION_MIGRATION.md
        │       ├── SUBMIT_TO_JOB.md
        │       ├── PATH_MIGRATION.md
        │       └── EXAMPLES.md
        ├── hive-sql-to-spark-sql/
        │   ├── SKILL.md
        │   └── references/
        │       ├── SYNTAX_RULES.md
        │       ├── UDF_MIGRATION.md
        │       └── EXAMPLES.md
        ├── sqoop-to-databricks/
        │   ├── SKILL.md
        │   └── references/
        │       ├── JDBC_PATTERNS.md
        │       ├── INCREMENTAL_PATTERNS.md
        │       └── EXAMPLES.md
        ├── hbase-to-databricks/
        │   ├── SKILL.md
        │   └── references/
        │       ├── TABLE_DESIGN.md
        │       ├── API_MIGRATION.md
        │       └── EXAMPLES.md
        └── oozie-to-databricks-workflows/
            ├── SKILL.md
            └── references/
                ├── DAG_MAPPING.md
                ├── ACTION_TYPE_MAPPING.md
                ├── COORDINATOR_MIGRATION.md
                └── EXAMPLES.md
```

---

## Analyzer: Key Design Details

### Config Format (`analyzer.conf.yaml`)

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

### Data Model (`models.py`)

```python
class WorkloadType(Enum):
    SPARK = "spark"
    HIVE = "hive"
    SQOOP = "sqoop"
    MAPREDUCE = "mapreduce"
    SHELL = "shell"
    HBASE = "hbase"
    IMPALA = "impala"
    UNKNOWN = "unknown"

@dataclass
class CodeArtifact:
    path: str                              # HDFS or local path
    location_type: str                     # "hdfs" | "local" | "embedded"
    artifact_type: str                     # "jar" | "hql" | "py" | "sh" | "xml"
    verified_exists: Optional[bool] = None # set by WebHDFS check

@dataclass
class WorkloadInventoryItem:
    workload_id: str                       # YARN app ID or Oozie action ID
    workload_name: str
    workload_type: WorkloadType
    user: str
    queue: str
    entry_point: Optional[str] = None      # main class or script name
    code_artifacts: List[CodeArtifact]     # JARs, scripts, SQL files
    dependencies: List[CodeArtifact]       # referenced JARs, configs
    oozie_workflow_id: Optional[str] = None
    oozie_workflow_name: Optional[str] = None
    oozie_app_path: Optional[str] = None   # HDFS path to workflow.xml
    yarn_app_id: Optional[str] = None
    source: str = ""                       # "yarn" | "oozie" | "spark_hs" | "impala"
    tags: List[str]                        # ["oozie-launched", "hive-initiated"]
```

### Key Oozie Correlation Logic (`inventory.py`)

YARN apps launched by Oozie have predictable name patterns:
- `oozie:launcher:T=<type>:W=<wf-name>:A=<action-name>:ID=<id>`
- Match on this pattern to correlate YARN app-level data with Oozie code-level details

Oozie workflow.xml `<action>` elements are dispatched to extractors by type:
- `<spark>` → `SparkActionExtractor` (extracts `<jar>`, `<class>`, `<spark-opts>`)
- `<hive>` / `<hive2>` → `HiveActionExtractor` (extracts `<script>`, `<query>`, `<file>`)
- `<sqoop>` → `SqoopActionExtractor` (parses `<command>` or `<arg>` for JDBC URL, tables)
- `<shell>` → `ShellActionExtractor` (extracts `<exec>`, `<file>`)
- `<map-reduce>` → `MapReduceActionExtractor` (extracts JAR from config properties)
- `<sub-workflow>` → `SubWorkflowExtractor` (extracts `<app-path>` for recursion)

Script paths in Oozie are often relative to the workflow app path — resolve to absolute HDFS paths.

### CLI Subcommands

```bash
python -m analyzer analyze --config analyzer.conf.yaml      # full pipeline
python -m analyzer parse-profiler --config analyzer.conf.yaml  # profiler output only (no Oozie)
python -m analyzer scan-oozie --config analyzer.conf.yaml      # Oozie only (no profiler)
python -m analyzer verify-paths --input inventory.json         # WebHDFS path verification
```

### Output Format (JSON)

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

---

## Converter: Skill Design

### Plugin Manifest (`plugin.json`)

```json
{
  "name": "hadoop-to-databricks-converter",
  "version": "1.0.0",
  "description": "Convert Hadoop workloads to Databricks equivalents.",
  "author": { "name": "Field Engineering" }
}
```

### Skill Summary

| Skill | Converts | Key Rules |
|-------|----------|-----------|
| `hive-ddl-to-uc` | Hive DDL → Unity Catalog DDL | 2-level→3-level namespace, STORED AS→USING DELTA, SerDe→format, LOCATION removal |
| `spark-to-databricks` | OSS Spark → Databricks Spark | Session init, spark-submit→Jobs, HDFS→DBFS/Volumes, metastore→UC |
| `hive-sql-to-spark-sql` | HiveQL → Databricks SQL | LATERAL VIEW, TRANSFORM, UDF migration, SET vars, DML syntax |
| `sqoop-to-databricks` | Sqoop → JDBC/Lakehouse Federation | import→spark.read.jdbc, export→df.write.jdbc, incremental→MERGE INTO |
| `hbase-to-databricks` | HBase → Delta tables | Row key→partition+Z-ORDER, column families→columns, API→DataFrame |
| `oozie-to-databricks-workflows` | Oozie → Databricks Workflows | DAG→multi-task job, coordinator→cron trigger, actions→task types |

### `/convert` Slash Command

Routes to appropriate skill based on first argument:
```
/convert hive-ddl <file-or-code>
/convert spark <file-or-code>
/convert hive-sql <file-or-code>
/convert sqoop <command>
/convert hbase <code>
/convert oozie <workflow.xml>
```

---

## Implementation Order

### Phase 1: Analyzer Foundation
1. Create `Analyzer/` directory structure with `__init__.py` files
2. `models.py` — data classes (shared contract for all modules)
3. `config.py` + `analyzer.conf.yaml` — YAML config loader
4. `parsers/yarn_parser.py` — parse YarnApplicationDump JSON
5. `parsers/spark_parser.py` + `parsers/impala_parser.py`
6. Test fixtures from `~/cloudera-profiler-output/Output/` sample data
7. Unit tests for all parsers
8. `requirements.txt`

### Phase 2: Oozie Connectivity
9. `connectors/oozie_client.py` — Oozie REST API client with pagination + Kerberos
10. `connectors/webhdfs_client.py` — WebHDFS path verification
11. Oozie XML test fixtures (workflow_spark.xml, workflow_hive.xml, coordinator_sample.xml)
12. Unit tests for connectors (mocked HTTP)

### Phase 3: Extractors
13. `extractors/spark_extractor.py`
14. `extractors/hive_extractor.py`
15. `extractors/sqoop_extractor.py`
16. `extractors/shell_extractor.py`
17. `extractors/mr_extractor.py`
18. `extractors/subworkflow_extractor.py`
19. Unit tests for all extractors

### Phase 4: Orchestration + CLI
20. `inventory.py` — InventoryBuilder (correlate YARN↔Oozie, deduplicate)
21. `reporters/json_reporter.py` + `reporters/csv_reporter.py`
22. `cli.py` — argparse with 4 subcommands
23. `Analyzer/README.md`

### Phase 5: Converter Plugin Scaffold
24. `Converter/.claude-plugin/plugin.json`
25. `Converter/README.md`
26. `Converter/resources/COMMON_PATTERNS.md`
27. `Converter/commands/convert.md`

### Phase 6: Converter Skills (2 per step)
28. `hive-ddl-to-uc` — SKILL.md + 4 reference files (most common conversion)
29. `hive-sql-to-spark-sql` — SKILL.md + 3 reference files
30. `spark-to-databricks` — SKILL.md + 4 reference files
31. `sqoop-to-databricks` — SKILL.md + 3 reference files
32. `oozie-to-databricks-workflows` — SKILL.md + 4 reference files (most complex)
33. `hbase-to-databricks` — SKILL.md + 3 reference files

### Phase 7: Verification
34. Run `python -m analyzer parse-profiler` against `~/cloudera-profiler-output/Output/`
35. Test each converter skill via `/convert <type> <sample-code>`
36. End-to-end: analyzer inventory → identify workload types → feed through converter skills

---

## File Count

- **Analyzer**: ~30 files (14 Python modules + 6 test files + 6 fixtures + README + config + requirements)
- **Converter**: ~35 files (6 skills × ~4 files each + plugin.json + README + COMMON_PATTERNS + convert command)
- **Total**: ~65 new files

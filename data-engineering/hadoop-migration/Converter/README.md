# Hadoop to Databricks Converter

A Claude Code plugin with 7 modular skills for converting Hadoop workloads to Databricks equivalents.

## Skills

| Skill | Converts | Description |
|-------|----------|-------------|
| `hive-ddl-to-uc` | Hive DDL → Unity Catalog DDL | Namespace mapping, STORED AS → USING DELTA, SerDe migration |
| `spark-to-databricks` | OSS Spark → Databricks Spark | Session init, legacy HiveContext, spark-submit → Jobs, HDFS → Volumes |
| `hive-sql-to-spark-sql` | HiveQL → Databricks SQL | LATERAL VIEW, TRANSFORM, UDF migration, SET vars |
| `sqoop-to-databricks` | Sqoop → JDBC/Lakehouse Federation | import → spark.read.jdbc, incremental → MERGE INTO |
| `hbase-to-databricks` | HBase → Lakebase (managed Postgres) | Row key → primary key, column families → columns, API → SQL |
| `oozie-to-databricks-workflows` | Oozie → Databricks Workflows | DAG → multi-task job, coordinator → triggers |
| `ranger-to-uc-policies` | Ranger → Unity Catalog Grants | GRANT/REVOKE mapping, row filters, column masks |

## Usage

### Via Slash Command

```
/convert hive-ddl <DDL-statement-or-file>
/convert spark <spark-code-or-file>
/convert hive-sql <hiveql-or-file>
/convert sqoop <sqoop-command>
/convert hbase <hbase-code>
/convert oozie <workflow.xml>
/convert ranger <policy-json>
```

### Direct Skill Invocation

Each skill can also be triggered by describing the conversion need naturally:
- "Convert this Hive CREATE TABLE to Unity Catalog"
- "Migrate this Spark job to Databricks"
- "Convert this Oozie workflow to Databricks Workflows"
- "Convert these Ranger policies to Unity Catalog grants"

## Paired with the Analyzer

Use the Analyzer module (`src/hadoop/Analyzer/`) to first inventory all workloads and their code artifacts, then feed the results through the appropriate converter skills:

1. Run: `python -m analyzer parse-profiler --config analyzer.conf.yaml`
2. Review the inventory JSON output
3. For each workload type, use the corresponding `/convert` command

## Testing

### Legacy PySpark codemod (cluster-setup fixtures)

Mechanical baseline converter for CDH-era PySpark (`HiveContext`, `hdfs://`, 2-part table names):

```bash
cd Converter
python3 skills/spark-to-databricks/scripts/legacy_pyspark_codemod.py \
  tests/input/spark-to-databricks/clickstream_transform.py
```

Smoke tests (input fixtures + golden outputs):

```bash
cd Converter
python3 -m unittest tests.test_legacy_pyspark_codemod -v
```

| Input | Golden output |
|-------|---------------|
| `tests/input/spark-to-databricks/clickstream_transform.py` | `tests/output/spark-to-databricks/clickstream_transform_databricks.py` |
| `tests/input/spark-to-databricks/session_metrics.py` | `tests/output/spark-to-databricks/session_metrics_databricks.py` |

The codemod applies deterministic rewrites; use `/convert spark` with `PYSPARK_MIGRATION.md` for LLM-assisted refinement.

## Shared Resources

- `resources/COMMON_PATTERNS.md` — Common migration patterns for HDFS paths, auth, config, file formats

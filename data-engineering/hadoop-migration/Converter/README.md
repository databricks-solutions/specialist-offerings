# Hadoop to Databricks Converter

A Claude Code plugin with 7 modular skills for converting Hadoop workloads to Databricks equivalents.

## Skills

| Skill | Converts | Description |
|-------|----------|-------------|
| `hive-ddl-to-uc` | Hive DDL → Unity Catalog DDL | Namespace mapping, STORED AS → USING DELTA, SerDe migration |
| `spark-to-databricks` | OSS Spark → Databricks Spark | Session init, spark-submit → Jobs, HDFS → Volumes |
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

## Shared Resources

- `resources/COMMON_PATTERNS.md` — Common migration patterns for HDFS paths, auth, config, file formats

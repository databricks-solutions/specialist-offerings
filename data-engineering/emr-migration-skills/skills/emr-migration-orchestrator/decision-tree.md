# EMR Migration Decision Tree

Use this flowchart to determine which specialized skill to invoke based on your current task.

---

## Start Here

```
What is your migration status?
│
├── "I haven't started yet" or "I need to understand my EMR environment"
│   └── Go to: ASSESSMENT
│
├── "I have an assessment and need to migrate"
│   └── Go to: CODE & DATA or INFRASTRUCTURE (based on task)
│
├── "Migration is done, I need to verify"
│   └── Go to: VALIDATION
│
└── "I'm not sure"
    └── Go to: ASSESSMENT (always start here)
```

---

## Assessment

For understanding, inventorying, and planning the migration.

```
What do you need?
│
├── "Inventory my EMR clusters, jobs, data, and costs"
│   └── /emr-migration-assessment
│       Produces: assessment report, dependency map, cost comparison
│
├── "Create a migration plan and prioritize workloads"
│   └── /emr-migration-assessment (planning mode)
│       Produces: migration plan, workload priority matrix, risk register
│
└── "Understand what will be hard to migrate"
    └── /emr-migration-assessment (risk analysis mode)
        Produces: complexity scoring, risk register, recommendations
```

**Skill:** `/emr-migration-assessment`

---

## Code & Data

For converting code, migrating catalogs, and transforming notebooks.

```
What are you migrating?
│
├── "Spark / PySpark / Scala code"
│   │
│   ├── Batch ETL jobs
│   │   └── /emr-spark-code-migration
│   │       Converts: S3 paths, Hive context, EMR-specific APIs, Delta adoption
│   │
│   └── Streaming jobs (Structured Streaming, Kafka, Kinesis)
│       └── /emr-streaming-migration
│           Converts: Kinesis connector, Kafka configs, checkpoint locations, trigger intervals
│
├── "Hive metastore or AWS Glue Data Catalog"
│   └── /emr-hive-to-unity-catalog
│       Converts: databases, tables, partitions, views, permissions → Unity Catalog
│
├── "Hive SQL / Presto SQL scripts"
│   └── /emr-spark-code-migration (SQL mode)
│       Converts: Hive DDL, UDFs, SerDe references, Presto syntax
│
├── "Notebooks (EMR Studio, Zeppelin, Jupyter)"
│   └── /emr-notebook-migration
│       Converts: .ipynb, .zpln, .json formats → Databricks .py/.sql notebooks
│
└── "Custom JARs and libraries"
    └── /emr-spark-code-migration (dependency mode)
        Handles: JAR packaging, Maven coordinates, wheel files, cluster libraries
```

**Skills:**
- `/emr-spark-code-migration` — Spark/PySpark/Scala code and SQL conversion
- `/emr-hive-to-unity-catalog` — Catalog migration (Glue/Hive to Unity Catalog)
- `/emr-notebook-migration` — Notebook format conversion
- `/emr-streaming-migration` — Streaming workload migration

---

## Infrastructure

For setting up Databricks to replace EMR compute, networking, configs, and orchestration.

```
What infrastructure task?
│
├── "Set up Databricks workspace, VPC, cluster policies"
│   └── /emr-infra-migration
│       Handles: workspace provisioning, VPC peering, Private Link,
│                cluster policies, instance pools, IAM roles → service principals
│
├── "Convert Spark / YARN / Hadoop configurations"
│   └── /emr-config-migration
│       Converts: spark-defaults.conf, yarn-site.xml, core-site.xml,
│                 hive-site.xml → Databricks cluster Spark conf & policies
│
├── "Convert EMR bootstrap actions to init scripts"
│   └── /emr-bootstrap-to-init-scripts
│       Converts: bootstrap .sh scripts → Databricks global/cluster init scripts
│       Handles: package installs, env vars, file downloads, mount points
│
└── "Convert EMR Steps / job flows / Step Functions to Databricks Workflows"
    └── /emr-steps-to-workflows
        Converts: EMR AddJobFlowSteps, Step Functions state machines,
                  Airflow EMR operators → Databricks Workflows (Jobs API 2.1)
```

**Skills:**
- `/emr-infra-migration` — Workspace, VPC, cluster policies, instance pools
- `/emr-config-migration` — Spark/YARN/Hadoop config conversion
- `/emr-bootstrap-to-init-scripts` — Bootstrap action conversion
- `/emr-steps-to-workflows` — Job orchestration conversion

---

## Validation

For verifying migrated workloads produce correct results and meet performance requirements.

```
What validation do you need?
│
├── "Compare data outputs (row counts, checksums, column values)"
│   └── /emr-migration-validation (data comparison mode)
│       Produces: row count diffs, checksum comparison, column-level spot checks
│
├── "Benchmark performance (job duration, cost, resources)"
│   └── /emr-migration-validation (performance mode)
│       Produces: duration comparison, cost comparison, resource utilization
│
├── "Run regression tests on business logic"
│   └── /emr-migration-validation (regression mode)
│       Produces: test pass/fail report, output diff analysis
│
└── "Full validation suite"
    └── /emr-migration-validation (full mode)
        Produces: comprehensive validation report covering data, performance, and regression
```

**Skill:** `/emr-migration-validation`

---

## Quick Reference Table

| I want to... | Invoke this skill |
|---|---|
| Assess my EMR environment | `/emr-migration-assessment` |
| Migrate Spark/PySpark/Scala code | `/emr-spark-code-migration` |
| Migrate Hive metastore or Glue catalog to Unity Catalog | `/emr-hive-to-unity-catalog` |
| Convert EMR Steps or job flows to Databricks Workflows | `/emr-steps-to-workflows` |
| Set up Databricks workspace and infrastructure | `/emr-infra-migration` |
| Convert Spark/YARN/Hadoop configs | `/emr-config-migration` |
| Convert bootstrap actions to init scripts | `/emr-bootstrap-to-init-scripts` |
| Migrate EMR Studio/Zeppelin/Jupyter notebooks | `/emr-notebook-migration` |
| Migrate Spark Structured Streaming or Kafka workloads | `/emr-streaming-migration` |
| Validate migrated workloads | `/emr-migration-validation` |

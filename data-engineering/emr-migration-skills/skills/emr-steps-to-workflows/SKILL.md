---
name: emr-steps-to-workflows
description: "Convert EMR Steps and job flows to Databricks Workflows/Jobs. Use when: (1) 'convert EMR steps', (2) 'EMR job flow to Databricks', (3) 'Step Functions to Databricks workflows', (4) 'Airflow EMR operators to Databricks', (5) migrating any EMR orchestration to Databricks Jobs."
---

# EMR Steps to Databricks Workflows

## Overview

EMR uses **Steps** — sequential actions executed within a job flow (cluster). Each step runs a script or JAR, and steps execute one after another with configurable failure behavior. Databricks uses **multi-task Jobs** with DAG-based dependencies, allowing parallel execution paths, conditional logic, and richer orchestration.

Key differences:
- EMR Steps are strictly sequential within a job flow; Databricks tasks form a DAG with `depends_on`
- EMR requires cluster management (create → add steps → terminate); Databricks Jobs handle cluster lifecycle automatically
- EMR job flows are imperative (API calls); Databricks Jobs can be defined declaratively via Asset Bundles

## Critical Rules

1. **Prefer Databricks Asset Bundles (DABs)** for defining workflows as code — version-controlled, environment-aware, CI/CD friendly
2. **Use job clusters** (not all-purpose clusters) for cost efficiency — clusters spin up for the job and terminate after
3. **Always add monitoring and alerting** — configure email/webhook/PagerDuty notifications on failure
4. **Map ActionOnFailure** carefully — EMR's CONTINUE/CANCEL_AND_WAIT/TERMINATE maps to Databricks retry policies and task-level failure behavior
5. **Consolidate small steps** — EMR patterns with many tiny steps can often be a single multi-task job on Databricks

## Step Type Mapping

| EMR Step Type | Databricks Task Type | Notes |
|---|---|---|
| Spark Step (spark-submit) | `spark_python_task` / `spark_jar_task` | Direct conversion — map `--class`, `--jars`, `--conf` to task parameters |
| Hive Script Step | `sql_task` | Convert HiveQL to Spark SQL; use Unity Catalog instead of HMS |
| Pig Script Step | `notebook_task` | Rewrite Pig Latin in PySpark; no Pig runtime on Databricks |
| Custom JAR Step | `spark_jar_task` | Same JAR works; verify dependencies and Spark version compatibility |
| Streaming Step | Run as continuous job | Set `max_concurrent_runs: 1`; use structured streaming |
| Shell Script Step | Not directly supported | Convert to `notebook_task` or `python_wheel_task`; use init scripts for env setup |
| s3-dist-cp Step | `notebook_task` with `dbutils.fs` | Or use Auto Loader for ongoing ingestion; for one-time copy use `dbutils.fs.cp` |

## Job Flow Conversion

An EMR **job flow** (cluster with steps) maps to a Databricks **Job** with multiple tasks:

```
EMR Job Flow:                      Databricks Job:
┌─────────────────┐                ┌─────────────────┐
│ Step 1: Ingest  │                │ Task: ingest     │
│ Step 2: Clean   │      →         │ Task: clean      │ depends_on: [ingest]
│ Step 3: Enrich  │                │ Task: enrich     │ depends_on: [clean]
│ Step 4: Export  │                │ Task: export     │ depends_on: [enrich]
└─────────────────┘                └─────────────────┘
```

With Databricks, you can also parallelize independent steps:

```
                                   ┌─────────────────┐
                                   │ Task: ingest     │
                                   └────────┬────────┘
                                   ┌────────┴────────┐
                                   │   depends_on     │
                              ┌────┴────┐       ┌────┴────┐
                              │ clean_a  │       │ clean_b  │
                              └────┬────┘       └────┬────┘
                                   │   depends_on     │
                                   └────────┬────────┘
                                   ┌────────┴────────┐
                                   │ Task: export     │
                                   └─────────────────┘
```

## Scheduling

| EMR Pattern | Databricks Equivalent |
|---|---|
| Periodic cluster launch (cron + RunJobFlow API) | Scheduled job with `trigger.periodic` or `schedule` |
| AWS Step Functions orchestration | Databricks Workflows with task dependencies |
| EventBridge rule → EMR | File arrival trigger or webhook-based trigger |
| Manual ad-hoc cluster | Interactive cluster + run job on demand |

Cron expressions are compatible — Databricks uses Quartz cron format (6 fields with seconds). See `scheduling-conversion.md` for details.

## Monitoring

| EMR (CloudWatch) | Databricks |
|---|---|
| EMR step state change events | Job run notifications (email, webhook, PagerDuty) |
| CloudWatch metrics (HDFS, YARN) | System tables: `system.lakeflow.job_run_timeline` |
| CloudWatch Logs | Driver logs in cluster UI; log delivery to S3/DBFS |
| Custom CloudWatch metrics | Ganglia metrics on cluster; custom metrics via Spark listeners |

## DABs Template Reference

Use the workflow template at:
`/Users/kishore.mannava/cursorprojects/umlaut-poc-emr-claude/templates/databricks_workflow.yml`

## Related Skills

- `emr-migration-orchestrator` — master orchestrator for end-to-end EMR migration
- `databricks-jobs` — detailed Databricks Jobs creation and management
- `databricks-asset-bundles` — DABs project setup and configuration
- `emr-config-migration` — convert Spark/YARN configs for the cluster definitions in workflows

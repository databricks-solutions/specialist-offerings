# Converting Airflow DAGs with EMR Operators to Databricks

## Overview

Many organizations use Apache Airflow to orchestrate EMR job flows. When migrating to Databricks, there are two paths:

1. **Keep Airflow, swap operators** — Replace EMR operators with Databricks operators (faster, less disruptive)
2. **Migrate to Databricks Workflows** — Eliminate Airflow entirely (simpler long-term, fewer moving parts)

This guide covers both approaches.

---

## Path 1: Swap Airflow Operators (Keep Airflow)

### Operator Mapping

| EMR Operator | Databricks Operator | Notes |
|---|---|---|
| `EmrCreateJobFlowOperator` | Not needed | Databricks manages cluster lifecycle via job clusters |
| `EmrAddStepsOperator` | `DatabricksSubmitRunOperator` | Submit a one-time run with inline task definition |
| `EmrStepSensor` | Built-in to `DatabricksRunNowOperator` | Databricks operators poll run status automatically |
| `EmrTerminateJobFlowOperator` | Not needed | Job clusters auto-terminate after task completion |
| `EmrServerlessCreateApplicationOperator` | Not needed | Databricks Serverless is managed |
| `EmrServerlessStartJobOperator` | `DatabricksSubmitRunOperator` | Same operator works for serverless |

### Connection Configuration

**Before (EMR):**
```python
# Airflow connection: aws_default
# Connection type: Amazon Web Services
# Extra: {"region_name": "us-east-1", "role_arn": "arn:aws:iam::123456789:role/emr-role"}
```

**After (Databricks):**
```python
# Airflow connection: databricks_default
# Connection type: Databricks
# Host: https://<workspace>.cloud.databricks.com
# Extra: {"token": "dapi..."} or {"use_azure_ad": true} for Azure
```

For production, use a service principal token or OAuth M2M credentials rather than a personal access token.

### Before: Airflow DAG with EMR Operators

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime

JOB_FLOW_OVERRIDES = {
    "Name": "daily-etl",
    "ReleaseLabel": "emr-6.15.0",
    "Instances": {
        "InstanceGroups": [
            {"Name": "Primary", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"Name": "Core", "InstanceRole": "CORE", "InstanceType": "m5.2xlarge", "InstanceCount": 4},
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
}

INGEST_STEP = {
    "Name": "Ingest Raw Data",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "--deploy-mode", "cluster", "s3://bucket/scripts/ingest.py"],
    },
}

TRANSFORM_STEP = {
    "Name": "Transform Data",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "--deploy-mode", "cluster", "s3://bucket/scripts/transform.py"],
    },
}

with DAG("emr_daily_etl", start_date=datetime(2024, 1, 1), schedule_interval="0 6 * * *") as dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    add_ingest_step = EmrAddStepsOperator(
        task_id="add_ingest_step",
        job_flow_id=create_cluster.output,
        steps=[INGEST_STEP],
    )

    watch_ingest = EmrStepSensor(
        task_id="watch_ingest",
        job_flow_id=create_cluster.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_ingest_step')[0] }}",
    )

    add_transform_step = EmrAddStepsOperator(
        task_id="add_transform_step",
        job_flow_id=create_cluster.output,
        steps=[TRANSFORM_STEP],
    )

    watch_transform = EmrStepSensor(
        task_id="watch_transform",
        job_flow_id=create_cluster.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_transform_step')[0] }}",
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_cluster.output,
        trigger_rule="all_done",
    )

    create_cluster >> add_ingest_step >> watch_ingest >> add_transform_step >> watch_transform >> terminate_cluster
```

### After: Airflow DAG with Databricks Operators

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from datetime import datetime

# Shared cluster definition (replaces EMR JOB_FLOW_OVERRIDES)
NEW_CLUSTER = {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "m5.2xlarge",
    "autoscale": {"min_workers": 2, "max_workers": 8},
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        "zone_id": "us-east-1a",
    },
}

with DAG("databricks_daily_etl", start_date=datetime(2024, 1, 1), schedule_interval="0 6 * * *") as dag:

    ingest = DatabricksSubmitRunOperator(
        task_id="ingest_raw_data",
        databricks_conn_id="databricks_default",
        new_cluster=NEW_CLUSTER,
        spark_python_task={
            "python_file": "/Workspace/Repos/etl/ingest.py",
        },
    )

    transform = DatabricksSubmitRunOperator(
        task_id="transform_data",
        databricks_conn_id="databricks_default",
        new_cluster=NEW_CLUSTER,
        spark_python_task={
            "python_file": "/Workspace/Repos/etl/transform.py",
        },
    )

    # No create_cluster, no sensors, no terminate — all handled automatically
    ingest >> transform
```

### Key Simplifications

1. **No cluster lifecycle management** — `EmrCreateJobFlowOperator` and `EmrTerminateJobFlowOperator` are eliminated. Job clusters auto-create and auto-terminate.
2. **No sensors** — `DatabricksSubmitRunOperator` blocks until the run completes (or fails). No need for `EmrStepSensor`.
3. **Fewer tasks** — The 6-task DAG reduces to 2 tasks.
4. **Use `DatabricksRunNowOperator`** for pre-defined jobs — if the job already exists in Databricks, trigger it by job ID.

### Using DatabricksRunNowOperator (for pre-existing jobs)

```python
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

run_job = DatabricksRunNowOperator(
    task_id="run_existing_job",
    databricks_conn_id="databricks_default",
    job_id=12345,  # Pre-existing Databricks job ID
    notebook_params={"date": "{{ ds }}"},
)
```

---

## Path 2: Migrate Entirely to Databricks Workflows

This eliminates Airflow and uses Databricks Workflows (Jobs) as the sole orchestrator.

### When to Choose This Path

- Airflow is only used for EMR orchestration (no other systems)
- You want fewer infrastructure components to manage
- The DAG complexity is within Databricks Workflows capabilities (DAG with dependencies, retries, conditional logic)
- You want native integration with Unity Catalog, Delta, and Databricks monitoring

### Airflow Concepts to Databricks Workflows Concepts

| Airflow | Databricks Workflows | Notes |
|---|---|---|
| DAG | Job | Top-level orchestration unit |
| Task | Task | Individual unit of work |
| `>>` operator (dependencies) | `depends_on` | DAG dependencies |
| `trigger_rule` | `run_if` | Conditional execution |
| `schedule_interval` / `timetable` | `schedule` with `quartz_cron_expression` | Cron-based scheduling |
| `execution_date` / `data_interval_start` | `{{job.start_time}}` or task parameters | Use job parameters for date references |
| XComs | Task values | Pass data between tasks |
| Variables / Connections | Databricks secrets / job parameters | Configuration management |
| Sensors | File arrival triggers | Event-based triggering |
| Pools | Cluster pools / max_concurrent_runs | Concurrency control |
| SLA | Timeout + notifications | SLA monitoring |
| `on_failure_callback` | Email/webhook notifications | Failure handling |

### Converted Databricks Workflow (DABs YAML)

```yaml
resources:
  jobs:
    daily_etl_pipeline:
      name: "Daily ETL Pipeline"

      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"

      parameters:
        - name: process_date
          default: ""  # Empty = use current date

      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "m5.2xlarge"
            autoscale:
              min_workers: 2
              max_workers: 8

      tasks:
        - task_key: ingest_raw_data
          spark_python_task:
            python_file: /Workspace/Repos/etl/ingest.py
            parameters:
              - "--date"
              - "{{job.parameters.process_date}}"
          job_cluster_key: etl_cluster

        - task_key: transform_data
          depends_on:
            - task_key: ingest_raw_data
          spark_python_task:
            python_file: /Workspace/Repos/etl/transform.py
            parameters:
              - "--date"
              - "{{job.parameters.process_date}}"
          job_cluster_key: etl_cluster
          max_retries: 1

      email_notifications:
        on_failure:
          - "team@company.com"

      max_concurrent_runs: 1
```

### Limitations of Databricks Workflows vs Airflow

- No cross-system orchestration (Airflow can trigger non-Databricks systems)
- No built-in backfill mechanism (Airflow has `catchup=True` and CLI backfill)
- Limited branching logic (Airflow has `BranchPythonOperator`; Databricks uses `run_if` conditions)
- No dynamic task generation at runtime (Airflow has dynamic task mapping)

If you need these capabilities, stay on Path 1 (Airflow with Databricks operators).

---

## Migration Checklist

- [ ] Inventory all Airflow DAGs that use EMR operators
- [ ] Choose path per DAG: swap operators vs. migrate to Workflows
- [ ] Update Airflow connections (aws_default to databricks_default)
- [ ] Install `apache-airflow-providers-databricks` package
- [ ] Convert EMR cluster configs to Databricks cluster specs
- [ ] Replace EMR operators with Databricks operators (Path 1)
- [ ] Or create Databricks Jobs via DABs (Path 2)
- [ ] Validate outputs match (see `emr-migration-validation` skill)
- [ ] Run in parallel (Airflow+EMR alongside new setup) for at least 1 week
- [ ] Decommission Airflow EMR DAGs after validation

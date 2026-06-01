---
name: emr-migration-assessment
description: "Analyze existing EMR clusters, jobs, costs, and dependencies for migration to Databricks. Use when: (1) 'assess EMR', (2) 'EMR inventory', (3) 'migration assessment', (4) 'analyze EMR clusters', (5) 'EMR cost analysis', (6) 'what EMR resources do we have'. Provides boto3/AWS CLI commands to extract EMR metadata."
---

# EMR Migration Assessment

## Overview

This skill is the **first step in any EMR-to-Databricks migration**. Before writing a single line of migration code, you need a complete picture of what exists in the source environment. This assessment covers:

1. **Cluster Inventory** -- every EMR cluster (active, waiting, terminated), its configuration, instance types, and software stack
2. **Job Flow Analysis** -- all steps, scripts, JARs, and scheduling mechanisms running on those clusters
3. **Data Catalog Inventory** -- Glue databases, tables, partitions, and their schemas
4. **Cost Analysis** -- current EMR spend and projected Databricks costs
5. **Dependency Mapping** -- job chains, data lineage, and external integrations

The output is a structured assessment report that feeds into the migration planning phase.

## Quick Start

Run the full assessment script:

```bash
cd /Users/kishore.mannava/cursorprojects/umlaut-poc-emr-claude
python scripts/assess_emr_cluster.py --region us-east-1 --output assessment-report.json
```

Or use the boto3 commands below to extract metadata interactively.

## Cluster Inventory

Use boto3 to enumerate all EMR clusters across states:

```python
import boto3
from datetime import datetime, timedelta

emr = boto3.client("emr", region_name="us-east-1")

def list_all_clusters(days_back=90):
    """List all EMR clusters created in the last N days, across all states."""
    states = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING",
              "TERMINATED", "TERMINATED_WITH_ERRORS"]
    created_after = datetime.utcnow() - timedelta(days=days_back)
    clusters = []

    for state in states:
        paginator = emr.get_paginator("list_clusters")
        for page in paginator.paginate(
            ClusterStates=[state],
            CreatedAfter=created_after
        ):
            clusters.extend(page.get("Clusters", []))
    return clusters

clusters = list_all_clusters()
print(f"Found {len(clusters)} clusters")
```

Describe each cluster to get full configuration:

```python
def describe_cluster_full(cluster_id):
    """Extract complete cluster configuration."""
    cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]

    # Instance groups
    instance_groups = emr.list_instance_groups(ClusterId=cluster_id).get("InstanceGroups", [])

    # Bootstrap actions
    bootstraps = emr.list_bootstrap_actions(ClusterId=cluster_id).get("BootstrapActions", [])

    # Steps (job history)
    steps = emr.list_steps(ClusterId=cluster_id).get("Steps", [])

    return {
        "cluster_id": cluster_id,
        "name": cluster.get("Name"),
        "status": cluster["Status"]["State"],
        "release_label": cluster.get("ReleaseLabel"),
        "applications": [a["Name"] for a in cluster.get("Applications", [])],
        "instance_groups": instance_groups,
        "bootstrap_actions": bootstraps,
        "configurations": cluster.get("Configurations", []),
        "security_config": cluster.get("SecurityConfiguration"),
        "auto_scaling_role": cluster.get("AutoScalingRole"),
        "custom_ami": cluster.get("CustomAmiId"),
        "log_uri": cluster.get("LogUri"),
        "tags": cluster.get("Tags", []),
        "steps_count": len(steps),
    }
```

Extract Spark, YARN, Hive, and EMRFS configurations:

```python
def extract_configurations(configurations):
    """Parse nested EMR configuration objects into a flat dict."""
    config_map = {}
    for config in configurations:
        classification = config.get("Classification", "unknown")
        properties = config.get("Properties", {})
        config_map[classification] = properties
        # Recurse into nested configurations
        for nested in config.get("Configurations", []):
            nested_class = nested.get("Classification", "unknown")
            config_map[f"{classification}/{nested_class}"] = nested.get("Properties", {})
    return config_map

# Key classifications to look for:
# - spark-defaults: Spark tuning parameters
# - yarn-site: YARN resource manager settings
# - hive-site: Hive metastore and execution config
# - emrfs-site: S3 consistency and encryption settings
# - core-site: Hadoop core settings (fs.defaultFS, etc.)
```

## Job Flow Analysis

Extract step definitions and execution history:

```python
def analyze_job_flows(cluster_id):
    """Extract all steps and their configurations from a cluster."""
    paginator = emr.get_paginator("list_steps")
    steps = []
    for page in paginator.paginate(ClusterId=cluster_id):
        for step in page.get("Steps", []):
            step_detail = {
                "name": step["Name"],
                "status": step["Status"]["State"],
                "type": step["Config"]["Jar"],
                "args": step["Config"].get("Args", []),
                "action_on_failure": step.get("ActionOnFailure"),
                "created": str(step["Status"]["Timeline"].get("CreationDateTime")),
                "started": str(step["Status"]["Timeline"].get("StartDateTime")),
                "ended": str(step["Status"]["Timeline"].get("EndDateTime")),
            }
            # Classify step type
            jar = step["Config"]["Jar"]
            if "command-runner.jar" in jar:
                if step["Config"].get("Args") and step["Config"]["Args"][0] == "spark-submit":
                    step_detail["step_type"] = "spark"
                elif step["Config"].get("Args") and step["Config"]["Args"][0] == "hive-script":
                    step_detail["step_type"] = "hive"
                elif step["Config"].get("Args") and step["Config"]["Args"][0] == "pig-script":
                    step_detail["step_type"] = "pig"
                else:
                    step_detail["step_type"] = "command-runner"
            elif "s3://" in jar:
                step_detail["step_type"] = "custom-jar"
            else:
                step_detail["step_type"] = "other"
            steps.append(step_detail)
    return steps
```

## Data Catalog Inventory

Enumerate the Glue Data Catalog (EMR's default metastore):

```python
glue = boto3.client("glue", region_name="us-east-1")

def inventory_glue_catalog():
    """List all Glue databases, tables, and partition counts."""
    catalog = {}
    paginator = glue.get_paginator("get_databases")
    for page in paginator.paginate():
        for db in page["DatabaseList"]:
            db_name = db["Name"]
            tables = []
            table_paginator = glue.get_paginator("get_tables")
            for t_page in table_paginator.paginate(DatabaseName=db_name):
                for table in t_page["TableList"]:
                    partition_count = 0
                    try:
                        partitions = glue.get_paginator("get_partitions")
                        for p_page in partitions.paginate(
                            DatabaseName=db_name, TableName=table["Name"]
                        ):
                            partition_count += len(p_page.get("Partitions", []))
                    except Exception:
                        pass
                    tables.append({
                        "name": table["Name"],
                        "location": table.get("StorageDescriptor", {}).get("Location"),
                        "input_format": table.get("StorageDescriptor", {}).get("InputFormat"),
                        "serde": table.get("StorageDescriptor", {}).get("SerdeInfo", {}).get("SerializationLibrary"),
                        "columns": len(table.get("StorageDescriptor", {}).get("Columns", [])),
                        "partition_keys": [p["Name"] for p in table.get("PartitionKeys", [])],
                        "partition_count": partition_count,
                    })
            catalog[db_name] = tables
    return catalog
```

## Cost Analysis

See [cost-estimation.md](cost-estimation.md) for the full cost comparison framework. The high-level approach:

1. **Pull EMR billing data** from AWS Cost Explorer with `Service = "ElasticMapReduce"` filter
2. **Break down by component**: EC2 hours, EMR surcharge, EBS, S3, data transfer
3. **Map instance types** to Databricks DBU consumption rates
4. **Calculate projected Databricks cost** using the DBU mapping and actual utilization metrics
5. **Compare TCO** side by side

```python
ce = boto3.client("ce", region_name="us-east-1")

def get_emr_costs(start_date, end_date):
    """Retrieve EMR-specific costs from Cost Explorer."""
    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start_date, "End": end_date},
        Granularity="MONTHLY",
        Metrics=["BlendedCost", "UsageQuantity"],
        Filter={
            "Dimensions": {
                "Key": "SERVICE",
                "Values": ["Amazon Elastic MapReduce"]
            }
        },
        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}]
    )
    return response["ResultsByTime"]
```

## Dependency Mapping

See [dependency-mapping.md](dependency-mapping.md) for techniques to discover:

- Job chains (Step Functions, Airflow DAGs, cron)
- Data lineage (S3 path analysis across jobs)
- Glue Catalog cross-references
- External system integrations (databases, APIs, queues)

## Output Template

The assessment produces a structured report:

```json
{
  "assessment_metadata": {
    "generated_at": "2026-04-12T00:00:00Z",
    "region": "us-east-1",
    "days_analyzed": 90
  },
  "cluster_inventory": [
    {
      "cluster_id": "j-XXXXXXXXXXXXX",
      "name": "Production ETL",
      "status": "WAITING",
      "release_label": "emr-6.15.0",
      "applications": ["Spark", "Hive", "Presto"],
      "instance_groups": [],
      "configurations": {},
      "tags": [],
      "steps_summary": {
        "total": 150,
        "by_type": {"spark": 120, "hive": 25, "custom-jar": 5}
      }
    }
  ],
  "glue_catalog": {
    "database_count": 5,
    "table_count": 120,
    "total_partitions": 50000,
    "databases": {}
  },
  "cost_summary": {
    "monthly_emr_cost": 5000.00,
    "cost_by_component": {},
    "projected_databricks_cost": 4200.00
  },
  "dependencies": {
    "job_chains": [],
    "data_lineage": [],
    "external_systems": []
  },
  "migration_complexity": "medium",
  "recommendations": []
}
```

## Helper Script

The full automated assessment script is located at:

```
/Users/kishore.mannava/cursorprojects/umlaut-poc-emr-claude/scripts/assess_emr_cluster.py
```

Run it with `--help` to see all options including region selection, output format, and filtering.

## Related Skills

- **emr-migration-orchestrator** -- end-to-end migration workflow coordination
- **emr-infra-migration** -- Terraform/DAB infrastructure conversion (EMR clusters to Databricks workspaces)
- **emr-config-migration** -- Spark/Hive/YARN config translation to Databricks runtime settings

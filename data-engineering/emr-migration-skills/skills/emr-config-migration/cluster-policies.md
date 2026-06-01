# Creating Databricks Cluster Policies from EMR Configurations

## Overview

EMR cluster configurations (instance types, scaling, bootstrap actions, security) map to Databricks **cluster policies**. Cluster policies are JSON documents that constrain what users can configure when creating clusters, ensuring governance, cost control, and standardization.

## Why Cluster Policies?

On EMR, governance is achieved through:
- IAM permissions (who can create clusters)
- YARN queue capacity limits (resource allocation)
- EMR security configurations (encryption, auth)
- Service Catalog or Terraform (standardized cluster templates)

On Databricks, **cluster policies** consolidate all of this:
- Who can use which instance types and sizes
- Min/max autoscaling range
- Required Spark version
- Auto-termination rules
- Required tags
- Allowed configurations

## Building a Policy from EMR Config

### Step 1: Extract EMR Cluster Configuration

```json
{
  "Name": "Production ETL Cluster",
  "ReleaseLabel": "emr-6.15.0",
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Primary",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.2xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Core",
        "InstanceRole": "CORE",
        "InstanceType": "m5.4xlarge",
        "InstanceCount": 4,
        "AutoScalingPolicy": {
          "Rules": [
            {
              "Name": "ScaleOut",
              "Action": { "SimpleScalingPolicyConfiguration": { "AdjustmentType": "CHANGE_IN_CAPACITY", "ScalingAdjustment": 2 } },
              "Trigger": { "CloudWatchAlarmDefinition": { "MetricName": "YARNMemoryAvailablePercentage", "Threshold": 15 } }
            }
          ],
          "Constraints": { "MinCapacity": 4, "MaxCapacity": 20 }
        }
      }
    ]
  },
  "Tags": [
    { "Key": "team", "Value": "data-engineering" },
    { "Key": "environment", "Value": "production" }
  ],
  "AutoTerminationPolicy": { "IdleTimeout": 3600 }
}
```

### Step 2: Map to Cluster Policy

```json
{
  "name": "Production ETL Policy",
  "description": "Migrated from EMR Production ETL Cluster configuration",
  "definition": {
    "spark_version": {
      "type": "regex",
      "pattern": "15\\.[0-9]+\\.x-scala2\\.12",
      "defaultValue": "15.4.x-scala2.12"
    },
    "node_type_id": {
      "type": "allowlist",
      "values": [
        "m5.2xlarge",
        "m5.4xlarge",
        "m6i.2xlarge",
        "m6i.4xlarge"
      ],
      "defaultValue": "m5.4xlarge"
    },
    "driver_node_type_id": {
      "type": "allowlist",
      "values": ["m5.2xlarge", "m6i.2xlarge"],
      "defaultValue": "m5.2xlarge"
    },
    "autoscale.min_workers": {
      "type": "range",
      "minValue": 2,
      "maxValue": 4,
      "defaultValue": 4
    },
    "autoscale.max_workers": {
      "type": "range",
      "minValue": 4,
      "maxValue": 20,
      "defaultValue": 12
    },
    "autotermination_minutes": {
      "type": "range",
      "minValue": 15,
      "maxValue": 120,
      "defaultValue": 60
    },
    "custom_tags.team": {
      "type": "fixed",
      "value": "data-engineering"
    },
    "custom_tags.environment": {
      "type": "fixed",
      "value": "production"
    },
    "aws_attributes.availability": {
      "type": "allowlist",
      "values": ["SPOT_WITH_FALLBACK", "ON_DEMAND"],
      "defaultValue": "SPOT_WITH_FALLBACK"
    },
    "aws_attributes.first_on_demand": {
      "type": "fixed",
      "value": 1
    },
    "spark_conf.spark.sql.adaptive.enabled": {
      "type": "fixed",
      "value": "true"
    },
    "spark_conf.spark.serializer": {
      "type": "fixed",
      "value": "org.apache.spark.serializer.KryoSerializer"
    }
  }
}
```

### Step 3: Deploy the Policy

**Databricks CLI:**

```bash
databricks cluster-policies create --json '{
  "name": "Production ETL Policy",
  "definition": "{...}"
}'
```

**Terraform:**

```hcl
resource "databricks_cluster_policy" "production_etl" {
  name = "Production ETL Policy"
  definition = jsonencode({
    "spark_version" = {
      "type"         = "regex"
      "pattern"      = "15\\.[0-9]+\\.x-scala2\\.12"
      "defaultValue" = "15.4.x-scala2.12"
    }
    "node_type_id" = {
      "type"         = "allowlist"
      "values"       = ["m5.2xlarge", "m5.4xlarge", "m6i.2xlarge", "m6i.4xlarge"]
      "defaultValue" = "m5.4xlarge"
    }
    "driver_node_type_id" = {
      "type"         = "allowlist"
      "values"       = ["m5.2xlarge", "m6i.2xlarge"]
      "defaultValue" = "m5.2xlarge"
    }
    "autoscale.min_workers" = {
      "type"         = "range"
      "minValue"     = 2
      "maxValue"     = 4
      "defaultValue" = 4
    }
    "autoscale.max_workers" = {
      "type"         = "range"
      "minValue"     = 4
      "maxValue"     = 20
      "defaultValue" = 12
    }
    "autotermination_minutes" = {
      "type"         = "range"
      "minValue"     = 15
      "maxValue"     = 120
      "defaultValue" = 60
    }
    "custom_tags.team" = {
      "type"  = "fixed"
      "value" = "data-engineering"
    }
    "custom_tags.environment" = {
      "type"  = "fixed"
      "value" = "production"
    }
    "aws_attributes.availability" = {
      "type"         = "allowlist"
      "values"       = ["SPOT_WITH_FALLBACK", "ON_DEMAND"]
      "defaultValue" = "SPOT_WITH_FALLBACK"
    }
    "aws_attributes.first_on_demand" = {
      "type"  = "fixed"
      "value" = 1
    }
  })
}

# Grant policy to a group
resource "databricks_permissions" "production_etl_policy" {
  cluster_policy_id = databricks_cluster_policy.production_etl.id

  access_control {
    group_name       = "data-engineering"
    permission_level = "CAN_USE"
  }
}
```

## Common Policy Templates

### Template 1: Cost-Controlled Development

For development/testing with tight cost controls.

```json
{
  "name": "Development Policy",
  "definition": {
    "spark_version": {
      "type": "regex",
      "pattern": "1[4-9]\\.[0-9]+\\.x-scala2\\.12"
    },
    "node_type_id": {
      "type": "allowlist",
      "values": ["m5.xlarge", "m5.2xlarge"],
      "defaultValue": "m5.xlarge"
    },
    "num_workers": {
      "type": "range",
      "minValue": 0,
      "maxValue": 4,
      "defaultValue": 1
    },
    "autotermination_minutes": {
      "type": "range",
      "minValue": 10,
      "maxValue": 60,
      "defaultValue": 30
    },
    "custom_tags.cost_center": {
      "type": "fixed",
      "value": "development"
    }
  }
}
```

### Template 2: Memory-Optimized Workloads

For workloads that required r5 instances on EMR.

```json
{
  "name": "Memory-Optimized Policy",
  "definition": {
    "node_type_id": {
      "type": "allowlist",
      "values": ["r5.xlarge", "r5.2xlarge", "r5.4xlarge", "r6i.xlarge", "r6i.2xlarge"],
      "defaultValue": "r5.2xlarge"
    },
    "autoscale.min_workers": {
      "type": "range",
      "minValue": 2,
      "maxValue": 8
    },
    "autoscale.max_workers": {
      "type": "range",
      "minValue": 4,
      "maxValue": 30
    },
    "runtime_engine": {
      "type": "fixed",
      "value": "PHOTON"
    }
  }
}
```

### Template 3: Streaming Workloads

For always-on structured streaming jobs migrated from EMR.

```json
{
  "name": "Streaming Policy",
  "definition": {
    "node_type_id": {
      "type": "allowlist",
      "values": ["m5.2xlarge", "m5.4xlarge", "c5.2xlarge", "c5.4xlarge"]
    },
    "autoscale.min_workers": {
      "type": "range",
      "minValue": 2,
      "maxValue": 4,
      "defaultValue": 2
    },
    "autoscale.max_workers": {
      "type": "range",
      "minValue": 4,
      "maxValue": 16,
      "defaultValue": 8
    },
    "autotermination_minutes": {
      "type": "fixed",
      "value": 0,
      "hidden": true
    },
    "aws_attributes.availability": {
      "type": "fixed",
      "value": "ON_DEMAND"
    },
    "custom_tags.workload_type": {
      "type": "fixed",
      "value": "streaming"
    }
  }
}
```

## Policy Attribute Reference

| Attribute | Type Options | Description |
|---|---|---|
| `spark_version` | fixed, regex, allowlist | Control allowed DBR versions |
| `node_type_id` | fixed, allowlist | Control allowed worker instance types |
| `driver_node_type_id` | fixed, allowlist | Control allowed driver instance types |
| `num_workers` | fixed, range | Fixed number of workers (no autoscaling) |
| `autoscale.min_workers` | fixed, range | Minimum workers with autoscaling |
| `autoscale.max_workers` | fixed, range | Maximum workers with autoscaling |
| `autotermination_minutes` | fixed, range | Auto-terminate idle cluster |
| `custom_tags.*` | fixed | Required tags for cost allocation |
| `spark_conf.*` | fixed | Lock specific Spark configs |
| `aws_attributes.availability` | fixed, allowlist | SPOT, ON_DEMAND, SPOT_WITH_FALLBACK |
| `aws_attributes.first_on_demand` | fixed, range | Number of on-demand nodes (driver) |
| `runtime_engine` | fixed | STANDARD or PHOTON |
| `instance_pool_id` | fixed | Require use of a specific pool |

## Migration Checklist

- [ ] Inventory all EMR cluster configurations (instance types, scaling, tags, security)
- [ ] Group clusters by workload type (ETL, interactive, streaming, ML)
- [ ] Create one cluster policy per workload type
- [ ] Map EMR instance types to Databricks allowlists
- [ ] Map EMR autoscaling rules to autoscale min/max ranges
- [ ] Map EMR tags to custom_tags (fixed values)
- [ ] Set auto-termination defaults (EMR IdleTimeout to autotermination_minutes)
- [ ] Grant policies to appropriate groups
- [ ] Validate users can create clusters with the policies
- [ ] Monitor cluster creation and cost to tune policies

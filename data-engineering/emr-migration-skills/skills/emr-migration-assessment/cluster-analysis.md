# Cluster Analysis -- Detailed boto3 Code Examples

This document provides comprehensive boto3 code for extracting every detail about EMR clusters needed for migration planning.

## Listing Clusters with State Filtering

```python
import boto3
import json
from datetime import datetime, timedelta

emr = boto3.client("emr", region_name="us-east-1")

def list_clusters_by_state(states, days_back=90):
    """
    List EMR clusters filtered by state.

    States: STARTING, BOOTSTRAPPING, RUNNING, WAITING,
            TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS
    """
    created_after = datetime.utcnow() - timedelta(days=days_back)
    clusters = []
    paginator = emr.get_paginator("list_clusters")

    for page in paginator.paginate(
        ClusterStates=states,
        CreatedAfter=created_after,
    ):
        for c in page.get("Clusters", []):
            clusters.append({
                "id": c["Id"],
                "name": c["Name"],
                "state": c["Status"]["State"],
                "created": str(c["Status"]["Timeline"]["CreationDateTime"]),
                "normalized_hours": c.get("NormalizedInstanceHours", 0),
            })
    return clusters


# Active clusters (currently consuming resources)
active = list_clusters_by_state(["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"])

# Recently terminated (for historical analysis)
terminated = list_clusters_by_state(["TERMINATED", "TERMINATED_WITH_ERRORS"], days_back=180)

# All clusters
all_clusters = active + terminated
print(f"Active: {len(active)}, Terminated: {len(terminated)}, Total: {len(all_clusters)}")
```

## Full Cluster Description

```python
def describe_cluster(cluster_id):
    """Get the complete cluster description from the EMR API."""
    response = emr.describe_cluster(ClusterId=cluster_id)
    cluster = response["Cluster"]
    return {
        "id": cluster["Id"],
        "name": cluster["Name"],
        "state": cluster["Status"]["State"],
        "state_change_reason": cluster["Status"].get("StateChangeReason", {}),
        "release_label": cluster.get("ReleaseLabel"),
        "applications": [
            {"name": a["Name"], "version": a.get("Version", "N/A")}
            for a in cluster.get("Applications", [])
        ],
        "log_uri": cluster.get("LogUri"),
        "log_encryption_kms_key_id": cluster.get("LogEncryptionKmsKeyId"),
        "requested_ami_version": cluster.get("RequestedAmiVersion"),
        "running_ami_version": cluster.get("RunningAmiVersion"),
        "custom_ami_id": cluster.get("CustomAmiId"),
        "auto_terminate": cluster.get("AutoTerminate", False),
        "termination_protected": cluster.get("TerminationProtected", False),
        "visible_to_all_users": cluster.get("VisibleToAllUsers", True),
        "service_role": cluster.get("ServiceRole"),
        "ec2_instance_attributes": cluster.get("Ec2InstanceAttributes", {}),
        "normalized_instance_hours": cluster.get("NormalizedInstanceHours", 0),
        "master_public_dns": cluster.get("MasterPublicDnsName"),
        "configurations": cluster.get("Configurations", []),
        "security_configuration": cluster.get("SecurityConfiguration"),
        "auto_scaling_role": cluster.get("AutoScalingRole"),
        "scale_down_behavior": cluster.get("ScaleDownBehavior"),
        "kerberos_attributes": cluster.get("KerberosAttributes"),
        "tags": {t["Key"]: t["Value"] for t in cluster.get("Tags", [])},
        "ebs_root_volume_size": cluster.get("EbsRootVolumeSize"),
        "step_concurrency_level": cluster.get("StepConcurrencyLevel", 1),
        "managed_scaling_policy": cluster.get("ManagedScalingPolicy"),
        "os_release_label": cluster.get("OSReleaseLabel"),
    }
```

## Instance Groups

```python
def list_instance_groups(cluster_id):
    """Get detailed instance group information for capacity planning."""
    response = emr.list_instance_groups(ClusterId=cluster_id)
    groups = []
    for ig in response.get("InstanceGroups", []):
        groups.append({
            "id": ig["Id"],
            "name": ig.get("Name"),
            "market": ig.get("Market"),  # ON_DEMAND or SPOT
            "instance_group_type": ig["InstanceGroupType"],  # MASTER, CORE, TASK
            "instance_type": ig["InstanceType"],
            "requested_count": ig.get("RequestedInstanceCount", 0),
            "running_count": ig.get("RunningInstanceCount", 0),
            "bid_price": ig.get("BidPrice"),  # For SPOT instances
            "ebs_config": ig.get("EbsBlockDevices", []),
            "auto_scaling_policy": ig.get("AutoScalingPolicy"),
            "configurations": ig.get("Configurations", []),
            "shrink_policy": ig.get("ShrinkPolicy"),
        })
    return groups
```

## Instance Fleets (alternative to Instance Groups)

```python
def list_instance_fleets(cluster_id):
    """Get instance fleet details (used instead of instance groups on some clusters)."""
    try:
        response = emr.list_instance_fleets(ClusterId=cluster_id)
        fleets = []
        for fleet in response.get("InstanceFleets", []):
            fleets.append({
                "id": fleet["Id"],
                "name": fleet.get("Name"),
                "fleet_type": fleet["InstanceFleetType"],  # MASTER, CORE, TASK
                "target_on_demand": fleet.get("TargetOnDemandCapacity", 0),
                "target_spot": fleet.get("TargetSpotCapacity", 0),
                "provisioned_on_demand": fleet.get("ProvisionedOnDemandCapacity", 0),
                "provisioned_spot": fleet.get("ProvisionedSpotCapacity", 0),
                "instance_type_specifications": fleet.get("InstanceTypeSpecifications", []),
                "launch_specifications": fleet.get("LaunchSpecifications"),
            })
        return fleets
    except emr.exceptions.InvalidRequestException:
        # Cluster uses instance groups, not fleets
        return None
```

## Extracting Spark, YARN, Hive, and EMRFS Configurations

```python
def extract_all_configurations(cluster_id):
    """
    Parse the nested EMR configuration tree into a flat, searchable structure.

    Key classifications for migration:
    - spark-defaults: spark.executor.memory, spark.driver.memory, etc.
    - spark: spark.dynamicAllocation.enabled, etc.
    - yarn-site: yarn.nodemanager.resource.memory-mb, etc.
    - hive-site: hive.metastore.uris, javax.jdo.option.ConnectionURL, etc.
    - emrfs-site: fs.s3.consistent, fs.s3.consistent.retryCount, etc.
    - core-site: fs.defaultFS, io.compression.codecs, etc.
    - hadoop-env: HADOOP_HEAPSIZE, etc.
    - spark-env: PYSPARK_PYTHON, etc.
    """
    cluster = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
    configurations = cluster.get("Configurations", [])

    flat_config = {}

    def flatten(configs, prefix=""):
        for config in configs:
            classification = config.get("Classification", "unknown")
            full_key = f"{prefix}/{classification}" if prefix else classification
            properties = config.get("Properties", {})
            if properties:
                flat_config[full_key] = properties
            # Recurse into nested configurations
            nested = config.get("Configurations", [])
            if nested:
                flatten(nested, prefix=full_key)

    flatten(configurations)
    return flat_config


def get_migration_critical_configs(flat_config):
    """Extract configurations that are critical for Databricks migration."""
    critical = {}

    # Spark defaults -- need to be translated to Databricks cluster spark conf
    if "spark-defaults" in flat_config:
        critical["spark_defaults"] = flat_config["spark-defaults"]

    # Spark configuration
    if "spark" in flat_config:
        critical["spark"] = flat_config["spark"]

    # YARN settings -- inform Databricks cluster sizing
    if "yarn-site" in flat_config:
        critical["yarn_site"] = flat_config["yarn-site"]

    # Hive metastore -- determines if external metastore migration is needed
    if "hive-site" in flat_config:
        critical["hive_site"] = flat_config["hive-site"]

    # EMRFS -- S3 consistency and encryption settings
    if "emrfs-site" in flat_config:
        critical["emrfs_site"] = flat_config["emrfs-site"]

    # Core site -- filesystem defaults
    if "core-site" in flat_config:
        critical["core_site"] = flat_config["core-site"]

    return critical
```

## Bootstrap Actions

```python
def list_bootstrap_actions(cluster_id):
    """
    Extract bootstrap actions -- these often install custom software
    that needs equivalent handling in Databricks init scripts.
    """
    paginator = emr.get_paginator("list_bootstrap_actions")
    actions = []
    for page in paginator.paginate(ClusterId=cluster_id):
        for action in page.get("BootstrapActions", []):
            actions.append({
                "name": action["Name"],
                "script_path": action["ScriptPath"],
                "args": action.get("Args", []),
            })
    return actions
```

## Security Configuration

```python
def get_security_configuration(config_name):
    """Retrieve the named security configuration for encryption and auth details."""
    if not config_name:
        return None
    try:
        response = emr.describe_security_configuration(Name=config_name)
        return {
            "name": response["Name"],
            "created": str(response["CreationDateTime"]),
            "config": json.loads(response["SecurityConfiguration"]),
        }
    except Exception as e:
        return {"name": config_name, "error": str(e)}
```

## CloudWatch Metrics for Utilization

```python
cloudwatch = boto3.client("cloudwatch", region_name="us-east-1")

def get_cluster_utilization(cluster_id, days_back=7):
    """
    Pull CloudWatch metrics to understand actual cluster utilization.
    This is critical for right-sizing the Databricks replacement.
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days_back)

    metrics = {}
    metric_names = [
        ("YARNMemoryAvailablePercentage", "Percent"),
        ("ContainerAllocated", "Count"),
        ("ContainerPending", "Count"),
        ("AppsRunning", "Count"),
        ("AppsPending", "Count"),
        ("HDFSUtilization", "Percent"),
        ("IsIdle", "None"),
        ("CoreNodesRunning", "Count"),
        ("TaskNodesRunning", "Count"),
        ("MRActiveNodes", "Count"),
    ]

    for metric_name, unit in metric_names:
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace="AWS/ElasticMapReduce",
                MetricName=metric_name,
                Dimensions=[{"Name": "JobFlowId", "Value": cluster_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1-hour granularity
                Statistics=["Average", "Maximum", "Minimum"],
            )
            datapoints = response.get("Datapoints", [])
            if datapoints:
                avg_values = [d["Average"] for d in datapoints]
                max_values = [d["Maximum"] for d in datapoints]
                metrics[metric_name] = {
                    "average": sum(avg_values) / len(avg_values),
                    "peak": max(max_values),
                    "min": min(d["Minimum"] for d in datapoints),
                    "datapoint_count": len(datapoints),
                }
        except Exception as e:
            metrics[metric_name] = {"error": str(e)}

    return metrics
```

## Generating the Structured Cluster Inventory

```python
def generate_cluster_inventory(days_back=90):
    """
    Produce a complete cluster inventory JSON suitable for migration planning.
    """
    all_clusters = list_clusters_by_state(
        ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING",
         "TERMINATED", "TERMINATED_WITH_ERRORS"],
        days_back=days_back,
    )

    inventory = []
    for c in all_clusters:
        cluster_id = c["id"]
        print(f"Analyzing cluster {cluster_id} ({c['name']})...")

        detail = describe_cluster(cluster_id)

        # Get compute details
        instance_groups = list_instance_groups(cluster_id)
        instance_fleets = list_instance_fleets(cluster_id)

        # Get configurations
        flat_config = extract_all_configurations(cluster_id)
        critical_config = get_migration_critical_configs(flat_config)

        # Get bootstrap actions
        bootstraps = list_bootstrap_actions(cluster_id)

        # Get security config
        sec_config = get_security_configuration(detail.get("security_configuration"))

        # Get utilization (only for active clusters)
        utilization = None
        if detail["state"] in ("RUNNING", "WAITING"):
            utilization = get_cluster_utilization(cluster_id)

        inventory.append({
            "cluster_detail": detail,
            "instance_groups": instance_groups,
            "instance_fleets": instance_fleets,
            "configurations": flat_config,
            "critical_configurations": critical_config,
            "bootstrap_actions": bootstraps,
            "security_configuration": sec_config,
            "utilization_metrics": utilization,
        })

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "cluster_count": len(inventory),
        "active_count": len([c for c in inventory if c["cluster_detail"]["state"] in ("RUNNING", "WAITING")]),
        "clusters": inventory,
    }


# Generate and save
inventory = generate_cluster_inventory(days_back=90)
with open("cluster-inventory.json", "w") as f:
    json.dump(inventory, f, indent=2, default=str)
print(f"Inventory saved: {inventory['cluster_count']} clusters analyzed")
```

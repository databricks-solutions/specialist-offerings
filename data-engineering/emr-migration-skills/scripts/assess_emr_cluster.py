#!/usr/bin/env python3
"""
EMR Cluster Assessment Tool

Enumerates EMR clusters, their configurations, steps, and instance groups
to produce a structured JSON report for migration planning.

Usage:
    python assess_emr_cluster.py --region us-east-1 --output assessment.json
    python assess_emr_cluster.py --cluster-id j-XXXXXXXXXXXXX
    python assess_emr_cluster.py --all-clusters --include-terminated

Prerequisites:
    pip install boto3
    AWS credentials configured (aws configure or environment variables)
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError


def get_emr_client(region: str) -> Any:
    """Create an EMR client for the specified region."""
    return boto3.client("emr", region_name=region)


def get_glue_client(region: str) -> Any:
    """Create a Glue client for the specified region."""
    return boto3.client("glue", region_name=region)


def list_clusters(
    emr_client: Any, include_terminated: bool = False, days_back: int = 30
) -> list[dict]:
    """List EMR clusters, optionally including recently terminated ones."""
    states = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
    if include_terminated:
        states.extend(["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"])

    clusters = []
    paginator = emr_client.get_paginator("list_clusters")

    kwargs: dict[str, Any] = {"ClusterStates": states}
    if include_terminated:
        kwargs["CreatedAfter"] = datetime.utcnow() - timedelta(days=days_back)

    for page in paginator.paginate(**kwargs):
        clusters.extend(page.get("Clusters", []))

    return clusters


def describe_cluster(emr_client: Any, cluster_id: str) -> dict:
    """Get detailed cluster information."""
    response = emr_client.describe_cluster(ClusterId=cluster_id)
    cluster = response["Cluster"]

    # Get instance groups
    try:
        ig_response = emr_client.list_instance_groups(ClusterId=cluster_id)
        cluster["InstanceGroups"] = ig_response.get("InstanceGroups", [])
    except ClientError:
        cluster["InstanceGroups"] = []

    # Try instance fleets
    try:
        if_response = emr_client.list_instance_fleets(ClusterId=cluster_id)
        cluster["InstanceFleets"] = if_response.get("InstanceFleets", [])
    except ClientError:
        cluster["InstanceFleets"] = []

    # Get bootstrap actions
    try:
        ba_response = emr_client.list_bootstrap_actions(ClusterId=cluster_id)
        cluster["BootstrapActions"] = ba_response.get("BootstrapActions", [])
    except ClientError:
        cluster["BootstrapActions"] = []

    # Get steps (last 50)
    try:
        steps_response = emr_client.list_steps(ClusterId=cluster_id)
        cluster["Steps"] = steps_response.get("Steps", [])
    except ClientError:
        cluster["Steps"] = []

    return cluster


def extract_configurations(cluster: dict) -> dict:
    """Extract Spark/Hadoop configurations from cluster detail."""
    configs = {}
    for config in cluster.get("Configurations", []):
        classification = config.get("Classification", "unknown")
        properties = config.get("Properties", {})
        configs[classification] = properties

        # Handle nested configurations
        for nested in config.get("Configurations", []):
            nested_class = nested.get("Classification", "unknown")
            nested_props = nested.get("Properties", {})
            configs[f"{classification}.{nested_class}"] = nested_props

    return configs


def extract_instance_summary(cluster: dict) -> dict:
    """Summarize instance types and counts."""
    summary = {"master": {}, "core": [], "task": []}

    for ig in cluster.get("InstanceGroups", []):
        role = ig.get("InstanceGroupType", "").lower()
        info = {
            "instance_type": ig.get("InstanceType"),
            "requested_count": ig.get("RequestedInstanceCount"),
            "running_count": ig.get("RunningInstanceCount"),
            "market": ig.get("Market"),  # ON_DEMAND or SPOT
            "ebs_config": ig.get("EbsBlockDevices", []),
        }
        if role == "master":
            summary["master"] = info
        elif role == "core":
            summary["core"].append(info)
        elif role == "task":
            summary["task"].append(info)

    for fleet in cluster.get("InstanceFleets", []):
        role = fleet.get("InstanceFleetType", "").lower()
        info = {
            "instance_types": [
                spec.get("InstanceType")
                for spec in fleet.get("InstanceTypeSpecifications", [])
            ],
            "target_on_demand": fleet.get("TargetOnDemandCapacity"),
            "target_spot": fleet.get("TargetSpotCapacity"),
            "provisioned_on_demand": fleet.get("ProvisionedOnDemandCapacity"),
            "provisioned_spot": fleet.get("ProvisionedSpotCapacity"),
        }
        if role == "master":
            summary["master"] = info
        elif role == "core":
            summary["core"].append(info)
        elif role == "task":
            summary["task"].append(info)

    return summary


def extract_step_summary(cluster: dict) -> list[dict]:
    """Summarize EMR steps for migration mapping."""
    steps = []
    for step in cluster.get("Steps", []):
        step_config = step.get("Config", {})
        steps.append(
            {
                "name": step.get("Name"),
                "id": step.get("Id"),
                "status": step.get("Status", {}).get("State"),
                "action_on_failure": step.get("ActionOnFailure"),
                "jar": step_config.get("Jar"),
                "main_class": step_config.get("MainClass"),
                "args": step_config.get("Args", []),
                "properties": step_config.get("Properties", {}),
            }
        )
    return steps


def get_glue_catalog_summary(glue_client: Any) -> dict:
    """Get summary of Glue Data Catalog for migration planning."""
    summary = {"databases": [], "total_tables": 0, "total_partitions_sampled": 0}

    try:
        paginator = glue_client.get_paginator("get_databases")
        for page in paginator.paginate():
            for db in page.get("DatabaseList", []):
                db_name = db["Name"]
                db_info = {
                    "name": db_name,
                    "description": db.get("Description", ""),
                    "location": db.get("LocationUri", ""),
                    "tables": [],
                }

                # Get tables for this database
                table_paginator = glue_client.get_paginator("get_tables")
                for table_page in table_paginator.paginate(DatabaseName=db_name):
                    for table in table_page.get("TableList", []):
                        storage = table.get("StorageDescriptor", {})
                        table_info = {
                            "name": table["Name"],
                            "table_type": table.get("TableType", ""),
                            "location": storage.get("Location", ""),
                            "input_format": storage.get("InputFormat", ""),
                            "output_format": storage.get("OutputFormat", ""),
                            "serde": storage.get("SerdeInfo", {}).get(
                                "SerializationLibrary", ""
                            ),
                            "columns": len(storage.get("Columns", [])),
                            "partition_keys": [
                                p["Name"] for p in table.get("PartitionKeys", [])
                            ],
                            "parameters": table.get("Parameters", {}),
                        }
                        db_info["tables"].append(table_info)
                        summary["total_tables"] += 1

                summary["databases"].append(db_info)

    except ClientError as e:
        summary["error"] = str(e)

    return summary


def build_assessment_report(
    cluster_details: list[dict], glue_summary: dict | None = None
) -> dict:
    """Build the final assessment report."""
    report = {
        "assessment_date": datetime.utcnow().isoformat(),
        "summary": {
            "total_clusters": len(cluster_details),
            "active_clusters": sum(
                1
                for c in cluster_details
                if c.get("Status", {}).get("State") in ("RUNNING", "WAITING")
            ),
            "total_steps": sum(
                len(c.get("Steps", [])) for c in cluster_details
            ),
            "unique_instance_types": list(
                set(
                    ig.get("InstanceType", "")
                    for c in cluster_details
                    for ig in c.get("InstanceGroups", [])
                )
            ),
            "emr_releases": list(
                set(
                    c.get("ReleaseLabel", "unknown") for c in cluster_details
                )
            ),
        },
        "clusters": [],
        "glue_catalog": glue_summary,
    }

    for cluster in cluster_details:
        cluster_report = {
            "cluster_id": cluster.get("Id"),
            "name": cluster.get("Name"),
            "state": cluster.get("Status", {}).get("State"),
            "release_label": cluster.get("ReleaseLabel"),
            "applications": [
                app.get("Name") for app in cluster.get("Applications", [])
            ],
            "auto_terminate": cluster.get("AutoTerminate", False),
            "configurations": extract_configurations(cluster),
            "instances": extract_instance_summary(cluster),
            "bootstrap_actions": [
                {
                    "name": ba.get("Name"),
                    "script": ba.get("ScriptPath"),
                    "args": ba.get("Args", []),
                }
                for ba in cluster.get("BootstrapActions", [])
            ],
            "steps": extract_step_summary(cluster),
            "security": {
                "service_role": cluster.get("ServiceRole"),
                "ec2_instance_profile": cluster.get("Ec2InstanceAttributes", {}).get(
                    "IamInstanceProfile"
                ),
                "security_groups": {
                    "master": cluster.get("Ec2InstanceAttributes", {}).get(
                        "EmrManagedMasterSecurityGroup"
                    ),
                    "slave": cluster.get("Ec2InstanceAttributes", {}).get(
                        "EmrManagedSlaveSecurityGroup"
                    ),
                },
                "subnet_id": cluster.get("Ec2InstanceAttributes", {}).get(
                    "Ec2SubnetId"
                ),
            },
            "tags": cluster.get("Tags", []),
        }
        report["clusters"].append(cluster_report)

    return report


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def main():
    parser = argparse.ArgumentParser(
        description="Assess EMR clusters for Databricks migration"
    )
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region (default: us-east-1)"
    )
    parser.add_argument("--cluster-id", help="Assess a specific cluster by ID")
    parser.add_argument(
        "--all-clusters", action="store_true", help="Assess all clusters in the region"
    )
    parser.add_argument(
        "--include-terminated",
        action="store_true",
        help="Include recently terminated clusters",
    )
    parser.add_argument(
        "--include-glue",
        action="store_true",
        help="Include Glue Data Catalog summary",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Days to look back for terminated clusters (default: 30)",
    )
    parser.add_argument("--output", default="-", help="Output file (- for stdout)")
    args = parser.parse_args()

    emr_client = get_emr_client(args.region)

    # Collect cluster details
    cluster_details = []

    if args.cluster_id:
        print(f"Assessing cluster: {args.cluster_id}", file=sys.stderr)
        detail = describe_cluster(emr_client, args.cluster_id)
        cluster_details.append(detail)
    elif args.all_clusters:
        print(f"Listing clusters in {args.region}...", file=sys.stderr)
        clusters = list_clusters(
            emr_client, args.include_terminated, args.days_back
        )
        print(f"Found {len(clusters)} clusters", file=sys.stderr)
        for cluster_summary in clusters:
            cid = cluster_summary["Id"]
            print(f"  Assessing {cid} ({cluster_summary.get('Name', 'N/A')})...", file=sys.stderr)
            try:
                detail = describe_cluster(emr_client, cid)
                cluster_details.append(detail)
            except ClientError as e:
                print(f"  Warning: Could not describe {cid}: {e}", file=sys.stderr)
    else:
        parser.error("Specify --cluster-id or --all-clusters")

    # Optionally include Glue catalog
    glue_summary = None
    if args.include_glue:
        print("Scanning Glue Data Catalog...", file=sys.stderr)
        glue_client = get_glue_client(args.region)
        glue_summary = get_glue_catalog_summary(glue_client)
        print(
            f"Found {len(glue_summary.get('databases', []))} databases, "
            f"{glue_summary.get('total_tables', 0)} tables",
            file=sys.stderr,
        )

    # Build report
    report = build_assessment_report(cluster_details, glue_summary)

    # Output
    output_json = json.dumps(report, indent=2, cls=DateTimeEncoder)
    if args.output == "-":
        print(output_json)
    else:
        with open(args.output, "w") as f:
            f.write(output_json)
        print(f"Assessment written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()

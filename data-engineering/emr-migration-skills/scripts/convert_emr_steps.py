#!/usr/bin/env python3
"""
EMR Steps to Databricks Workflows Converter

Reads EMR step definitions (from cluster assessment JSON or AWS CLI output)
and generates Databricks Asset Bundle workflow YAML.

Usage:
    python convert_emr_steps.py --input assessment.json --output workflow.yml
    python convert_emr_steps.py --cluster-id j-XXXXX --region us-east-1 --output workflow.yml

Prerequisites:
    pip install boto3 pyyaml
"""

import argparse
import json
import re
import sys
from typing import Any

try:
    import yaml
except ImportError:
    print("Error: pyyaml is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


def classify_step(step: dict) -> str:
    """Classify an EMR step into a Databricks task type."""
    jar = step.get("jar", "") or ""
    args = step.get("args", []) or []
    name = step.get("name", "").lower()

    # Spark submit
    if "command-runner.jar" in jar and args and args[0] == "spark-submit":
        # Check if it's a Python script or JAR
        for arg in args:
            if arg.endswith(".py"):
                return "spark_python_task"
            if arg.endswith(".jar"):
                return "spark_jar_task"
        return "spark_python_task"  # Default to Python

    # Hive
    if "command-runner.jar" in jar and args and args[0] in ("hive-script", "hive"):
        return "sql_task"

    # s3-dist-cp
    if "command-runner.jar" in jar and args and args[0] == "s3-dist-cp":
        return "notebook_task"

    # Custom JAR
    if jar and "command-runner.jar" not in jar:
        return "spark_jar_task"

    # Shell/script
    if "script-runner.jar" in jar:
        return "notebook_task"

    return "notebook_task"  # Default


def extract_spark_submit_args(args: list[str]) -> dict:
    """Parse spark-submit arguments into structured form."""
    result = {
        "main_file": None,
        "main_class": None,
        "jars": [],
        "packages": [],
        "conf": {},
        "app_args": [],
    }

    i = 0
    past_main = False
    while i < len(args):
        arg = args[i]

        if arg == "spark-submit":
            i += 1
            continue

        if arg == "--class" and i + 1 < len(args):
            result["main_class"] = args[i + 1]
            i += 2
            continue

        if arg == "--jars" and i + 1 < len(args):
            result["jars"] = args[i + 1].split(",")
            i += 2
            continue

        if arg == "--packages" and i + 1 < len(args):
            result["packages"] = args[i + 1].split(",")
            i += 2
            continue

        if arg == "--conf" and i + 1 < len(args):
            conf_str = args[i + 1]
            if "=" in conf_str:
                key, value = conf_str.split("=", 1)
                result["conf"][key] = value
            i += 2
            continue

        # Skip other spark-submit flags
        if arg.startswith("--"):
            # Flags that take a value
            if arg in (
                "--master", "--deploy-mode", "--driver-memory",
                "--executor-memory", "--executor-cores", "--num-executors",
                "--files", "--py-files", "--driver-class-path",
                "--name", "--queue", "--proxy-user",
            ):
                i += 2
                continue
            i += 1
            continue

        # Main file or application arguments
        if not past_main and (arg.endswith(".py") or arg.endswith(".jar") or arg.startswith("s3://")):
            result["main_file"] = arg
            past_main = True
            i += 1
            continue

        if past_main:
            result["app_args"].append(arg)

        i += 1

    return result


def convert_step_to_task(step: dict, task_index: int) -> dict:
    """Convert a single EMR step to a Databricks task definition."""
    task_type = classify_step(step)
    task_key = re.sub(r"[^a-zA-Z0-9_]", "_", step.get("name", f"task_{task_index}")).lower()

    task = {
        "task_key": task_key,
        "description": f"Converted from EMR step: {step.get('name', 'unknown')}",
    }

    args = step.get("args", []) or []

    if task_type == "spark_python_task":
        spark_args = extract_spark_submit_args(args)
        python_file = spark_args["main_file"] or "TODO: specify python file path"

        task["spark_python_task"] = {
            "python_file": f"# TODO: Upload to workspace and update path\n# Original: {python_file}",
        }
        if spark_args["app_args"]:
            task["spark_python_task"]["parameters"] = spark_args["app_args"]

        # Add libraries from --packages
        if spark_args["packages"]:
            task["libraries"] = [
                {"maven": {"coordinates": pkg}} for pkg in spark_args["packages"]
            ]

    elif task_type == "spark_jar_task":
        spark_args = extract_spark_submit_args(args)

        task["spark_jar_task"] = {
            "main_class_name": spark_args.get("main_class") or "TODO: specify main class",
        }
        if spark_args["app_args"]:
            task["spark_jar_task"]["parameters"] = spark_args["app_args"]

        # Add JAR as library
        jars = spark_args.get("jars", [])
        if spark_args["main_file"] and spark_args["main_file"].endswith(".jar"):
            jars.insert(0, spark_args["main_file"])
        if jars:
            task["libraries"] = [
                {"jar": f"# TODO: Upload to UC Volume\n# Original: {jar}"}
                for jar in jars
            ]

    elif task_type == "sql_task":
        # Extract HiveQL script path
        script_path = None
        for i, arg in enumerate(args):
            if arg == "-f" and i + 1 < len(args):
                script_path = args[i + 1]
                break

        task["sql_task"] = {
            "query": {
                "query_string": f"-- TODO: Convert HiveQL to Spark SQL\n-- Original script: {script_path or 'inline'}"
            },
            "warehouse_id": "TODO: specify SQL warehouse ID",
        }

    else:
        task["notebook_task"] = {
            "notebook_path": f"# TODO: Create notebook for this step\n# Original: {step.get('name', 'unknown')}",
            "base_parameters": {},
        }

    # Retry policy based on ActionOnFailure
    action = step.get("action_on_failure", "CONTINUE")
    if action == "CONTINUE":
        task["max_retries"] = 0
    elif action in ("CANCEL_AND_WAIT", "TERMINATE_CLUSTER"):
        task["max_retries"] = 1
        task["min_retry_interval_millis"] = 60000

    return task


def convert_steps_to_workflow(
    steps: list[dict],
    job_name: str = "emr_migrated_job",
    cluster_id: str | None = None,
) -> dict:
    """Convert EMR steps to a Databricks workflow definition."""
    tasks = []
    prev_task_key = None

    for i, step in enumerate(steps):
        task = convert_step_to_task(step, i)

        # Add dependency on previous task (EMR steps are sequential)
        if prev_task_key:
            task["depends_on"] = [{"task_key": prev_task_key}]

        tasks.append(task)
        prev_task_key = task["task_key"]

    workflow = {
        "resources": {
            "jobs": {
                job_name: {
                    "name": f"{job_name} (migrated from EMR)",
                    "description": f"Migrated from EMR cluster {cluster_id or 'unknown'}",
                    "tasks": tasks,
                    "tags": {
                        "migrated_from": "emr",
                        "emr_cluster_id": cluster_id or "unknown",
                    },
                }
            }
        }
    }

    return workflow


def load_steps_from_assessment(filepath: str) -> list[tuple[str, list[dict]]]:
    """Load steps from an assessment JSON file (from assess_emr_cluster.py)."""
    with open(filepath) as f:
        data = json.load(f)

    results = []
    for cluster in data.get("clusters", []):
        cluster_id = cluster.get("cluster_id", "unknown")
        steps = cluster.get("steps", [])
        if steps:
            results.append((cluster_id, steps))

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Convert EMR steps to Databricks workflow YAML"
    )
    parser.add_argument(
        "--input", help="Assessment JSON file from assess_emr_cluster.py"
    )
    parser.add_argument("--cluster-id", help="EMR cluster ID (fetches steps via API)")
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region (for API mode)"
    )
    parser.add_argument("--output", default="-", help="Output YAML file (- for stdout)")
    parser.add_argument(
        "--job-name",
        default="emr_migrated_job",
        help="Name for the Databricks job",
    )
    args = parser.parse_args()

    if args.input:
        cluster_steps = load_steps_from_assessment(args.input)
        if not cluster_steps:
            print("No steps found in assessment file", file=sys.stderr)
            sys.exit(1)

        # Use first cluster's steps (or combine if multiple)
        cluster_id, steps = cluster_steps[0]
        if len(cluster_steps) > 1:
            print(
                f"Found steps in {len(cluster_steps)} clusters, using first: {cluster_id}",
                file=sys.stderr,
            )

    elif args.cluster_id:
        import boto3

        emr = boto3.client("emr", region_name=args.region)
        response = emr.list_steps(ClusterId=args.cluster_id)
        cluster_id = args.cluster_id
        steps = []
        for step in response.get("Steps", []):
            config = step.get("Config", {})
            steps.append(
                {
                    "name": step.get("Name"),
                    "jar": config.get("Jar"),
                    "main_class": config.get("MainClass"),
                    "args": config.get("Args", []),
                    "action_on_failure": step.get("ActionOnFailure"),
                }
            )
    else:
        parser.error("Specify --input or --cluster-id")

    print(f"Converting {len(steps)} steps from cluster {cluster_id}", file=sys.stderr)

    workflow = convert_steps_to_workflow(steps, args.job_name, cluster_id)

    # Output YAML
    yaml_str = yaml.dump(workflow, default_flow_style=False, sort_keys=False, width=120)

    # Add header comment
    output = f"# Databricks Workflow - Migrated from EMR\n# Cluster: {cluster_id}\n# Steps: {len(steps)}\n#\n# TODO:\n#   1. Upload Python/JAR files to Databricks workspace or UC Volumes\n#   2. Update file paths in task definitions\n#   3. Configure cluster settings (node type, autoscaling)\n#   4. Add schedule if needed\n#   5. Validate with: databricks bundle validate\n\n{yaml_str}"

    if args.output == "-":
        print(output)
    else:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Workflow written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()

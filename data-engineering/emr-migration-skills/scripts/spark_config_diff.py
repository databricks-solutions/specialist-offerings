#!/usr/bin/env python3
"""
Spark Configuration Diff Tool

Compares EMR Spark/Hadoop configurations against Databricks Runtime defaults
and recommends actions (keep, modify, remove, replace) for each config.

Usage:
    python spark_config_diff.py --input assessment.json
    python spark_config_diff.py --cluster-id j-XXXXX --region us-east-1
    python spark_config_diff.py --config-file spark-defaults.conf

Prerequisites:
    pip install boto3
"""

import argparse
import json
import os
import sys
from typing import Any

# Load the mapping from the mappings directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPINGS_DIR = os.path.join(SCRIPT_DIR, "..", "mappings")


def load_config_mapping() -> dict[str, dict]:
    """Load the spark config mapping from JSON file."""
    mapping_file = os.path.join(MAPPINGS_DIR, "spark_config_mapping.json")
    with open(mapping_file) as f:
        data = json.load(f)

    # Build lookup by EMR config key
    mapping = {}
    for entry in data.get("mappings", []):
        mapping[entry["emr_config"]] = entry
    return mapping


def load_classification_mapping() -> dict[str, str]:
    """Load EMR classification to Databricks mapping."""
    mapping_file = os.path.join(MAPPINGS_DIR, "spark_config_mapping.json")
    with open(mapping_file) as f:
        data = json.load(f)
    return data.get("emr_classification_mapping", {})


def classify_config(key: str, mapping: dict[str, dict]) -> dict:
    """Classify a single configuration key."""
    # Exact match
    if key in mapping:
        return mapping[key]

    # Pattern matching for common prefixes
    patterns = {
        "spark.yarn.": {"action": "remove", "notes": "YARN-specific config. Not applicable on Databricks."},
        "spark.hadoop.fs.s3.": {"action": "remove", "notes": "EMRFS config. Databricks uses its own S3 connector."},
        "spark.hadoop.fs.s3a.": {"action": "replace", "notes": "S3A config. Replace with Unity Catalog storage credentials."},
        "spark.hadoop.fs.s3n.": {"action": "remove", "notes": "Deprecated S3N config. Not needed on Databricks."},
        "spark.sql.hive.metastore.": {"action": "remove", "notes": "Hive metastore config. Not needed with Unity Catalog."},
        "spark.hadoop.yarn.": {"action": "remove", "notes": "YARN config. Not applicable."},
        "spark.hadoop.mapreduce.": {"action": "remove", "notes": "MapReduce config. Not applicable."},
        "spark.dynamicAllocation.": {"action": "remove", "notes": "Dynamic allocation. Managed by Databricks autoscaling."},
    }

    for prefix, info in patterns.items():
        if key.startswith(prefix):
            return {
                "emr_config": key,
                "action": info["action"],
                "dbr_equivalent": info.get("dbr_equivalent"),
                "notes": info["notes"],
            }

    # Default: keep (unknown configs are likely Spark core configs that work as-is)
    return {
        "emr_config": key,
        "action": "keep",
        "dbr_equivalent": key,
        "notes": "Not in mapping database. Likely works as-is — verify on Databricks.",
    }


def extract_configs_from_assessment(filepath: str) -> dict[str, dict[str, str]]:
    """Extract configurations from assessment JSON."""
    with open(filepath) as f:
        data = json.load(f)

    all_configs = {}
    for cluster in data.get("clusters", []):
        cluster_id = cluster.get("cluster_id", "unknown")
        configs = cluster.get("configurations", {})
        for classification, props in configs.items():
            if classification not in all_configs:
                all_configs[classification] = {}
            all_configs[classification].update(props)

    return all_configs


def extract_configs_from_file(filepath: str) -> dict[str, str]:
    """Extract configs from a spark-defaults.conf file."""
    configs = {}
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                configs[parts[0]] = parts[1]
    return configs


def generate_report(
    configs: dict[str, dict[str, str]],
    mapping: dict[str, dict],
    classification_mapping: dict[str, str],
) -> dict:
    """Generate a migration report for all configurations."""
    report = {
        "summary": {"total_configs": 0, "keep": 0, "modify": 0, "remove": 0, "replace": 0},
        "classifications": {},
        "recommendations": [],
    }

    for classification, props in configs.items():
        # Check if the entire classification is relevant
        class_note = classification_mapping.get(classification, "")

        class_report = {
            "databricks_equivalent": class_note,
            "configs": [],
        }

        for key, value in props.items():
            full_key = key
            # For spark-defaults, keys are already full (spark.xxx)
            # For other classifications, they might be short keys

            result = classify_config(full_key, mapping)
            result["current_value"] = value

            class_report["configs"].append(result)
            report["summary"]["total_configs"] += 1
            report["summary"][result["action"]] += 1

        report["classifications"][classification] = class_report

    # Generate top-level recommendations
    if report["summary"]["remove"] > 0:
        report["recommendations"].append(
            f"Remove {report['summary']['remove']} configs that are not applicable on Databricks (YARN, EMRFS, HDFS)"
        )
    if report["summary"]["replace"] > 0:
        report["recommendations"].append(
            f"Replace {report['summary']['replace']} configs with Databricks equivalents (S3 credentials → UC, JARs → cluster libraries)"
        )
    if report["summary"]["modify"] > 0:
        report["recommendations"].append(
            f"Modify {report['summary']['modify']} configs for Databricks compatibility"
        )
    if report["summary"]["keep"] > 0:
        report["recommendations"].append(
            f"Keep {report['summary']['keep']} configs as-is (standard Spark configs)"
        )

    return report


def print_report(report: dict, format: str = "text"):
    """Print the report in the specified format."""
    if format == "json":
        print(json.dumps(report, indent=2))
        return

    # Text format
    print("=" * 70)
    print("EMR → Databricks Spark Configuration Migration Report")
    print("=" * 70)
    print()

    summary = report["summary"]
    print(f"Total configurations analyzed: {summary['total_configs']}")
    print(f"  KEEP   (works as-is):         {summary['keep']}")
    print(f"  MODIFY (needs adjustment):     {summary['modify']}")
    print(f"  REMOVE (not applicable):       {summary['remove']}")
    print(f"  REPLACE (use alternative):     {summary['replace']}")
    print()

    if report["recommendations"]:
        print("Recommendations:")
        for rec in report["recommendations"]:
            print(f"  - {rec}")
        print()

    for classification, class_report in report["classifications"].items():
        print(f"\n--- Classification: {classification} ---")
        if class_report["databricks_equivalent"]:
            print(f"    Databricks: {class_report['databricks_equivalent']}")
        print()

        for config in class_report["configs"]:
            action = config["action"].upper()
            icon = {"keep": "+", "modify": "~", "remove": "-", "replace": ">"}.get(
                config["action"], "?"
            )
            print(f"  [{icon}] {action}: {config['emr_config']}")
            print(f"      Value: {config.get('current_value', 'N/A')}")
            if config.get("dbr_equivalent") and config["dbr_equivalent"] != config["emr_config"]:
                print(f"      Databricks: {config['dbr_equivalent']}")
            print(f"      Notes: {config.get('notes', '')}")
            print()


def main():
    parser = argparse.ArgumentParser(
        description="Compare EMR Spark configs vs Databricks defaults"
    )
    parser.add_argument(
        "--input", help="Assessment JSON from assess_emr_cluster.py"
    )
    parser.add_argument(
        "--config-file", help="spark-defaults.conf file"
    )
    parser.add_argument(
        "--cluster-id", help="EMR cluster ID (fetches config via API)"
    )
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    parser.add_argument("--output", default="-", help="Output file (- for stdout)")
    args = parser.parse_args()

    mapping = load_config_mapping()
    classification_mapping = load_classification_mapping()

    configs: dict[str, dict[str, str]] = {}

    if args.input:
        configs = extract_configs_from_assessment(args.input)
    elif args.config_file:
        configs = {"spark-defaults": extract_configs_from_file(args.config_file)}
    elif args.cluster_id:
        import boto3

        emr = boto3.client("emr", region_name=args.region)
        response = emr.describe_cluster(ClusterId=args.cluster_id)
        cluster = response["Cluster"]
        for config in cluster.get("Configurations", []):
            classification = config.get("Classification", "unknown")
            configs[classification] = config.get("Properties", {})
    else:
        parser.error("Specify --input, --config-file, or --cluster-id")

    report = generate_report(configs, mapping, classification_mapping)

    if args.output == "-":
        print_report(report, args.format)
    else:
        if args.format == "json":
            with open(args.output, "w") as f:
                json.dump(report, f, indent=2)
        else:
            import io

            buf = io.StringIO()
            old_stdout = sys.stdout
            sys.stdout = buf
            print_report(report, args.format)
            sys.stdout = old_stdout
            with open(args.output, "w") as f:
                f.write(buf.getvalue())
        print(f"Report written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
EC2 Instance Type to Databricks Node Type Mapper

Maps EMR instance types to Databricks equivalents with cost comparison.

Usage:
    python map_instance_types.py --instance-types m5.xlarge r5.2xlarge c5.4xlarge
    python map_instance_types.py --input assessment.json
    python map_instance_types.py --list-all

Prerequisites:
    No external dependencies (stdlib only)
"""

import argparse
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPINGS_DIR = os.path.join(SCRIPT_DIR, "..", "mappings")


def load_instance_mapping() -> dict:
    """Load instance type mapping from JSON."""
    mapping_file = os.path.join(MAPPINGS_DIR, "instance_type_mapping.json")
    with open(mapping_file) as f:
        data = json.load(f)

    lookup = {}
    for entry in data.get("mappings", []):
        lookup[entry["ec2_type"]] = entry

    return lookup, data.get("recommendations", {})


def map_instance_type(instance_type: str, lookup: dict) -> dict:
    """Map a single EC2 instance type to Databricks equivalent."""
    if instance_type in lookup:
        return lookup[instance_type]

    # Try to infer from family
    family = instance_type.rsplit(".", 1)[0] if "." in instance_type else instance_type
    for key, entry in lookup.items():
        if key.startswith(family + "."):
            return {
                "ec2_type": instance_type,
                "dbr_node_type": instance_type,
                "category": entry.get("category", "unknown"),
                "notes": f"Not in mapping; inferred from {family} family. Verify availability as Databricks node type.",
            }

    return {
        "ec2_type": instance_type,
        "dbr_node_type": instance_type,
        "category": "unknown",
        "notes": "Not in mapping database. Instance type may work as-is if available in your Databricks region.",
    }


def extract_instance_types_from_assessment(filepath: str) -> list[str]:
    """Extract unique instance types from assessment JSON."""
    with open(filepath) as f:
        data = json.load(f)

    instance_types = set()
    for cluster in data.get("clusters", []):
        instances = cluster.get("instances", {})

        # Master
        master = instances.get("master", {})
        if "instance_type" in master:
            instance_types.add(master["instance_type"])
        for it in master.get("instance_types", []):
            instance_types.add(it)

        # Core and Task
        for group in ["core", "task"]:
            for entry in instances.get(group, []):
                if "instance_type" in entry:
                    instance_types.add(entry["instance_type"])
                for it in entry.get("instance_types", []):
                    instance_types.add(it)

    return sorted(instance_types)


def print_mapping_table(mappings: list[dict]):
    """Print a formatted mapping table."""
    header = f"{'EC2 Type':<20} {'DBR Node Type':<20} {'vCPU':>6} {'Memory':>8} {'DBU/hr':>8} {'Category':<25} {'Notes'}"
    print(header)
    print("-" * len(header))

    for m in mappings:
        print(
            f"{m.get('ec2_type', 'N/A'):<20} "
            f"{m.get('dbr_node_type', 'N/A'):<20} "
            f"{m.get('vcpu', 'N/A'):>6} "
            f"{str(m.get('memory_gb', 'N/A')) + 'GB':>8} "
            f"{m.get('dbu_per_hour', 'N/A'):>8} "
            f"{m.get('category', 'N/A'):<25} "
            f"{m.get('notes', '')}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Map EC2 instance types to Databricks node types"
    )
    parser.add_argument(
        "--instance-types", nargs="+", help="EC2 instance types to map"
    )
    parser.add_argument(
        "--input", help="Assessment JSON from assess_emr_cluster.py"
    )
    parser.add_argument(
        "--list-all", action="store_true", help="List all known mappings"
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    args = parser.parse_args()

    lookup, recommendations = load_instance_mapping()

    if args.list_all:
        all_mappings = list(lookup.values())
        if args.format == "json":
            print(json.dumps(all_mappings, indent=2))
        else:
            print_mapping_table(all_mappings)
            print(f"\nRecommendations:")
            for key, value in recommendations.items():
                print(f"  {key}: {value}")
        return

    instance_types = []
    if args.instance_types:
        instance_types = args.instance_types
    elif args.input:
        instance_types = extract_instance_types_from_assessment(args.input)
        print(f"Found {len(instance_types)} unique instance types in assessment\n", file=sys.stderr)
    else:
        parser.error("Specify --instance-types, --input, or --list-all")

    mappings = [map_instance_type(it, lookup) for it in instance_types]

    if args.format == "json":
        print(json.dumps(mappings, indent=2))
    else:
        print_mapping_table(mappings)
        print(f"\nRecommendations:")
        for key, value in recommendations.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()

"""Command-line interface for the Hadoop Workload Analyzer."""

import argparse
import json
import logging
import sys

from analyzer.config import load_config
from analyzer.inventory import InventoryBuilder
from analyzer.models import WorkloadInventoryItem
from analyzer.reporters.csv_reporter import generate_csv_report
from analyzer.reporters.json_reporter import generate_json_report


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def cmd_analyze(args):
    """Full analysis pipeline: profiler + Oozie + optional path verification."""
    config = load_config(args.config)
    builder = InventoryBuilder(config)
    items = builder.build_full()

    if config.webhdfs.enabled:
        items = builder.verify_paths(items)

    _output_report(items, config)


def cmd_parse_profiler(args):
    """Parse profiler output only (no Oozie)."""
    config = load_config(args.config)
    builder = InventoryBuilder(config)
    items = builder.build_from_profiler()

    _output_report(items, config)


def cmd_scan_oozie(args):
    """Scan Oozie only (no profiler output)."""
    config = load_config(args.config)
    builder = InventoryBuilder(config)
    items = builder.build_from_oozie()

    _output_report(items, config)


def cmd_verify_paths(args):
    """Verify HDFS paths in an existing inventory JSON file."""
    config = load_config(args.config)

    with open(args.input, "r") as f:
        data = json.load(f)

    # Re-hydrate WorkloadInventoryItem objects from JSON
    from analyzer.models import CodeArtifact, WorkloadType
    items = []
    for entry in data.get("inventory", []):
        item = WorkloadInventoryItem(
            workload_id=entry.get("workload_id", ""),
            workload_name=entry.get("workload_name", ""),
            workload_type=WorkloadType(entry.get("workload_type", "unknown")),
            user=entry.get("user", ""),
            queue=entry.get("queue", ""),
            entry_point=entry.get("entry_point"),
            code_artifacts=[
                CodeArtifact(**a) for a in entry.get("code_artifacts", [])
            ],
            dependencies=[
                CodeArtifact(**a) for a in entry.get("dependencies", [])
            ],
            oozie_workflow_id=entry.get("oozie_workflow_id"),
            oozie_workflow_name=entry.get("oozie_workflow_name"),
            oozie_app_path=entry.get("oozie_app_path"),
            yarn_app_id=entry.get("yarn_app_id"),
            source=entry.get("source", ""),
            tags=entry.get("tags", []),
        )
        items.append(item)

    builder = InventoryBuilder(config)
    items = builder.verify_paths(items)

    _output_report(items, config)


def _output_report(items, config):
    """Generate reports based on config."""
    output_dir = config.output.dir
    fmt = config.output.format

    if fmt in ("json", "both"):
        path = generate_json_report(items, output_dir)
        print(f"JSON report: {path}")

    if fmt in ("csv", "both"):
        path = generate_csv_report(items, output_dir)
        print(f"CSV report: {path}")

    # Print summary to stdout
    print(f"\nTotal workloads: {len(items)}")
    from collections import Counter
    type_counts = Counter(item.workload_type.value for item in items)
    for wtype, count in type_counts.most_common():
        print(f"  {wtype}: {count}")


def main():
    parser = argparse.ArgumentParser(
        prog="analyzer",
        description="Hadoop Workload Analyzer — build code-level inventory from profiler output and Oozie",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="Full pipeline: profiler + Oozie")
    p_analyze.add_argument("--config", required=True, help="Path to analyzer.conf.yaml")
    p_analyze.set_defaults(func=cmd_analyze)

    # parse-profiler
    p_profiler = subparsers.add_parser("parse-profiler", help="Parse profiler output only")
    p_profiler.add_argument("--config", required=True, help="Path to analyzer.conf.yaml")
    p_profiler.set_defaults(func=cmd_parse_profiler)

    # scan-oozie
    p_oozie = subparsers.add_parser("scan-oozie", help="Scan Oozie only")
    p_oozie.add_argument("--config", required=True, help="Path to analyzer.conf.yaml")
    p_oozie.set_defaults(func=cmd_scan_oozie)

    # verify-paths
    p_verify = subparsers.add_parser("verify-paths", help="Verify HDFS paths in inventory")
    p_verify.add_argument("--config", required=True, help="Path to analyzer.conf.yaml")
    p_verify.add_argument("--input", required=True, help="Path to inventory JSON file")
    p_verify.set_defaults(func=cmd_verify_paths)

    args = parser.parse_args()
    setup_logging(args.verbose)
    args.func(args)


if __name__ == "__main__":
    main()

"""CLI entry point for the DuckDB exporter: python -m duckdb_exporter"""

import argparse
import logging
import sys

logger = logging.getLogger("duckdb_exporter")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def cmd_export(args):
    """Export profiler JSON output to a DuckDB file."""
    from duckdb_exporter.config import ExporterConfig, ProfilerOutputConfig, OutputConfig, \
        CostRatesConfig, SourcesConfig, load_config

    if args.config:
        config = load_config(args.config)
    else:
        config = ExporterConfig()

    # CLI args override config file
    if args.profiler_output:
        config.profiler_output.base_dir = args.profiler_output
    if args.output:
        config.output.db_path = args.output
    if args.dbu_rate is not None:
        config.cost_rates.dbu_rate = args.dbu_rate
    if args.vm_rate is not None:
        config.cost_rates.vm_rate = args.vm_rate
    if args.sources:
        enabled = [s.strip().lower() for s in args.sources.split(",")]
        config.sources = SourcesConfig(
            yarn="yarn" in enabled,
            spark="spark" in enabled,
            impala="impala" in enabled,
            cm="cm" in enabled,
        )

    if not config.profiler_output.base_dir:
        print("Error: --profiler-output or config profiler_output.base_dir is required")
        sys.exit(1)

    from duckdb_exporter.exporter import run_export
    run_export(config)


def cmd_validate(args):
    """Validate profiler output directory structure."""
    import os
    from duckdb_exporter.utils import find_json_files

    base_dir = args.profiler_output
    if not os.path.isdir(base_dir):
        print(f"Error: directory not found: {base_dir}")
        sys.exit(1)

    print(f"Validating profiler output: {base_dir}\n")
    total = 0

    for subdir, pattern, label in [
        ("YARN", "YarnApplicationDump*.json", "YARN Applications"),
        ("YARN", "YarnMetricsDump*.json", "YARN Metrics"),
        ("YARN", "YarnNodesDump*.json", "YARN Nodes"),
        ("YARN", "YarnSchedulerDump*.json", "YARN Scheduler"),
        ("SPARK", "Spark_Applications*.json", "Spark Applications"),
        ("IMPALA", "impala_*.json", "Impala Queries"),
        ("CM", "cmHosts_*.json", "CM Hosts"),
        ("CM", "cmServices_*.json", "CM Services"),
        ("CM", "cmConfig_*.json", "CM Config"),
        ("CM", "cmExport_*.json", "CM Export"),
        ("CM", "cmHostRoles_*.json", "CM Host Roles"),
        ("CM", "cmHDFSUsage_*.json", "CM HDFS Usage"),
        ("CM", "cmClusterCPUUtilization_*.json", "CM CPU Utilization"),
        ("CM", "cmClusterMemoryUtilization_*.json", "CM Memory Utilization"),
        ("CM", "cmYarnMemoryAndCPU_*.json", "CM YARN Memory/CPU"),
        ("CM", "cmYarnUtilization_*.json", "CM YARN Utilization"),
        ("CM", "cmImpalaUtilization_*.json", "CM Impala Utilization"),
    ]:
        files = find_json_files(base_dir, subdir, pattern)
        count = len(files)
        total += count
        status = "OK" if count > 0 else "MISSING"
        print(f"  [{status}] {label}: {count} file(s)")

    print(f"\nTotal JSON files found: {total}")


def cmd_query(args):
    """Query an existing DuckDB file."""
    import duckdb

    conn = duckdb.connect(args.db, read_only=True)
    try:
        result = conn.execute(args.sql).fetchdf()
        print(result.to_string())
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        prog="duckdb_exporter",
        description="Export Hadoop Profiler JSON output to a DuckDB database file",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # export
    p_export = subparsers.add_parser("export", help="Export profiler JSON to DuckDB")
    p_export.add_argument("--profiler-output", help="Path to profiler Output/ directory")
    p_export.add_argument("--output", help="Path for output .duckdb file")
    p_export.add_argument("--config", help="Path to duckdb_exporter.conf.yaml")
    p_export.add_argument("--dbu-rate", type=float, help="DBU cost rate ($/GB-hour)")
    p_export.add_argument("--vm-rate", type=float, help="VM cost rate ($/GB-hour)")
    p_export.add_argument("--sources", help="Comma-separated sources: yarn,spark,impala,cm")
    p_export.set_defaults(func=cmd_export)

    # validate
    p_validate = subparsers.add_parser("validate", help="Validate profiler output directory")
    p_validate.add_argument("--profiler-output", required=True, help="Path to profiler Output/")
    p_validate.set_defaults(func=cmd_validate)

    # query
    p_query = subparsers.add_parser("query", help="Query an existing DuckDB file")
    p_query.add_argument("--db", required=True, help="Path to .duckdb file")
    p_query.add_argument("--sql", required=True, help="SQL query to execute")
    p_query.set_defaults(func=cmd_query)

    args = parser.parse_args()
    setup_logging(args.verbose)
    args.func(args)


if __name__ == "__main__":
    main()

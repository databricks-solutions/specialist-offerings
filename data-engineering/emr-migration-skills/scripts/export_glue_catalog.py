#!/usr/bin/env python3
"""
Glue Data Catalog Export Tool

Exports AWS Glue Data Catalog metadata and generates Unity Catalog DDL
statements for migration to Databricks.

Usage:
    python export_glue_catalog.py --region us-east-1 --catalog kishoremannava --output migration.sql
    python export_glue_catalog.py --database my_db --catalog main --schema migrated
    python export_glue_catalog.py --all-databases --catalog main --output-dir ./ddl/

Prerequisites:
    pip install boto3
"""

import argparse
import json
import sys
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Hive SerDe to Spark data source format mapping
SERDE_TO_FORMAT = {
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe": "PARQUET",
    "org.apache.hadoop.hive.ql.io.orc.OrcSerde": "ORC",
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe": "CSV",
    "org.apache.hadoop.hive.serde2.OpenCSVSerde": "CSV",
    "org.apache.hive.hcatalog.data.JsonSerDe": "JSON",
    "org.apache.hadoop.hive.serde2.JsonSerDe": "JSON",
    "org.apache.hadoop.hive.serde2.avro.AvroSerDe": "AVRO",
    "com.databricks.spark.avro": "AVRO",
}

# Hive InputFormat to Spark format mapping (fallback if SerDe not recognized)
INPUT_FORMAT_TO_FORMAT = {
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat": "PARQUET",
    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat": "ORC",
    "org.apache.hadoop.mapred.TextInputFormat": "CSV",
    "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat": "AVRO",
}

# Hive type to Spark/Delta type mapping
HIVE_TYPE_MAP = {
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "integer": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "string": "STRING",
    "boolean": "BOOLEAN",
    "binary": "BINARY",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "void": "VOID",
}


def get_glue_client(region: str) -> Any:
    return boto3.client("glue", region_name=region)


def map_hive_type(hive_type: str) -> str:
    """Map Hive type to Spark SQL type."""
    hive_type_lower = hive_type.lower().strip()

    # Direct mapping
    if hive_type_lower in HIVE_TYPE_MAP:
        return HIVE_TYPE_MAP[hive_type_lower]

    # Decimal
    if hive_type_lower.startswith("decimal"):
        return hive_type.upper()

    # VARCHAR/CHAR → STRING
    if hive_type_lower.startswith(("varchar", "char")):
        return "STRING"

    # Complex types (pass through with recursive mapping)
    if hive_type_lower.startswith("array<"):
        inner = hive_type[6:-1]
        return f"ARRAY<{map_hive_type(inner)}>"

    if hive_type_lower.startswith("map<"):
        inner = hive_type[4:-1]
        # Split on first comma (key, value)
        parts = _split_type_params(inner)
        if len(parts) == 2:
            return f"MAP<{map_hive_type(parts[0])}, {map_hive_type(parts[1])}>"

    if hive_type_lower.startswith("struct<"):
        # Pass through struct types
        return hive_type.upper()

    # Fallback: return as-is
    return hive_type.upper()


def _split_type_params(type_str: str) -> list[str]:
    """Split type parameters respecting nested angle brackets."""
    parts = []
    depth = 0
    current = []
    for char in type_str:
        if char == "<":
            depth += 1
            current.append(char)
        elif char == ">":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current).strip())
    return parts


def detect_format(table: dict) -> str:
    """Detect the data format from table metadata."""
    storage = table.get("StorageDescriptor", {})
    serde = storage.get("SerdeInfo", {}).get("SerializationLibrary", "")
    input_fmt = storage.get("InputFormat", "")

    # Check SerDe first
    if serde in SERDE_TO_FORMAT:
        return SERDE_TO_FORMAT[serde]

    # Fallback to InputFormat
    if input_fmt in INPUT_FORMAT_TO_FORMAT:
        return INPUT_FORMAT_TO_FORMAT[input_fmt]

    # Check parameters for Delta/Iceberg
    params = table.get("Parameters", {})
    if params.get("table_type") == "DELTA" or "delta.lastCommitTimestamp" in params:
        return "DELTA"
    if params.get("table_type") == "ICEBERG" or "iceberg.catalog" in params:
        return "ICEBERG"

    return "UNKNOWN"


def generate_external_table_ddl(
    table: dict,
    database: str,
    uc_catalog: str,
    uc_schema: str | None = None,
) -> str:
    """Generate CREATE TABLE DDL for Unity Catalog external table."""
    schema = uc_schema or database
    table_name = table["Name"]
    storage = table.get("StorageDescriptor", {})
    location = storage.get("Location", "")
    data_format = detect_format(table)

    # Build column definitions
    columns = []
    for col in storage.get("Columns", []):
        col_name = col["Name"]
        col_type = map_hive_type(col.get("Type", "STRING"))
        comment = col.get("Comment", "")
        col_def = f"  `{col_name}` {col_type}"
        if comment:
            col_def += f" COMMENT '{comment}'"
        columns.append(col_def)

    # Partition columns
    partition_cols = []
    for pcol in table.get("PartitionKeys", []):
        col_name = pcol["Name"]
        col_type = map_hive_type(pcol.get("Type", "STRING"))
        partition_cols.append(f"  `{col_name}` {col_type}")

    # Build DDL
    ddl_parts = [f"CREATE EXTERNAL TABLE IF NOT EXISTS `{uc_catalog}`.`{schema}`.`{table_name}` ("]
    ddl_parts.append(",\n".join(columns))
    ddl_parts.append(")")

    if partition_cols:
        ddl_parts.append("PARTITIONED BY (")
        ddl_parts.append(",\n".join(partition_cols))
        ddl_parts.append(")")

    if data_format and data_format != "UNKNOWN":
        ddl_parts.append(f"USING {data_format}")

    if location:
        ddl_parts.append(f"LOCATION '{location}'")

    # Table comment
    table_comment = table.get("Description") or table.get("Parameters", {}).get(
        "comment", ""
    )
    if table_comment:
        ddl_parts.append(f"COMMENT '{table_comment}'")

    ddl_parts.append(";")
    return "\n".join(ddl_parts)


def generate_managed_table_ddl(
    table: dict,
    database: str,
    uc_catalog: str,
    uc_schema: str | None = None,
) -> str:
    """Generate CTAS DDL to create managed Delta table from external source."""
    schema = uc_schema or database
    table_name = table["Name"]
    storage = table.get("StorageDescriptor", {})
    location = storage.get("Location", "")
    data_format = detect_format(table)

    if data_format == "UNKNOWN":
        data_format = "PARQUET"

    source_format = data_format.lower()

    ddl = f"""-- Managed table (copies data and converts to Delta)
CREATE TABLE IF NOT EXISTS `{uc_catalog}`.`{schema}`.`{table_name}`
AS SELECT * FROM {source_format}.`{location}`;

-- After migration, optimize the table
OPTIMIZE `{uc_catalog}`.`{schema}`.`{table_name}`;
"""
    return ddl


def generate_schema_ddl(database: dict, uc_catalog: str, uc_schema: str | None = None) -> str:
    """Generate CREATE SCHEMA DDL."""
    schema_name = uc_schema or database["Name"]
    description = database.get("Description", "")
    comment = f" COMMENT '{description}'" if description else ""
    return f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{schema_name}`{comment};"


def export_database(
    glue_client: Any,
    database_name: str,
    uc_catalog: str,
    uc_schema: str | None = None,
    managed: bool = False,
) -> tuple[str, dict]:
    """Export a single database to DDL statements."""
    # Get database metadata
    db_response = glue_client.get_database(Name=database_name)
    db = db_response["Database"]

    ddl_statements = []
    stats = {"tables": 0, "partitioned_tables": 0, "formats": {}}

    # Schema DDL
    ddl_statements.append(generate_schema_ddl(db, uc_catalog, uc_schema))
    ddl_statements.append("")

    # Get all tables
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page.get("TableList", []):
            stats["tables"] += 1
            fmt = detect_format(table)
            stats["formats"][fmt] = stats["formats"].get(fmt, 0) + 1

            if table.get("PartitionKeys"):
                stats["partitioned_tables"] += 1

            ddl_statements.append(f"-- Table: {table['Name']} (format: {fmt})")
            if managed:
                ddl = generate_managed_table_ddl(
                    table, database_name, uc_catalog, uc_schema
                )
            else:
                ddl = generate_external_table_ddl(
                    table, database_name, uc_catalog, uc_schema
                )
            ddl_statements.append(ddl)
            ddl_statements.append("")

    return "\n".join(ddl_statements), stats


def main():
    parser = argparse.ArgumentParser(
        description="Export Glue Data Catalog to Unity Catalog DDL"
    )
    parser.add_argument(
        "--region", default="us-east-1", help="AWS region (default: us-east-1)"
    )
    parser.add_argument("--database", help="Export a specific database")
    parser.add_argument(
        "--all-databases", action="store_true", help="Export all databases"
    )
    parser.add_argument(
        "--catalog", required=True, help="Target Unity Catalog catalog name"
    )
    parser.add_argument("--schema", help="Target schema name (defaults to database name)")
    parser.add_argument(
        "--managed",
        action="store_true",
        help="Generate CTAS for managed Delta tables (default: external tables)",
    )
    parser.add_argument("--output", default="-", help="Output file (- for stdout)")
    parser.add_argument(
        "--output-dir", help="Output directory (one file per database)"
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only output statistics, no DDL",
    )
    args = parser.parse_args()

    glue_client = get_glue_client(args.region)
    all_ddl = []
    all_stats = {}

    # Header
    all_ddl.append(f"-- EMR to Databricks Migration: Glue Catalog Export")
    all_ddl.append(f"-- Target Catalog: {args.catalog}")
    all_ddl.append(f"-- Strategy: {'Managed (Delta)' if args.managed else 'External'}")
    all_ddl.append(f"-- Generated by: export_glue_catalog.py")
    all_ddl.append("")
    all_ddl.append(f"CREATE CATALOG IF NOT EXISTS `{args.catalog}`;")
    all_ddl.append(f"USE CATALOG `{args.catalog}`;")
    all_ddl.append("")

    databases = []
    if args.database:
        databases = [args.database]
    elif args.all_databases:
        paginator = glue_client.get_paginator("get_databases")
        for page in paginator.paginate():
            databases.extend([db["Name"] for db in page.get("DatabaseList", [])])
    else:
        parser.error("Specify --database or --all-databases")

    for db_name in databases:
        print(f"Exporting database: {db_name}", file=sys.stderr)
        try:
            ddl, stats = export_database(
                glue_client, db_name, args.catalog, args.schema, args.managed
            )
            all_stats[db_name] = stats

            if args.output_dir:
                import os

                os.makedirs(args.output_dir, exist_ok=True)
                filepath = os.path.join(args.output_dir, f"{db_name}.sql")
                with open(filepath, "w") as f:
                    f.write(ddl)
                print(f"  Written to {filepath}", file=sys.stderr)
            else:
                all_ddl.append(f"-- ========== Database: {db_name} ==========")
                all_ddl.append(ddl)

        except ClientError as e:
            print(f"  Error exporting {db_name}: {e}", file=sys.stderr)
            all_stats[db_name] = {"error": str(e)}

    # Output stats
    print("\n--- Export Summary ---", file=sys.stderr)
    for db_name, stats in all_stats.items():
        if "error" in stats:
            print(f"  {db_name}: ERROR - {stats['error']}", file=sys.stderr)
        else:
            print(
                f"  {db_name}: {stats['tables']} tables "
                f"({stats['partitioned_tables']} partitioned), "
                f"formats: {stats['formats']}",
                file=sys.stderr,
            )

    if args.stats_only:
        print(json.dumps(all_stats, indent=2))
        return

    # Write DDL output
    if not args.output_dir:
        output_text = "\n".join(all_ddl)
        if args.output == "-":
            print(output_text)
        else:
            with open(args.output, "w") as f:
                f.write(output_text)
            print(f"DDL written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()

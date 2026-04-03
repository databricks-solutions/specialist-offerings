#!/usr/bin/env python3
"""Generate Unity Catalog DDL from parsed Hive DDL JSON.

Usage:
    python generate_uc_ddl.py \
        --catalog aa_catalog \
        --schema retail_analytics \
        --format DELTA \
        --clustering liquid \
        [--suffix _ice] \
        [--prefix uc_] \
        [--input parsed.json]   # default: stdin

Output:
    UC-compatible SQL to stdout.
"""

import argparse
import json
import re
import sys

import sqlglot
from sqlglot import expressions as exp

# Hive TBLPROPERTIES to remove during migration
HIVE_PROPS_TO_REMOVE = {
    "orc.compress", "orc.stripe.size", "orc.row.index.stride", "orc.create.index",
    "parquet.compression",
    "transactional", "transactional_properties", "transient_lastDdlTime",
    "skip.header.line.count",
    "auto.purge",
}
HIVE_PROPS_PREFIX_REMOVE = ("hive.mapred.",)


def should_remove_prop(key):
    """Check if a Hive TBLPROPERTY should be removed."""
    if key in HIVE_PROPS_TO_REMOVE:
        return True
    return any(key.startswith(pfx) for pfx in HIVE_PROPS_PREFIX_REMOVE)


def determine_original_format(stmt):
    """Build a human-readable string describing the original Hive storage format."""
    fmt = stmt.get("storage_format")
    serde = stmt.get("serde")

    if isinstance(fmt, dict):
        # INPUT_OUTPUT_FORMAT type
        serde_label = serde.split(".")[-1] if serde and serde != "ROW_FORMAT_DELIMITED" else "CustomInputFormat"
        return f"TEXTFILE/{serde_label}"

    fmt_str = str(fmt).upper() if fmt else ""
    if serde and serde not in ("ROW_FORMAT_DELIMITED", None):
        serde_short = serde.split(".")[-1]
        return f"{fmt_str}/{serde_short}" if fmt_str else serde_short

    return fmt_str or "UNKNOWN"


def compute_col_pad(columns):
    """Calculate padding width so column types align."""
    if not columns:
        return 16
    max_name = max(len(c["name"]) for c in columns)
    return max(max_name + 1, 16)


def update_view_references(query_sql, catalog, schema, prefix, suffix):
    """Rewrite table references in view SQL to use 3-level namespace."""
    try:
        parsed = sqlglot.parse_one(query_sql, dialect="hive")
        for table in parsed.find_all(exp.Table):
            if table.catalog:
                continue  # already fully qualified
            old_name = table.name
            new_name = f"{prefix}{old_name}{suffix}"
            table.set("this", exp.to_identifier(new_name))
            table.set("db", exp.to_identifier(schema))
            table.set("catalog", exp.to_identifier(catalog))
        return parsed.sql(dialect="hive", pretty=True)
    except Exception:
        return query_sql


# ---------------------------------------------------------------------------
# Statement generators
# ---------------------------------------------------------------------------

def generate_create_table(stmt, config, table_counter):
    """Generate UC CREATE TABLE DDL."""
    catalog = config["catalog"]
    schema = config["schema"]
    prefix = config.get("prefix", "")
    suffix = config.get("suffix", "")
    uc_format = config["format"]
    clustering = config["clustering"]

    table_name = f"{prefix}{stmt['table']}{suffix}"
    fqn = f"{catalog}.{schema}.{table_name}"
    original_format = determine_original_format(stmt)
    is_external = stmt.get("is_external", False)
    partition_cols = stmt.get("partition_columns", [])
    clustered_by = stmt.get("clustered_by") or []

    # ---- Build change annotations ----
    changes = []
    changes.append(f"[a] Namespace: → {fqn}")

    fmt_display = stmt.get("storage_format")
    if isinstance(fmt_display, dict):
        fmt_display = "INPUTFORMAT/OUTPUTFORMAT"
    if fmt_display:
        changes.append(f"[b] STORED AS {fmt_display} → USING {uc_format}")

    if stmt.get("location"):
        changes.append("[c] LOCATION removed (managed table)")

    serde = stmt.get("serde")
    if serde and serde != "ROW_FORMAT_DELIMITED":
        changes.append(f"[d] {serde.split('.')[-1]} removed ({uc_format} handles natively)")
    elif serde == "ROW_FORMAT_DELIMITED":
        changes.append(f"[d] ROW FORMAT DELIMITED removed ({uc_format} handles natively)")

    removed_props = [k for k in stmt.get("tblproperties", {}) if should_remove_prop(k)]
    if removed_props:
        changes.append(f"[e] {', '.join(removed_props)} removed (Hive-specific)")

    if partition_cols and clustering == "liquid":
        pcnames = ", ".join(pc["name"] for pc in partition_cols)
        changes.append(f"[f] PARTITIONED BY ({pcnames}) → CLUSTER BY; columns merged into schema")

    if clustered_by and clustering == "liquid":
        nb = stmt.get("num_buckets", "?")
        changes.append(f"[f] CLUSTERED BY ... INTO BUCKETS → CLUSTER BY (Liquid Clustering)")

    changes.append("[g] Lineage TBLPROPERTIES added")

    # ---- Header comment block ----
    lines = []
    lines.append("-- =============================================================================")
    lines.append(f"-- Table {table_counter}: {table_name}")

    source_parts = ["EXTERNAL TABLE" if is_external else "Managed table"]
    if partition_cols:
        pcnames = ", ".join(pc["name"] for pc in partition_cols)
        source_parts.append(f"PARTITIONED BY ({pcnames})")
    if clustered_by:
        nb = stmt.get("num_buckets", "?")
        source_parts.append(f"CLUSTERED BY ({', '.join(clustered_by)}) INTO {nb} BUCKETS")
    source_parts.append(original_format)
    lines.append(f"-- Source: {', '.join(source_parts)}")
    lines.append("-- Changes:")
    for c in changes:
        lines.append(f"--   {c}")
    lines.append("-- =============================================================================")

    # ---- CREATE TABLE ----
    if_not_exists = "IF NOT EXISTS " if stmt.get("if_not_exists") else ""
    lines.append(f"CREATE TABLE {if_not_exists}{fqn} (")

    # Merge columns + partition columns (for liquid clustering)
    all_columns = list(stmt.get("columns", []))
    partition_names_set = set()
    if partition_cols and clustering == "liquid":
        for pc in partition_cols:
            partition_names_set.add(pc["name"])
            all_columns.append({
                "name": pc["name"],
                "type": pc.get("type") or "STRING",
                "_is_former_partition": True,
            })

    pad = compute_col_pad(all_columns)
    col_entries = []  # (main_part, inline_comment_or_None)
    for col in all_columns:
        name_padded = col["name"].ljust(pad)
        main = f"    {name_padded}{col['type']}"
        if col.get("comment") and not col.get("_is_former_partition"):
            main += f" COMMENT '{col['comment']}'"
        inline_comment = "-- formerly partition column" if col.get("_is_former_partition") else None
        col_entries.append((main, inline_comment))

    col_lines = []
    for i, (main, comment) in enumerate(col_entries):
        is_last = (i == len(col_entries) - 1)
        line = main + ("," if not is_last else "")
        if comment:
            line += f"    {comment}"
        col_lines.append(line)
    lines.append("\n".join(col_lines))
    lines.append(")")

    # USING format
    lines.append(f"USING {uc_format}")

    # Table COMMENT
    if stmt.get("comment"):
        lines.append(f"COMMENT '{stmt['comment']}'")

    # CLUSTER BY / PARTITIONED BY
    cluster_keys = []
    if clustering == "liquid":
        if partition_cols:
            cluster_keys = [pc["name"] for pc in partition_cols]
        if clustered_by:
            # Add clustered-by columns that aren't already partition keys
            for cb in clustered_by:
                if cb not in partition_names_set:
                    cluster_keys.append(cb)
            if not cluster_keys:
                cluster_keys = list(clustered_by)
    elif clustering == "preserve" and partition_cols:
        pcol_defs = ", ".join(f"{pc['name']} {pc.get('type', 'STRING')}" for pc in partition_cols)
        lines.append(f"PARTITIONED BY ({pcol_defs})")

    if cluster_keys:
        lines.append(f"CLUSTER BY ({', '.join(cluster_keys)})")

    # TBLPROPERTIES
    props = {}
    if uc_format == "DELTA":
        props["delta.autoOptimize.optimizeWrite"] = "true"
        props["delta.autoOptimize.autoCompact"] = "true"
    if uc_format == "ICEBERG" and clustering == "liquid" and cluster_keys:
        props["delta.enableDeletionVectors"] = "false"
        props["delta.enableRowTracking"] = "false"
    props["migrated_from"] = "hive"
    props["original_format"] = original_format

    prop_lines = [f"    '{k}' = '{v}'" for k, v in props.items()]
    lines.append("TBLPROPERTIES (")
    lines.append(",\n".join(prop_lines))
    lines.append(");")

    # ---- Post-table warnings ----
    if stmt.get("location"):
        hdfs_path = stmt["location"]
        serde_note = ""
        if serde and serde not in (None, "ROW_FORMAT_DELIMITED"):
            serde_note = f" with {serde.split('.')[-1]} parsing"
        lines.append(f"-- MANUAL REVIEW: Data loading — use COPY INTO or Auto Loader to ingest from")
        lines.append(f"-- original HDFS path {hdfs_path}{serde_note}")

    if isinstance(stmt.get("storage_format"), dict):
        sf = stmt["storage_format"]
        inp = sf.get("input_format", "").split(".")[-1]
        outp = sf.get("output_format", "").split(".")[-1]
        lines.append(f"-- MANUAL REVIEW: Custom InputFormat ({inp}) / OutputFormat ({outp})")
        if stmt.get("location"):
            lines.append(f"-- Data loading — use COPY INTO or Auto Loader from {stmt['location']}")

    if clustered_by and clustering == "liquid":
        nb = stmt.get("num_buckets", "?")
        lines.append(f"-- Note: CLUSTERED BY ({', '.join(clustered_by)}) INTO {nb} BUCKETS replaced by Liquid Clustering")

    if partition_cols and clustering == "liquid":
        lines.append("-- Note: PARTITIONED BY converted to CLUSTER BY (Liquid Clustering)")

    return "\n".join(lines)


def generate_create_view(stmt, config):
    """Generate UC CREATE VIEW DDL."""
    catalog = config["catalog"]
    schema = config["schema"]
    prefix = config.get("prefix", "")
    suffix = config.get("suffix", "")

    view_name = f"{prefix}{stmt['view']}{suffix}"
    fqn = f"{catalog}.{schema}.{view_name}"

    query_sql = stmt.get("query_sql", "")
    if query_sql:
        query_sql = update_view_references(query_sql, catalog, schema, prefix, suffix)

    lines = []
    lines.append("-- =============================================================================")
    lines.append(f"-- View: {view_name}")
    lines.append("-- Changes:")
    lines.append("--   [a] All table references updated to 3-level namespace")
    lines.append("-- =============================================================================")

    if_not_exists = "IF NOT EXISTS " if stmt.get("if_not_exists") else ""
    lines.append(f"CREATE VIEW {if_not_exists}{fqn} AS")
    if query_sql:
        lines.append(query_sql + ";")
    else:
        lines.append("-- WARNING: No query SQL found for this view")

    return "\n".join(lines)


def generate_create_schema(stmt, config):
    """Generate UC CREATE SCHEMA from a CREATE DATABASE (or COMMAND fallback)."""
    catalog = config["catalog"]
    schema = config["schema"]
    fqn = f"{catalog}.{schema}"

    lines = []
    lines.append("-- Database → Schema")

    raw = stmt.get("raw_sql", "")
    if raw:
        first_line = raw.strip().split("\n")[0].strip()
        # Remove leading block-comment prefix if present
        first_line = re.sub(r"^/\*.*?\*/\s*", "", first_line).strip()
        lines.append(f"-- Hive: {first_line}")

    lines.append("-- UC: LOCATION removed — Unity Catalog manages storage for managed schemas")

    # Extract comment
    comment = stmt.get("comment")
    if not comment and raw:
        m = re.search(r"COMMENT\s+'([^']*)'", raw, re.IGNORECASE)
        if m:
            comment = m.group(1)

    # Extract DBPROPERTIES
    db_props = dict(stmt.get("properties", {}))
    if not db_props and raw:
        m = re.search(r"DBPROPERTIES\s*\(([^)]+)\)", raw, re.IGNORECASE)
        if m:
            for pair in re.findall(r"'([^']+)'\s*=\s*'([^']+)'", m.group(1)):
                db_props[pair[0]] = pair[1]

    schema_line = f"CREATE SCHEMA IF NOT EXISTS {fqn}"
    lines.append(schema_line)

    if comment:
        lines.append(f"COMMENT '{comment} (migrated from Hive)'")

    db_props["migrated_from"] = "hive"
    prop_lines = [f"    '{k}' = '{v}'" for k, v in db_props.items()]
    lines.append("WITH DBPROPERTIES (")
    lines.append(",\n".join(prop_lines))
    lines.append(");")

    return "\n".join(lines)


def generate_use(stmt, config):
    """Generate UC USE CATALOG + USE SCHEMA."""
    lines = []
    lines.append("-- USE database → USE CATALOG + USE SCHEMA")
    lines.append(f"USE CATALOG {config['catalog']};")
    lines.append(f"USE SCHEMA {config['schema']};")
    return "\n".join(lines)


def generate_alter_partition_skip(stmt):
    """Generate skip comment for ALTER TABLE ADD PARTITION."""
    return f"-- SKIPPED: {stmt['raw_sql']};"


def generate_index_skip(stmt):
    """Generate skip comment for CREATE INDEX."""
    raw = stmt.get("raw_sql", "")
    first_line = raw.strip().split("\n")[0].strip()
    first_line = re.sub(r"^/\*.*?\*/\s*", "", first_line).strip()

    lines = []
    lines.append(f"-- SKIPPED: {first_line} ...")
    lines.append("-- Note: Hive indexes are deprecated since Hive 3.x and have no UC equivalent.")
    lines.append("-- Liquid Clustering on relevant columns provides similar query acceleration.")
    return "\n".join(lines)


def is_create_database_command(stmt):
    raw = stmt.get("raw_sql", "")
    return bool(re.search(r"CREATE\s+DATABASE", raw, re.IGNORECASE))


def is_create_index_command(stmt):
    raw = stmt.get("raw_sql", "")
    return bool(re.search(r"CREATE\s+INDEX", raw, re.IGNORECASE))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate UC DDL from parsed Hive DDL JSON")
    parser.add_argument("--catalog", required=True, help="Target UC catalog name")
    parser.add_argument("--schema", required=True, help="Target UC schema name")
    parser.add_argument("--format", required=True, choices=["DELTA", "ICEBERG"],
                        help="Table format (DELTA or ICEBERG)")
    parser.add_argument("--clustering", default="liquid", choices=["liquid", "preserve"],
                        help="Clustering strategy")
    parser.add_argument("--suffix", default="", help="Table name suffix")
    parser.add_argument("--prefix", default="", help="Table name prefix")
    parser.add_argument("--input", help="Input JSON file (default: stdin)")

    args = parser.parse_args()

    config = {
        "catalog": args.catalog,
        "schema": args.schema,
        "format": args.format,
        "clustering": args.clustering,
        "suffix": args.suffix,
        "prefix": args.prefix,
    }

    # Read parsed JSON
    if args.input:
        with open(args.input) as f:
            data = json.load(f)
    else:
        data = json.load(sys.stdin)

    statements = data.get("statements", [])

    # ---- Reorder: schema/use first, then tables/views, then alters/indexes ----
    def stmt_sort_key(s):
        t = s.get("type", "")
        raw = s.get("raw_sql", "")
        if t == "CREATE_DATABASE" or (t == "COMMAND" and re.search(r"CREATE\s+DATABASE", raw, re.I)):
            return 0
        if t == "USE_DATABASE":
            return 1
        if t == "CREATE_TABLE":
            return 2
        if t == "CREATE_VIEW":
            return 3
        if t == "ALTER_TABLE":
            return 4
        return 5

    statements = sorted(statements, key=stmt_sort_key)

    # ---- File header ----
    out = []
    out.append("-- =============================================================================")
    out.append(f"-- Unity Catalog DDL — Converted from Hive ({config['schema']})")
    naming = "unchanged"
    if config["suffix"]:
        naming = f"suffix={config['suffix']}"
    elif config["prefix"]:
        naming = f"prefix={config['prefix']}"
    out.append(f"-- Config: catalog={config['catalog']}, schema={config['schema']}, "
               f"format={config['format']},")
    out.append(f"--         naming={naming}, clustering={config['clustering']}")
    out.append("-- =============================================================================")
    out.append("")

    # ---- Process statements ----
    alter_partition_stmts = []
    index_stmts = []
    table_counter = 0

    for stmt in statements:
        stype = stmt.get("type", "")

        if stype == "CREATE_TABLE":
            table_counter += 1
            out.append(generate_create_table(stmt, config, table_counter))
            out.append("")

        elif stype == "CREATE_VIEW":
            out.append(generate_create_view(stmt, config))
            out.append("")

        elif stype == "CREATE_DATABASE":
            out.append(generate_create_schema(stmt, config))
            out.append("")

        elif stype == "USE_DATABASE":
            out.append(generate_use(stmt, config))
            out.append("")

        elif stype == "ALTER_TABLE":
            if stmt.get("operation") == "ADD_PARTITION" and config["clustering"] == "liquid":
                alter_partition_stmts.append(stmt)
            else:
                out.append(f"-- {stmt.get('raw_sql', 'ALTER TABLE ...')}")
                out.append("")

        elif stype == "COMMAND":
            if is_create_database_command(stmt):
                out.append(generate_create_schema(stmt, config))
                out.append("")
            elif is_create_index_command(stmt):
                index_stmts.append(stmt)
            else:
                out.append(f"-- UNHANDLED COMMAND: {stmt.get('raw_sql', '')}")
                out.append("")

        elif stype == "CREATE_INDEX":
            index_stmts.append(stmt)

        else:
            raw = stmt.get("raw_sql", "")
            if raw:
                out.append(f"-- UNHANDLED ({stype}): {raw}")
                out.append("")

    # ---- Deferred sections ----

    if alter_partition_stmts:
        out.append("-- =============================================================================")
        out.append("-- ALTER TABLE — Partition additions (no longer needed with Liquid Clustering)")
        out.append("-- =============================================================================")
        for s in alter_partition_stmts:
            out.append(generate_alter_partition_skip(s))
        out.append("-- Note: Liquid Clustering does not require explicit partition management.")
        out.append("-- Data is automatically organized by cluster keys.")
        out.append("")

    if index_stmts:
        out.append("-- =============================================================================")
        out.append("-- Index (deprecated) — not supported in Unity Catalog")
        out.append("-- =============================================================================")
        for s in index_stmts:
            out.append(generate_index_skip(s))
        out.append("")

    print("\n".join(out))


if __name__ == "__main__":
    main()

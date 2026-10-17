#!/usr/bin/env python3
"""Parse Hive DDL using sqlglot and output structured JSON.

Usage:
    python parse_hive_ddl.py <input.hql>

Output:
    JSON to stdout with extracted metadata for each DDL statement.
"""

import json
import sys
from pathlib import Path

import sqlglot
from sqlglot import expressions as exp


def extract_tblproperties(node):
    """Extract TBLPROPERTIES from a CREATE statement's properties."""
    props = {}
    properties = node.args.get("properties")
    if not properties:
        return props
    # These property types are NOT TBLPROPERTIES — they are structural DDL clauses
    structural_types = (
        exp.PartitionedByProperty, exp.FileFormatProperty,
        exp.SchemaCommentProperty, exp.LocationProperty,
        exp.RowFormatSerdeProperty, exp.RowFormatDelimitedProperty,
        exp.ExternalProperty, exp.ClusteredByProperty,
    )
    for prop in properties.expressions:
        if isinstance(prop, exp.Property) and not isinstance(prop, structural_types):
            key = prop.name
            value = prop.args.get("value")
            if value is not None:
                props[key] = value.this if isinstance(value, exp.Literal) else str(value)
    return props


def extract_columns(node):
    """Extract column definitions from a CREATE TABLE statement."""
    columns = []
    schema = node.this
    if not schema or not hasattr(schema, "expressions"):
        return columns
    for col_expr in schema.expressions:
        if isinstance(col_expr, exp.ColumnDef):
            col_info = {
                "name": col_expr.this.name,
                "type": col_expr.args["kind"].sql(dialect="hive") if col_expr.args.get("kind") else "UNKNOWN",
                "comment": None,
            }
            for constraint in col_expr.args.get("constraints", []):
                if isinstance(constraint, exp.ColumnConstraint):
                    kind = constraint.args.get("kind")
                    if isinstance(kind, exp.CommentColumnConstraint):
                        col_info["comment"] = kind.this.this
            columns.append(col_info)
    return columns


def extract_partition_columns(node):
    """Extract PARTITIONED BY columns."""
    partitions = []
    properties = node.args.get("properties")
    if not properties:
        return partitions
    for prop in properties.expressions:
        if isinstance(prop, exp.PartitionedByProperty):
            schema_or_cols = prop.this
            if hasattr(schema_or_cols, "expressions"):
                for col in schema_or_cols.expressions:
                    if isinstance(col, exp.ColumnDef):
                        partitions.append({
                            "name": col.this.name,
                            "type": col.args["kind"].sql(dialect="hive") if col.args.get("kind") else "STRING",
                        })
                    elif isinstance(col, exp.Column):
                        partitions.append({"name": col.name, "type": None})
    return partitions


def extract_storage_format(node):
    """Extract STORED AS format."""
    properties = node.args.get("properties")
    if not properties:
        return None
    for prop in properties.expressions:
        if isinstance(prop, exp.FileFormatProperty):
            fmt = prop.this
            if isinstance(fmt, exp.InputOutputFormat):
                return {
                    "type": "INPUT_OUTPUT_FORMAT",
                    "input_format": fmt.args.get("input_format", exp.Literal.string("")).this,
                    "output_format": fmt.args.get("output_format", exp.Literal.string("")).this,
                }
            return str(fmt).upper() if fmt else None
    return None


def extract_serde(node):
    """Extract ROW FORMAT SERDE info."""
    properties = node.args.get("properties")
    if not properties:
        return None, {}
    for prop in properties.expressions:
        if isinstance(prop, exp.RowFormatSerdeProperty):
            serde_class = prop.this.this if isinstance(prop.this, exp.Literal) else str(prop.this)
            serde_props = {}
            serde_properties_node = prop.args.get("serde_properties")
            if serde_properties_node:
                # May be a SerdeProperties wrapper or a list
                items = serde_properties_node.expressions if hasattr(serde_properties_node, "expressions") else serde_properties_node
                for sp in items:
                    if isinstance(sp, exp.Property):
                        key = sp.name
                        val = sp.args.get("value")
                        serde_props[key] = val.this if isinstance(val, exp.Literal) else str(val)
            return serde_class, serde_props
        if isinstance(prop, exp.RowFormatDelimitedProperty):
            fields = prop.args.get("fields")
            field_sep = fields.this if fields and isinstance(fields, exp.Literal) else str(fields) if fields else None
            return "ROW_FORMAT_DELIMITED", {"fields_terminated_by": field_sep}
    return None, {}


def extract_location(node):
    """Extract LOCATION clause."""
    properties = node.args.get("properties")
    if not properties:
        return None
    for prop in properties.expressions:
        if isinstance(prop, exp.LocationProperty):
            return prop.this.this if isinstance(prop.this, exp.Literal) else str(prop.this)
    return None


def extract_clustered_by(node):
    """Extract CLUSTERED BY / SORTED BY / INTO BUCKETS."""
    properties = node.args.get("properties")
    if not properties:
        return None, None, None
    clustered_by = None
    sorted_by = None
    num_buckets = None
    for prop in properties.expressions:
        if isinstance(prop, exp.ClusteredByProperty):
            # Columns are in prop.expressions (not prop.this) in sqlglot v28+
            col_exprs = prop.expressions if prop.expressions else (prop.this.expressions if prop.this and hasattr(prop.this, "expressions") else [])
            clustered_by = [col.name for col in col_exprs]
            sorted_by_list = prop.args.get("sorted_by")
            if sorted_by_list:
                sorted_by = []
                for item in sorted_by_list:
                    # sorted_by items may be Ordered(this=Column(...)) or Column(...)
                    if hasattr(item, "this") and hasattr(item.this, "name"):
                        sorted_by.append(item.this.name)
                    elif hasattr(item, "name"):
                        sorted_by.append(item.name)
            num_buckets_expr = prop.args.get("buckets")
            if num_buckets_expr:
                num_buckets = int(num_buckets_expr.this) if isinstance(num_buckets_expr, exp.Literal) else None
    return clustered_by, sorted_by, num_buckets


def extract_comment(node):
    """Extract table-level COMMENT."""
    properties = node.args.get("properties")
    if not properties:
        return None
    for prop in properties.expressions:
        if isinstance(prop, exp.SchemaCommentProperty):
            return prop.this.this if isinstance(prop.this, exp.Literal) else str(prop.this)
    return None


def is_external(node):
    """Check if table is EXTERNAL."""
    properties = node.args.get("properties")
    if not properties:
        return False
    return any(isinstance(p, exp.ExternalProperty) for p in properties.expressions)


def extract_db_properties(node):
    """Extract database properties (WITH DBPROPERTIES)."""
    props = {}
    properties = node.args.get("properties")
    if not properties:
        return props
    for prop in properties.expressions:
        if isinstance(prop, exp.Property):
            key = prop.name
            val = prop.args.get("value")
            if val is not None:
                props[key] = val.this if isinstance(val, exp.Literal) else str(val)
    return props


def parse_create_table(stmt):
    """Parse a CREATE TABLE / CREATE EXTERNAL TABLE statement."""
    table_name = stmt.this.this.name if stmt.this and stmt.this.this else "UNKNOWN"

    columns = extract_columns(stmt)
    partition_columns = extract_partition_columns(stmt)
    storage_format = extract_storage_format(stmt)
    serde, serde_properties = extract_serde(stmt)
    location = extract_location(stmt)
    tblprops = extract_tblproperties(stmt)
    clustered_by, sorted_by, num_buckets = extract_clustered_by(stmt)
    comment = extract_comment(stmt)
    external = is_external(stmt)

    # Filter out partition columns from the main column list
    # (sqlglot may merge them depending on dialect handling)
    partition_names = {pc["name"] for pc in partition_columns}
    columns = [c for c in columns if c["name"] not in partition_names]

    return {
        "type": "CREATE_TABLE",
        "table": table_name,
        "is_external": external,
        "if_not_exists": stmt.args.get("exists") is not None,
        "columns": columns,
        "partition_columns": partition_columns,
        "storage_format": storage_format,
        "serde": serde,
        "serde_properties": serde_properties,
        "location": location,
        "tblproperties": tblprops,
        "clustered_by": clustered_by,
        "sorted_by": sorted_by,
        "num_buckets": num_buckets,
        "comment": comment,
    }


def parse_create_view(stmt):
    """Parse a CREATE VIEW statement."""
    view_name = stmt.this.this.name if stmt.this and stmt.this.this else "UNKNOWN"
    query = stmt.expression
    return {
        "type": "CREATE_VIEW",
        "view": view_name,
        "if_not_exists": stmt.args.get("exists") is not None,
        "query_sql": query.sql(dialect="hive") if query else None,
    }


def parse_create_database(stmt):
    """Parse a CREATE DATABASE statement."""
    this = stmt.this
    if hasattr(this, "name"):
        db_name = this.name
    elif hasattr(this, "this"):
        db_name = this.this.name if hasattr(this.this, "name") else str(this.this)
    else:
        db_name = str(this)
    return {
        "type": "CREATE_DATABASE",
        "database": db_name,
        "if_not_exists": stmt.args.get("exists") is not None,
        "comment": extract_comment(stmt),
        "location": extract_location(stmt),
        "properties": extract_db_properties(stmt),
    }


def parse_alter_table(stmt):
    """Parse an ALTER TABLE / ALTER statement."""
    # exp.Alter has .this as the Table expression
    table_expr = stmt.this
    if hasattr(table_expr, "this") and hasattr(table_expr.this, "name"):
        table_name = table_expr.this.name
    elif hasattr(table_expr, "name"):
        table_name = table_expr.name
    else:
        table_name = str(table_expr)

    actions = stmt.args.get("actions") or []
    result = {
        "type": "ALTER_TABLE",
        "table": table_name,
        "operation": "UNKNOWN",
        "raw_sql": stmt.sql(dialect="hive"),
    }
    for action in actions:
        type_name = type(action).__name__
        if "AddPartition" in type_name or isinstance(action, exp.AddPartition):
            result["operation"] = "ADD_PARTITION"
    return result


def parse_use(stmt):
    """Parse a USE statement."""
    this = stmt.this
    if hasattr(this, "name"):
        db_name = this.name
    elif hasattr(this, "this"):
        db_name = this.this.name if hasattr(this.this, "name") else str(this.this)
    else:
        db_name = str(this)
    return {
        "type": "USE_DATABASE",
        "database": db_name,
    }


def parse_statement(stmt):
    """Route a parsed statement to the appropriate handler."""
    if isinstance(stmt, exp.Create):
        kind = stmt.args.get("kind", "").upper()
        if kind == "TABLE":
            return parse_create_table(stmt)
        elif kind == "VIEW":
            return parse_create_view(stmt)
        elif kind in ("DATABASE", "SCHEMA"):
            return parse_create_database(stmt)
        elif kind == "INDEX":
            return {
                "type": "CREATE_INDEX",
                "raw_sql": stmt.sql(dialect="hive"),
                "unsupported": True,
                "warning": "Hive indexes are deprecated and have no UC equivalent.",
            }
        else:
            return {
                "type": f"CREATE_{kind}",
                "raw_sql": stmt.sql(dialect="hive"),
            }
    elif isinstance(stmt, exp.Alter):
        return parse_alter_table(stmt)
    elif isinstance(stmt, exp.Use):
        return parse_use(stmt)
    else:
        return {
            "type": type(stmt).__name__.upper(),
            "raw_sql": stmt.sql(dialect="hive"),
            "warning": "Unrecognized statement type — passed through as raw SQL.",
        }


def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_hive_ddl.py <input.hql>", file=sys.stderr)
        sys.exit(1)

    input_file = Path(sys.argv[1])
    if not input_file.exists():
        print(f"Error: File not found: {input_file}", file=sys.stderr)
        sys.exit(1)

    sql_text = input_file.read_text()

    # Parse all statements
    try:
        parsed_stmts = sqlglot.parse(sql_text, dialect="hive")
    except Exception as e:
        print(json.dumps({
            "source_file": input_file.name,
            "error": f"Failed to parse file: {str(e)}",
            "statements": [],
            "summary": {"total_statements": 0},
        }, indent=2))
        sys.exit(1)

    statements = []
    counts = {"tables": 0, "views": 0, "databases": 0, "alter_statements": 0, "unsupported": 0, "use_statements": 0}

    for stmt in parsed_stmts:
        if stmt is None:
            continue
        try:
            result = parse_statement(stmt)
            statements.append(result)

            stmt_type = result.get("type", "")
            if stmt_type == "CREATE_TABLE":
                counts["tables"] += 1
            elif stmt_type == "CREATE_VIEW":
                counts["views"] += 1
            elif stmt_type == "CREATE_DATABASE":
                counts["databases"] += 1
            elif stmt_type == "ALTER_TABLE":
                counts["alter_statements"] += 1
            elif stmt_type == "USE_DATABASE":
                counts["use_statements"] += 1
            if result.get("unsupported"):
                counts["unsupported"] += 1
        except Exception as e:
            statements.append({
                "type": "PARSE_ERROR",
                "raw_sql": stmt.sql(dialect="hive") if hasattr(stmt, "sql") else str(stmt),
                "error": str(e),
            })

    output = {
        "source_file": input_file.name,
        "statements": statements,
        "summary": {
            "total_statements": len(statements),
            **counts,
        },
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()

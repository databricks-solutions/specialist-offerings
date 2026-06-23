"""Tag-driven expectations.

At pipeline definition time, look up UC column tags for the target table,
join to the rules table, fill the {column} placeholder, and expose:
  - get_expectations(table) -> {rule_id: expression} for @dp.expect_all*
  - failed_expectations_col(expectations) -> Column producing ARRAY<STRING>
    of the rule names that failed for each row.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

import _config as cfg


def get_expectations(target_table_short_name: str) -> dict[str, str]:
    """Returns {f"{rule_name}__{column}": expression}. Empty dict if no tags yet."""
    spark = SparkSession.getActiveSession()
    rows = spark.sql(f"""
        SELECT ct.column_name, ct.tag_name, r.rule_name, r.expression_template
        FROM system.information_schema.column_tags ct
        JOIN {cfg.RULES_TABLE} r ON ct.tag_name = r.tag_name
        WHERE ct.catalog_name = '{cfg.CATALOG}'
          AND ct.schema_name  = '{cfg.SCHEMA}'
          AND ct.table_name   = '{target_table_short_name}'
    """).collect()

    out: dict[str, str] = {}
    for row in rows:
        expr = row.expression_template.replace("{column}", row.column_name)
        out[f"{row.rule_name}__{row.column_name}"] = expr
    return out


def failed_expectations_col(expectations: dict[str, str]):
    """Column producing ARRAY<STRING> of failed rule names per row.
    Empty array (never null) when expectations is empty, so the column schema stays stable."""
    if not expectations:
        return F.array().cast("array<string>")
    when_exprs = [
        F.when(F.expr(f"NOT ({expr}) OR ({expr}) IS NULL"), F.lit(name))
         .otherwise(F.lit(None).cast("string"))
        for name, expr in expectations.items()
    ]
    return F.array_compact(F.array(*when_exprs))

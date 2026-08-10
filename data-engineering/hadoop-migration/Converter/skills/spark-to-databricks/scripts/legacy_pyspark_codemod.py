#!/usr/bin/env python3
"""Mechanical transforms for legacy CDH PySpark → Databricks notebook style.

Applies deterministic rewrites from PYSPARK_MIGRATION.md. Use as a baseline
before LLM-assisted /convert spark refinement.

Usage:
    python legacy_pyspark_codemod.py <input.py> [-o output.py] [--catalog main]
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import List, Optional


LEGACY_IMPORTS = {
    "from pyspark import SparkContext, SparkConf",
    "from pyspark import SparkConf, SparkContext",
    "from pyspark.sql import HiveContext",
    "from pyspark.sql import SQLContext",
}

REMOVED_LINE_PATTERNS = (
    re.compile(r"^\s*conf\s*=\s*SparkConf\(\)"),
    re.compile(r"^\s*sc\s*=\s*SparkContext\("),
    re.compile(r"^\s*sqlContext\s*=\s*HiveContext\("),
    re.compile(r"^\s*sqlContext\s*=\s*SQLContext\("),
    re.compile(r"^\s*hiveContext\s*=\s*HiveContext\("),
    re.compile(r"^\s*spark\s*=\s*HiveContext\("),
    re.compile(r"^\s*spark\s*=\s*SQLContext\("),
    re.compile(r"^\s*sc\.stop\(\)\s*$"),
    re.compile(r"^\s*spark\.stop\(\)\s*$"),
    re.compile(r"^\s*if __name__ == ['\"]__main__['\"]:\s*$"),
    re.compile(r"^\s*main\(\)\s*$"),
    re.compile(r"^\s*def main\(\):\s*$"),
    re.compile(r"^\s*try:\s*$"),
    re.compile(r"^\s*except\b"),
    re.compile(r"^\s*finally:\s*$"),
    re.compile(r"^\s*import traceback\s*$"),
    re.compile(r"^\s*traceback\.print_exc\(\)\s*$"),
    re.compile(r"^\s*raise\s*$"),
    re.compile(r'^\s*spark\.conf\.set\(\s*["\']spark\.sql\.shuffle\.partitions["\']'),
)


def _extract_shuffle_partitions(source: str) -> Optional[str]:
    match = re.search(
        r'(?:sqlContext|spark)\.(?:setConf|conf\.set)\(\s*["\']spark\.sql\.shuffle\.partitions["\']\s*,\s*["\'](\d+)["\']\s*\)',
        source,
    )
    return match.group(1) if match else None


def _strip_module_docstring(source: str) -> str:
    source = re.sub(r"^#!.*\n", "", source)
    source = re.sub(r"^# -\*- coding:.*\n", "", source)
    source = re.sub(r'^""".*?"""\s*\n', "", source, count=1, flags=re.DOTALL)
    return source


def _replace_session_api(source: str) -> str:
    source = re.sub(r"\bsqlContext\b", "spark", source)
    source = re.sub(r"\bhiveContext\b", "spark", source)
    source = re.sub(
        r"spark\.setConf\(\s*(['\"]spark\.sql\.shuffle\.partitions['\"])\s*,\s*(['\"][^'\"]+['\"])\s*\)",
        r"spark.conf.set(\1, \2)",
        source,
    )
    return source


def _replace_hdfs_paths(source: str, catalog: str) -> str:
    def _hdfs_repl(match: re.Match) -> str:
        path = match.group(1)
        if path.startswith("/data/"):
            path = path[len("/data"):]
        return f'"/Volumes/{catalog}{path}"'

    return re.sub(r'["\']hdfs://[^"\']*?(/[^"\']*)["\']', _hdfs_repl, source)


def _upgrade_table_names(source: str, catalog: str) -> str:
    def _save_as_table(match: re.Match) -> str:
        name = match.group(1)
        if name.count(".") >= 2:
            return match.group(0)
        return f'.saveAsTable("{catalog}.{name}")'

    source = re.sub(r'\.saveAsTable\(\s*["\']([^"\']+)["\']\s*\)', _save_as_table, source)

    def _select_star_table(match: re.Match) -> str:
        table = match.group(1)
        if table.count(".") >= 2:
            return match.group(0)
        return f'spark.table("{catalog}.{table}")'

    return re.sub(
        r'spark\.sql\(\s*["\']SELECT \* FROM ([^"\']+)["\']\s*\)',
        _select_star_table,
        source,
    )


def _fix_python2_prints(source: str) -> str:
    def _print_percent(match: re.Match) -> str:
        template, var = match.group(1), match.group(2).strip()
        template = template.replace("%d", f"{{{var}}}").replace("%s", f"{{{var}}}")
        return f'print(f"{template}")'

    source = re.sub(
        r'print\(\s*["\']([^"\']*%[ds][^"\']*)["\']\s*%\s*([^)]+)\)',
        _print_percent,
        source,
    )
    return source


def _remove_coalesce(source: str) -> str:
    source = re.sub(r"\\\s*\n\s*\.coalesce\(1\)\s*\\\s*\n", " \\\n", source)
    source = re.sub(r"\.coalesce\(1\)\s*\\\s*\n\s*", "", source)
    source = re.sub(r"\.coalesce\(1\)\s*\n\s*\.write", ".write", source)
    return source


def _strip_legacy_lines(lines: List[str]) -> List[str]:
    kept: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped in LEGACY_IMPORTS:
            continue
        if any(pat.match(line) for pat in REMOVED_LINE_PATTERNS):
            continue
        if stripped.startswith("print(") and "failed:" in stripped:
            continue
        kept.append(line)
    return kept


def _normalize_indent(lines: List[str]) -> List[str]:
    """Dedent main()/try-block body while keeping imports at column 0."""
    result: List[str] = []
    for line in lines:
        if line.startswith("        "):
            result.append(line[8:])
        elif line.startswith("    ") and not line.startswith("from ") and not line.startswith("import "):
            result.append(line[4:])
        else:
            result.append(line)
    # Drop leftover docstring fragments from partial stripping
    return [
        line for line in result
        if line.strip() not in ('"""', "'''")
        and not (line.strip().startswith('"""') and line.strip().endswith('"""'))
        and not line.strip().startswith("Compatible with Spark")
        and not line.strip().endswith(".scala")
        and not line.strip().endswith("aggregations.")
    ]


def _build_header(catalog: str) -> str:
    return (
        f"# Converted from legacy CDH PySpark (mechanical codemod)\n"
        f"# Catalog placeholder: {catalog} — confirm with customer\n"
        f"# Review: coalesce(1) removed, session init stripped for notebook use\n"
    )


def convert_legacy_pyspark(source: str, catalog: str = "main") -> str:
    """Apply mechanical legacy PySpark → Databricks notebook transforms."""
    shuffle = _extract_shuffle_partitions(source)

    transformed = _strip_module_docstring(source)
    transformed = _replace_session_api(transformed)
    transformed = _replace_hdfs_paths(transformed, catalog)
    transformed = _upgrade_table_names(transformed, catalog)
    transformed = _fix_python2_prints(transformed)
    transformed = _remove_coalesce(transformed)

    lines = _normalize_indent(_strip_legacy_lines(transformed.splitlines()))
    body = "\n".join(lines).strip()
    body = re.sub(r"\n{3,}", "\n\n", body)

    prefix = _build_header(catalog)
    if shuffle:
        prefix += f'\nspark.conf.set("spark.sql.shuffle.partitions", "{shuffle}")\n'

    return prefix + "\n" + body + "\n"


def assert_no_legacy_patterns(source: str) -> List[str]:
    """Return list of legacy patterns still present (for tests)."""
    violations: List[str] = []
    checks = [
        ("HiveContext", r"\bHiveContext\b"),
        ("SparkContext init", r"\bSparkContext\s*\("),
        ("SparkConf init", r"\bSparkConf\s*\("),
        ("hdfs:// path", r"hdfs://"),
        ("sc.stop()", r"\bsc\.stop\(\)"),
        ("sqlContext", r"\bsqlContext\b"),
    ]
    for label, pattern in checks:
        if re.search(pattern, source):
            violations.append(label)
    return violations


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Convert legacy PySpark to Databricks style")
    parser.add_argument("input", help="Source .py file")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    parser.add_argument("--catalog", default="main", help="Unity Catalog name (default: main)")
    args = parser.parse_args(argv)

    with open(args.input, "r", encoding="utf-8") as f:
        source = f.read()

    result = convert_legacy_pyspark(source, catalog=args.catalog)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result)
    else:
        sys.stdout.write(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

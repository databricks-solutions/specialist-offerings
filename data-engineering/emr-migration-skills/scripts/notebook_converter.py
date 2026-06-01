#!/usr/bin/env python3
"""
Notebook Format Converter

Converts Zeppelin (.json/.zpln) and Jupyter (.ipynb) notebooks
to Databricks Python notebook format (.py).

Usage:
    python notebook_converter.py --input notebook.json --output notebook.py
    python notebook_converter.py --input notebook.ipynb --output notebook.py
    python notebook_converter.py --input-dir ./notebooks/ --output-dir ./converted/

Prerequisites:
    No external dependencies (stdlib only)
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Zeppelin interpreter to Databricks magic command mapping
ZEPPELIN_MAGIC_MAP = {
    "%pyspark": "# COMMAND ----------",  # Default in Databricks (Python)
    "%spark": "# MAGIC %scala",
    "%spark.sql": "# MAGIC %sql",
    "%sql": "# MAGIC %sql",
    "%sh": "# MAGIC %sh",
    "%md": "# MAGIC %md",
    "%r": "# MAGIC %r",
    "%python": "# COMMAND ----------",  # Default
    "%angular": "# NOTE: Angular interpreter not supported on Databricks. Use dbutils.widgets instead.",
}

# Zeppelin API to Databricks API mapping
ZEPPELIN_API_REPLACEMENTS = [
    ("z.show(", "display("),
    ("z.input(", "dbutils.widgets.text("),
    ("z.select(", "dbutils.widgets.dropdown("),
    ("z.checkbox(", "dbutils.widgets.multiselect("),
    ("z.textbox(", "dbutils.widgets.text("),
    ("z.run(", "dbutils.notebook.run("),
    ("z.getInterpreterContext()", "# z.getInterpreterContext() not available on Databricks"),
]


def convert_zeppelin_notebook(input_path: str) -> str:
    """Convert a Zeppelin notebook to Databricks Python format."""
    with open(input_path) as f:
        notebook = json.load(f)

    lines = []
    lines.append("# Databricks notebook source")
    lines.append(f"# Converted from Zeppelin notebook: {os.path.basename(input_path)}")
    lines.append("")

    # Get notebook name
    name = notebook.get("name", "Untitled")
    lines.append(f"# Original notebook: {name}")
    lines.append("")

    # Process paragraphs
    paragraphs = notebook.get("paragraphs", [])
    for i, para in enumerate(paragraphs):
        text = para.get("text", "").strip()
        title = para.get("title", "")
        status = para.get("status", "")

        if not text:
            continue

        # Add cell separator (Databricks format)
        if i > 0:
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")

        # Add title as comment if present
        if title:
            lines.append(f"# TITLE: {title}")

        # Parse interpreter/magic command
        first_line = text.split("\n")[0].strip()
        remaining = text

        # Check for Zeppelin magic
        magic = None
        for zep_magic in ZEPPELIN_MAGIC_MAP:
            if first_line.startswith(zep_magic):
                magic = zep_magic
                remaining = text[len(first_line):].strip()
                if not remaining:
                    remaining = "\n".join(text.split("\n")[1:]).strip()
                break

        if magic:
            dbr_magic = ZEPPELIN_MAGIC_MAP[magic]
            if magic in ("%md",):
                # Markdown cell
                lines.append("# MAGIC %md")
                for md_line in remaining.split("\n"):
                    lines.append(f"# MAGIC {md_line}")
            elif magic in ("%spark.sql", "%sql"):
                # SQL cell
                lines.append("# MAGIC %sql")
                for sql_line in remaining.split("\n"):
                    lines.append(f"# MAGIC {sql_line}")
            elif magic in ("%sh",):
                # Shell cell
                lines.append("# MAGIC %sh")
                for sh_line in remaining.split("\n"):
                    lines.append(f"# MAGIC {sh_line}")
            elif magic == "%scala" or magic == "%spark":
                # Scala cell
                lines.append("# MAGIC %scala")
                for scala_line in remaining.split("\n"):
                    lines.append(f"# MAGIC {scala_line}")
            elif magic == "%r":
                lines.append("# MAGIC %r")
                for r_line in remaining.split("\n"):
                    lines.append(f"# MAGIC {r_line}")
            elif magic == "%angular":
                lines.append("# WARNING: Angular interpreter not supported on Databricks")
                lines.append("# Use dbutils.widgets for interactive elements")
                for ang_line in remaining.split("\n"):
                    lines.append(f"# {ang_line}")
            else:
                # Default: Python code
                code = apply_api_replacements(remaining)
                lines.append(code)
        else:
            # No magic: treat as Python (default in Zeppelin with PySpark)
            code = apply_api_replacements(text)
            lines.append(code)

    return "\n".join(lines)


def convert_jupyter_notebook(input_path: str) -> str:
    """Convert a Jupyter notebook to Databricks Python format."""
    with open(input_path) as f:
        notebook = json.load(f)

    lines = []
    lines.append("# Databricks notebook source")
    lines.append(f"# Converted from Jupyter notebook: {os.path.basename(input_path)}")
    lines.append("")

    # Get kernel info
    kernel = notebook.get("metadata", {}).get("kernelspec", {})
    kernel_name = kernel.get("display_name", "Python")
    lines.append(f"# Original kernel: {kernel_name}")
    lines.append("")

    cells = notebook.get("cells", [])
    for i, cell in enumerate(cells):
        cell_type = cell.get("cell_type", "code")
        source = cell.get("source", [])

        if isinstance(source, list):
            source_text = "".join(source)
        else:
            source_text = source

        source_text = source_text.strip()
        if not source_text:
            continue

        # Add cell separator
        if i > 0:
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")

        if cell_type == "markdown":
            lines.append("# MAGIC %md")
            for md_line in source_text.split("\n"):
                lines.append(f"# MAGIC {md_line}")

        elif cell_type == "code":
            # Check for cell magics
            if source_text.startswith("%%sql"):
                lines.append("# MAGIC %sql")
                sql_code = "\n".join(source_text.split("\n")[1:])
                for sql_line in sql_code.split("\n"):
                    lines.append(f"# MAGIC {sql_line}")
            elif source_text.startswith("%%bash") or source_text.startswith("%%sh"):
                lines.append("# MAGIC %sh")
                sh_code = "\n".join(source_text.split("\n")[1:])
                for sh_line in sh_code.split("\n"):
                    lines.append(f"# MAGIC {sh_line}")
            elif source_text.startswith("%%scala"):
                lines.append("# MAGIC %scala")
                scala_code = "\n".join(source_text.split("\n")[1:])
                for scala_line in scala_code.split("\n"):
                    lines.append(f"# MAGIC {scala_line}")
            else:
                # Regular Python code
                code = apply_api_replacements(source_text)
                # Replace common Jupyter-specific patterns
                code = code.replace(
                    "%matplotlib inline",
                    "# %matplotlib inline  # Not needed on Databricks, use display()",
                )
                lines.append(code)

        elif cell_type == "raw":
            lines.append(f"# Raw cell:")
            for raw_line in source_text.split("\n"):
                lines.append(f"# {raw_line}")

    return "\n".join(lines)


def apply_api_replacements(code: str) -> str:
    """Apply Zeppelin API to Databricks API replacements."""
    for old, new in ZEPPELIN_API_REPLACEMENTS:
        code = code.replace(old, new)

    # Replace SparkContext creation (pre-initialized on Databricks)
    if "SparkContext()" in code or "SparkContext(conf" in code:
        code = (
            "# NOTE: SparkContext is pre-initialized as 'sc' on Databricks\n"
            "# " + code.replace("\n", "\n# ")
            if "sc = " in code or "sc=" in code
            else code
        )

    # Replace SparkSession.builder creation
    if "SparkSession.builder" in code and (".getOrCreate()" in code or ".create()" in code):
        code = (
            "# NOTE: SparkSession is pre-initialized as 'spark' on Databricks\n"
            "# " + code.replace("\n", "\n# ")
            if "spark = " in code or "spark=" in code
            else code
        )

    return code


def detect_format(filepath: str) -> str:
    """Detect notebook format from file extension and content."""
    ext = Path(filepath).suffix.lower()

    if ext == ".ipynb":
        return "jupyter"
    elif ext in (".json", ".zpln"):
        # Check if it's a Zeppelin notebook
        with open(filepath) as f:
            try:
                data = json.load(f)
                if "paragraphs" in data:
                    return "zeppelin"
                elif "cells" in data:
                    return "jupyter"
            except json.JSONDecodeError:
                pass
    return "unknown"


def convert_notebook(input_path: str, output_path: str | None = None) -> str:
    """Convert a notebook file to Databricks format."""
    fmt = detect_format(input_path)

    if fmt == "zeppelin":
        result = convert_zeppelin_notebook(input_path)
    elif fmt == "jupyter":
        result = convert_jupyter_notebook(input_path)
    else:
        raise ValueError(f"Unknown notebook format for: {input_path}")

    if output_path:
        with open(output_path, "w") as f:
            f.write(result)
        print(f"Converted: {input_path} → {output_path}", file=sys.stderr)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Convert Zeppelin/Jupyter notebooks to Databricks format"
    )
    parser.add_argument("--input", help="Input notebook file (.json/.zpln/.ipynb)")
    parser.add_argument("--output", default="-", help="Output file (- for stdout)")
    parser.add_argument("--input-dir", help="Directory of notebooks to convert")
    parser.add_argument("--output-dir", help="Output directory for converted notebooks")
    args = parser.parse_args()

    if args.input:
        result = convert_notebook(args.input)
        if args.output == "-":
            print(result)
        else:
            with open(args.output, "w") as f:
                f.write(result)
            print(f"Converted to {args.output}", file=sys.stderr)

    elif args.input_dir and args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
        input_dir = Path(args.input_dir)
        converted = 0
        errors = 0

        for filepath in sorted(input_dir.glob("**/*")):
            if filepath.suffix.lower() in (".json", ".zpln", ".ipynb"):
                rel_path = filepath.relative_to(input_dir)
                output_path = Path(args.output_dir) / rel_path.with_suffix(".py")
                output_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    convert_notebook(str(filepath), str(output_path))
                    converted += 1
                except (ValueError, json.JSONDecodeError) as e:
                    print(f"  Skipped {filepath}: {e}", file=sys.stderr)
                    errors += 1

        print(
            f"\nDone: {converted} converted, {errors} skipped",
            file=sys.stderr,
        )
    else:
        parser.error("Specify --input or both --input-dir and --output-dir")


if __name__ == "__main__":
    main()

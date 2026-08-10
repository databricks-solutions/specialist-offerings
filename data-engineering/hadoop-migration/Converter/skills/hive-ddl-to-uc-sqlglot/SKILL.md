---
name: hive-ddl-to-uc-sqlglot
description: "Convert Hive DDL to Unity Catalog DDL using sqlglot parser. Triggers on: convert Hive DDL sqlglot, hive-ddl-sqlglot, sqlglot convert"
version: 1.0.0
---

# Hive DDL to Unity Catalog Converter (sqlglot-powered)

Hybrid skill: uses sqlglot for deterministic SQL parsing/generation, with LLM intelligence for interactive config, review, and commentary.

## Prerequisites

- Python 3.8+ with sqlglot installed (`pip install sqlglot`)

## When to Use

- Converting `CREATE TABLE` / `CREATE EXTERNAL TABLE` from Hive to Unity Catalog
- Migrating database/schema definitions
- Converting SerDe-based table definitions to Delta/Iceberg
- Remapping 2-level namespaces to 3-level (catalog.schema.table)
- When deterministic, script-driven conversion is preferred over pure LLM

## Instructions

When given Hive DDL to convert:

0. **Detect parameters** from invocation args
   - Read `references/PARAMETERS.md` for the full parameter specification
   - Parse the user's invocation args for explicit values. Look for these patterns:
     - **catalog**: `catalog <name>` → set target catalog
     - **schema**: `schema <name>` → set target schema
     - **format**: `format delta`, `format iceberg`, `use Iceberg`, `use Delta` → set table format
     - **naming**: `suffix <str>` or `prefix <str>` → set table naming convention
     - **clustering**: `clustering preserve`, `keep partitions` → preserve; otherwise default to `liquid`
   - Track which of the 5 parameters are already specified vs still need defaults/prompting
   - Apply defaults for any unspecified parameters: catalog=`main`, schema=auto-derive, format=`DELTA`, naming=no change, clustering=`liquid`

1. **Prompt for missing configuration** (skip entirely if all 5 parameters were provided in args)
   - Use `AskUserQuestion` to collect only the parameters NOT already specified in args
   - Combine catalog and schema into one question; ask up to 4 questions max:

     **Q1 — Target catalog & schema** (skip if both catalog and schema were provided):
     - header: "Namespace"
     - question: "Which Unity Catalog namespace should the tables be created in?"
     - options:
       - `main (default catalog, auto-derive schema)` — Uses `main` catalog; schema derived from Hive DB name (Recommended)
       - `Custom` — Specify custom catalog and/or schema names

     **Q2 — Table format** (skip if format was provided):
     - header: "Format"
     - question: "Which table format should be used?"
     - options:
       - `Delta (Recommended)` — Standard Databricks format with full feature support
       - `Iceberg` — UniForm Iceberg tables; adds required TBLPROPERTIES for clustering compatibility

     **Q3 — Table naming** (skip if suffix or prefix was provided):
     - header: "Naming"
     - question: "How should the converted tables be named?"
     - options:
       - `Keep original names (Recommended)` — Table names remain unchanged
       - `Add suffix (e.g. _ice, _uc)` — Append a suffix to each table name
       - `Add prefix (e.g. uc_, delta_)` — Prepend a prefix to each table name

     **Q4 — Clustering strategy** (skip if clustering was provided):
     - header: "Clustering"
     - question: "How should partitioned tables be handled?"
     - options:
       - `Convert to Liquid Clustering (Recommended)` — Converts PARTITIONED BY to CLUSTER BY
       - `Preserve as PARTITIONED BY` — Keeps original Hive-style partitioning

   - If user selects "Custom" for namespace, or "Add suffix"/"Add prefix" for naming, follow up to collect the specific values
   - Merge prompted values with arg-detected values to form the final configuration

2. **Parse input DDL** using sqlglot
   - Save the user's Hive DDL to a temporary `.hql` file if provided inline
   - Run: `python {skill_dir}/scripts/parse_hive_ddl.py <input_file>`
   - The script outputs structured JSON to stdout with extracted metadata for each statement
   - Review the JSON output for:
     - `COMMAND` types (sqlglot fallback for unsupported syntax — the raw SQL is preserved)
     - `PARSE_ERROR` types (statements that failed to parse)
   - Note any issues for manual handling in step 4

3. **Generate UC DDL** using the parsed data + user config
   - Pipe the JSON from step 2 into the generator:
     ```
     python {skill_dir}/scripts/parse_hive_ddl.py <input_file> | \
     python {skill_dir}/scripts/generate_uc_ddl.py \
       --catalog <catalog> \
       --schema <schema> \
       --format <DELTA|ICEBERG> \
       --clustering <liquid|preserve> \
       [--suffix <suffix>] \
       [--prefix <prefix>]
     ```
   - The generator applies all deterministic transformations:
     - Namespace mapping (2-level → 3-level)
     - Storage format conversion (STORED AS → USING)
     - SerDe removal
     - Hive TBLPROPERTIES cleanup + UC TBLPROPERTIES addition
     - Partition/bucket → Liquid Clustering conversion
     - Iceberg + Liquid Clustering DV/RT disabled properties
     - View table reference rewriting (via sqlglot)
     - CREATE DATABASE → CREATE SCHEMA
     - USE → USE CATALOG + USE SCHEMA
     - ALTER TABLE ADD PARTITION → skip (with Liquid Clustering)
     - CREATE INDEX → skip (deprecated)

4. **Review and enhance** the generated SQL
   - Read the output SQL from step 3
   - Check for `-- MANUAL REVIEW:` and `-- WARNING:` markers
   - Apply LLM judgment for edge cases:
     - Custom SerDe classes that may need special ingestion logic
     - HDFS locations that need cloud storage mapping
     - Ambiguous TBLPROPERTIES that may need to be preserved
   - Add or refine inline comments where the script output needs more context
   - For `COMMAND` type fallbacks, manually convert the raw SQL

5. **Output** the final DDL to the user (or write to file if requested)

6. **Flag** constructs needing manual review with a summary section at the end

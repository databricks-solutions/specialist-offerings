---
name: hive-ddl-to-uc
description: "Convert Hive DDL statements to Unity Catalog DDL. Triggers on: convert Hive DDL, migrate Hive tables, Hive to Unity Catalog, CREATE TABLE to UC, convert Hive schema"
version: 1.1.0
---

# Hive DDL to Unity Catalog Converter

Convert Hive Data Definition Language (DDL) statements to Databricks Unity Catalog compatible DDL.

## When to Use

- Converting `CREATE TABLE` / `CREATE EXTERNAL TABLE` statements from Hive to Unity Catalog
- Migrating database/schema definitions
- Converting SerDe-based table definitions to Delta Lake format
- Remapping 2-level namespaces (database.table) to 3-level (catalog.schema.table)

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

2. **Read references** for detailed rules:
   - `references/NAMESPACE_MAPPING.md` — 2-level to 3-level namespace conversion
   - `references/DDL_RULES.md` — DDL syntax transformation rules
   - `references/SERDE_MIGRATION.md` — SerDe to format mapping
   - `references/EXAMPLES.md` — Before/after examples

3. **Apply transformations** in this order, using the resolved configuration from steps 0-1:
   a. Map namespace: `database.table` → `{catalog}.{schema}.{prefix}{table}{suffix}`
      - Use the configured catalog (default: `main`)
      - Use the configured schema, or auto-derive from the Hive database name
      - Apply prefix/suffix to table name if configured (default: no change)
   b. Convert `STORED AS <format>` → `USING {format}` (use configured format: `DELTA` or `ICEBERG`)
   c. Remove/convert `LOCATION` clauses (managed tables don't need LOCATION in UC)
   d. Convert SerDe definitions to format options
   e. Remove Hive-specific properties (`TBLPROPERTIES` that don't apply)
   f. Handle partitioning based on clustering config:
      - If clustering = `liquid`: convert `PARTITIONED BY` → `CLUSTER BY` (Liquid Clustering)
      - If clustering = `preserve`: keep `PARTITIONED BY` as-is
   g. Add `COMMENT` and `TBLPROPERTIES` for lineage tracking
   h. **Iceberg + Liquid Clustering**: When format = `ICEBERG` AND clustering = `liquid`,
      any table with `CLUSTER BY` must include these in TBLPROPERTIES:
      `'delta.enableDeletionVectors' = false` and `'delta.enableRowTracking' = false`.
      This is required because Iceberg v2 spec does not support deletion vectors or
      row tracking, which are normally required for Liquid Clustering concurrency control.
      (Skip this step if format = `DELTA` or clustering = `preserve`)

4. **Output** the converted DDL with inline comments explaining each change

5. **Flag** any constructs that need manual review (custom SerDes, UDTFs, etc.)

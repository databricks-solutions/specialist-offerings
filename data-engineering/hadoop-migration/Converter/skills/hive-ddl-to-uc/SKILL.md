---
name: hive-ddl-to-uc
description: "Convert Hive DDL statements to Unity Catalog DDL. Triggers on: convert Hive DDL, migrate Hive tables, Hive to Unity Catalog, CREATE TABLE to UC, convert Hive schema"
version: 1.0.0
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

1. **Read references** for detailed rules:
   - `references/NAMESPACE_MAPPING.md` — 2-level to 3-level namespace conversion
   - `references/DDL_RULES.md` — DDL syntax transformation rules
   - `references/SERDE_MIGRATION.md` — SerDe to format mapping
   - `references/EXAMPLES.md` — Before/after examples

2. **Apply transformations** in this order:
   a. Map namespace: `database.table` → `catalog.schema.table`
   b. Convert `STORED AS <format>` → `USING DELTA` (or appropriate format)
   c. Remove/convert `LOCATION` clauses (managed tables don't need LOCATION in UC)
   d. Convert SerDe definitions to format options
   e. Remove Hive-specific properties (`TBLPROPERTIES` that don't apply)
   f. Convert partitioning syntax if needed
   g. Add `COMMENT` and `TBLPROPERTIES` for lineage tracking

3. **Output** the converted DDL with inline comments explaining each change

4. **Flag** any constructs that need manual review (custom SerDes, UDTFs, etc.)

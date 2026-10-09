---
name: hbase-to-databricks
description: "Convert HBase tables and applications to Databricks Lakebase (managed Postgres). Triggers on: convert HBase, migrate HBase, HBase to Databricks, HBase to Lakebase, HBase to Postgres, HBase API migration"
version: 2.0.0
---

# HBase to Lakebase Converter

Convert Apache HBase table designs and application code to Databricks Lakebase (managed PostgreSQL) — the natural replacement for HBase's low-latency key-value lookup pattern.

## Why Lakebase

HBase is primarily used for low-latency key-value lookups. Lakebase (Databricks-managed PostgreSQL) is the best replacement because it provides:

- Sub-millisecond point lookups by primary key (matching HBase Get latency)
- Full SQL support (no custom Java API)
- ACID transactions natively
- Standard PostgreSQL wire protocol (any Postgres client works)
- Managed by Databricks — no ZooKeeper, no RegionServer tuning

## When to Use

- Migrating HBase tables used for key-value lookups to Lakebase
- Converting HBase Java API calls (Get/Put/Scan/Delete) to SQL
- Mapping HBase row keys to Postgres primary keys
- Flattening column families into Postgres table columns
- Converting HBase shell commands to SQL DDL/DML

## Instructions

When given HBase schemas or code to convert:

1. **Read references** for detailed rules:
   - `references/TABLE_DESIGN.md` — Row key → primary key, column families → columns
   - `references/API_MIGRATION.md` — HBase Java API → SQL / psycopg2 / JDBC
   - `references/EXAMPLES.md` — Full before/after examples

2. **Apply transformations**:
   a. Map HBase row key → Postgres `PRIMARY KEY` column(s)
   b. Split composite row keys into individual columns forming a composite PK
   c. Flatten column families into explicit Postgres columns
   d. Drop salt prefixes (Postgres handles distribution automatically)
   e. Drop reversed timestamps (use `ORDER BY ... DESC` at query time)
   f. Convert HBase Put/Get/Scan/Delete → INSERT/SELECT/UPDATE/DELETE SQL
   g. Map HBase filters → SQL WHERE clauses
   h. Convert batch operations to bulk INSERT/UPSERT patterns

3. **Output** converted table DDL + access code + migration notes

4. **Flag** patterns needing special attention:
   - TTL → scheduled cleanup or Postgres row-level TTL extension
   - Versions → SCD Type 2 or audit table pattern
   - Coprocessors → Postgres triggers or application-layer logic
   - Wide/dynamic columns → JSONB column or key-value table

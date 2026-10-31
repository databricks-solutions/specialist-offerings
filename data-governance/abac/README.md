---
title: "ABAC with Governed Tags"
owner: "akshay.amin@databricks.com"
region: "Global"
team: ""
status: "Active"
business_outcome: "Enforce row- and column-level access uniformly across native and federated tables with a single tag-driven Unity Catalog ABAC policy model"
tags: [ssa-offering, global, data-governance, unity-catalog, abac, governed-tags, row-filters, column-masks, lakehouse-federation, aws-glue]
strategic_priority: "High"
imperative: "Data Governance"
last_certified: "2026-10-30"
products: "Unity Catalog, Lakehouse Federation"
product_lines: "Data Governance"
---

## ABAC with Governed Tags

> **SSA offering — how to engage:** If you are interested in this SSA Offering, follow the ASQ process (go/gethelp): Product Help > Pilot/Production Advisory; then put the tag #ssa-offering #abac in both the **Title** and **Description** fields.

**GOAL:** Show a customer that one set of governed tags plus a handful of Unity Catalog ABAC policies can replace per-table grants, row filters, and column masks, and that the same policies apply unchanged to tables federated from AWS Glue.

### Overview

This is a runnable proof of concept for **attribute-based access control (ABAC)** in Unity Catalog. It models a bank with two lines of business, each owning a schema of tables. Access is decided entirely by tags:

- **Table-level tags** (`abac_sublob`, `abac_data_use_category`) decide whether a group can see a table at all. Tables that fail the condition return zero rows.
- **Column-level tags** (`abac_protection_group`, `abac_sensitive`, `abac_pci`) decide which columns a group sees in clear text. Everything else is masked.
- **Governed tags** restrict the allowed values for each key and require `ASSIGN` permission, so data owners cannot tag their way around policy.

Policies are created once at the catalog level and apply to every current and future table whose tags match. The final notebook applies the identical policy text to a Glue-federated foreign catalog to prove the model works across native and federated data.

A customer walks away with a working reference they can retag to their own classification scheme, plus the exact `CREATE POLICY` syntax for row filters and column masks driven by `hasTagValue`.

### What is in this folder

| Notebook | What it does |
|---|---|
| `01_setup.py` | Creates the five governed tags through the Tag Policies REST API, creates three tables in `af_schema` and three in `bb_schema`, applies table and column tags, loads sample rows, and verifies via `information_schema.table_tags` and `column_tags`. |
| `02_functions.py` | Creates the SQL UDFs the policies reference: a `filter_all` row filter that hides every row, and `mask_col` masking functions, including a VARIANT-typed variant that returns a type-appropriate masked value. |
| `03_abac_policies.py` | Creates six catalog-level ABAC policies, one row-filter and one column-mask policy for each of three groups, with test queries between them so you can switch group membership and watch results change. |
| `04_abac_glue_fed.py` | Applies the same row-filter and column-mask policies to a Glue-federated foreign catalog, and contrasts with a classic `ALTER COLUMN ... SET MASK`. |

### The tag model

Table tags, one pair per table:

| Tag key | Allowed values | Purpose |
|---|---|---|
| `abac_sublob` | `AF`, `BB`, `CB`, `CCB` | Which line of business owns the table |
| `abac_data_use_category` | `true`, `false` | Whether the table carries a usage restriction |

Column tags, one triple per column:

| Tag key | Allowed values | Purpose |
|---|---|---|
| `abac_protection_group` | `INTL`, `CNFD`, `HIGHPI`, `HICNFD`, `PIDRID`, `GOVTID`, `CNFDPI`, `RISKSC` | Protection classification |
| `abac_sensitive` | `true`, `false` | Column holds sensitive data |
| `abac_pci` | `true`, `false` | Column holds PCI-regulated data |

### Entitlements demonstrated

| Group | Tables visible | Columns in clear text |
|---|---|---|
| `abac_group_af1` | `abac_sublob` in AF, CB, CCB and `abac_data_use_category` = false | `abac_protection_group` = INTL, not sensitive, not PCI |
| `abac_group_af2` | Same as af1 | `abac_protection_group` in INTL or CNFD, not sensitive, not PCI |
| `abac_group_bb1` | `abac_sublob` in CB, CCB, BB and `abac_data_use_category` = false | Any protection group, not sensitive, not PCI |

Every other table returns no rows. Every other column is masked.

### Prerequisites

- Databricks Runtime 16.1 or later, or a serverless SQL warehouse.
- Account admin or metastore admin to create governed tags. Tag creation uses the `POST /api/2.1/tag-policies` endpoint, which has no SQL equivalent.
- A catalog named `aa_catalog` containing empty schemas `af_schema` and `bb_schema`. The notebooks do not create these.
- `CREATE TABLE`, `APPLY TAG`, and `ASSIGN` on the governed tags, plus `MANAGE` on the catalog to create policies.
- Three account-level groups named `abac_group_af1`, `abac_group_af2`, and `abac_group_bb1`. The demo works by adding and removing your test user from these groups.
- For `04_abac_glue_fed.py` only: a Lakehouse Federation connection to AWS Glue surfaced as foreign catalog `aa_fed_glue`, with tables already tagged using the same keys.

The only Python dependency is `requests`, which ships with Databricks Runtime.

### How to run

1. Import the four notebooks into a workspace folder.
2. Search and replace `aa_catalog` if you want a different catalog. The same name appears in all four notebooks.
3. Run `01_setup.py` end to end. Confirm the two `information_schema` queries at the end show tags on every table and column.
4. Run `02_functions.py`.
5. Run `03_abac_policies.py` cell by cell. Each markdown heading tells you which group to add your test user to before running the next `CREATE POLICY` and the `SELECT` queries that follow. Expect `af_schema` tables to return rows for `af1` and `af2` and nothing for `bb1`, and expect masked values to differ between `af1` and `af2`.
6. Optionally run `04_abac_glue_fed.py` against the Glue foreign catalog and observe the same behaviour on federated tables.

### GTM Signals — When to Engage

- Customer manages hundreds of per-table grants, views, or masking functions and wants policy defined once by classification.
- Customer has a data classification taxonomy in a catalog tool such as Collibra or Alation and wants Unity Catalog to enforce it.
- Customer federates data from AWS Glue, Snowflake, or another source and asks whether governance applies there too.
- Regulated environment with PII, PCI, or restricted data and auditors asking how access is decided.

### Technical Readiness Criteria

- Unity Catalog enabled with a metastore admin available for the session.
- A classification scheme, even a draft one, that can be expressed as a small set of tag keys and allowed values.
- Account groups that map to the business roles the policies will target.

### Customer Requirements

- Metastore admin or account admin for governed tag creation.
- Ability to create and modify account groups for the test users.
- Optional: an existing Lakehouse Federation connection if the federated scenario is in scope.

### Known gaps and limitations

- Names are hard-coded. Catalog, schema, group, and foreign catalog names live inline rather than in widgets.
- There is no teardown notebook. Dropping the policies, functions, tables, and governed tags is manual.
- ABAC column masks cannot be applied to `ARRAY<STRUCT<...>>` columns through a VARIANT-typed mask function because the implicit cast is unsupported. Store such columns as VARIANT if you need to mask them.
- Governed tag creation has no SQL DDL today. If the Tag Policies API changes, update the first cell of `01_setup.py`.

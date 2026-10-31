---
title: "Lake Formation TBAC to Unity Catalog ABAC Migration"
owner: "akshay.amin@databricks.com"
region: "Global"
team: ""
status: "Active"
business_outcome: "Translate AWS Lake Formation tag-based access control rules into Unity Catalog ABAC policies so governance moves to Databricks without rewriting entitlements table by table"
tags: [ssa-offering, global, data-governance, unity-catalog, abac, governed-tags, lake-formation, tbac, migration, row-filters, column-masks, lakehouse-federation, aws-glue]
strategic_priority: "High"
imperative: "Data Governance"
last_certified: "2026-10-30"
products: "Unity Catalog, Lakehouse Federation"
product_lines: "Data Governance"
---

## Lake Formation TBAC to Unity Catalog ABAC Migration

> **SSA offering — how to engage:** If you are interested in this SSA Offering, follow the ASQ process (go/gethelp): Product Help > Pilot/Production Advisory; then put the tag #ssa-offering #abac in both the **Title** and **Description** fields.

**GOAL:** Take a customer's existing AWS Lake Formation tag-based access control (LF-TBAC) entitlement matrix and reproduce it in Unity Catalog with governed tags and ABAC policies, proving parity on native Delta tables and on the same Glue tables accessed through Lakehouse Federation.

### Overview

Customers running Lake Formation on AWS commonly govern their lake with **LF-Tags**. Tags are stamped on databases, tables, and columns, and access is granted to a principal as a **tag expression** rather than as a list of tables. A large bank might organise its lake into **dataspaces**, each a collection of schemas and tables governed as one unit, where tables can be very wide and only a subset of columns is visible to a given dataspace.

When that customer adopts Unity Catalog, the question is not "can UC do row filters and column masks" but "can we carry our tag-driven entitlement model over without re-expressing every rule per table". This offering answers yes and shows how.

Unity Catalog's ABAC gives you the same building blocks: **governed tags** with controlled allowed values, and **catalog-level policies** that use `hasTagValue()` to decide which tables a group can see and which columns are returned in clear text. The notebooks in this folder take a representative LF-TBAC matrix, implement it as UC ABAC policies, and verify the result from each group's point of view.

The customer walks away with a working mapping from their LF concepts to UC concepts, the exact `CREATE POLICY` syntax for both table visibility and column masking, and a repeatable recipe for translating each remaining row of their own matrix.

### The source requirement: a Lake Formation entitlement matrix

This is the LF-TBAC model the notebooks reproduce. Group names below are the sanitised ones used in the code.

| Dataspace | Sub-dataspace | Group | Sub-LOB (table tag) | Data Use Category (table tag) | Protection Group (column tag) | Sensitive (column tag) | PCI (column tag) |
|---|---|---|---|---|---|---|---|
| AF | AF01 | `abac_group_af1` | AF, CB, CCB | false | INTL | false | false |
| AF | AF02 | `abac_group_af2` | AF, CB, CCB | false | INTL, CNFD | false | false |
| BB | BB01 | `abac_group_bb1` | CB, CCB, BB | false | INTL, CNFD, HICNFD, HIGHPI, PIDRID, CNFDPI, GOVTID, RISKSC | false | false |
| BB | BB02 | `abac_group_bb2` | CB, CCB, BB | false | INTL, CNFD | AGE, false | false |

Read each row as a Lake Formation grant. The group may access any **table** whose Sub-LOB tag is in the listed set and whose Data Use Category is `false`. Within those tables, the group may read any **column** whose Protection Group tag is in the listed set and whose Sensitive and PCI tags match the listed values. Lake Formation evaluates this because LF-Tags can be granted directly to users and groups.

### Concept mapping

| Lake Formation | Unity Catalog | Notes |
|---|---|---|
| LF-Tag key with allowed values | Governed tag with allowed values | Created via the Tag Policies REST API. Values are enforced on assignment, and assigning requires `ASSIGN` permission. |
| Tag on a database or table | Table-level tag (`ALTER TABLE ... SET TAGS`) | Table tags drive table visibility. |
| Tag on a column | Column-level tag (`ALTER TABLE ... ALTER COLUMN ... SET TAGS`) | Column tags drive masking. |
| Tag expression granted to a principal | ABAC policy `TO <group>` with a `WHEN` and `MATCH COLUMNS` condition | UC puts the entitlement in the policy rather than on the principal. |
| Table-level grant via tag expression | Row filter policy `FOR TABLES WHEN NOT(<table tag condition>)` | Tables outside the entitlement return zero rows instead of raising an error. |
| Column-level grant via tag expression | Column mask policy `MATCH COLUMNS NOT(<column tag condition>)` | Columns outside the entitlement are returned masked rather than removed. |
| Grant scope | `ON CATALOG` | One policy covers every current and future table in the catalog. Schema scope is also available. |
| Glue table in the lake | Foreign table via Lakehouse Federation | The same policy text applies to a federated Glue catalog. |

The one semantic difference worth setting expectations on: Lake Formation hides tables and columns the principal is not entitled to. UC ABAC, as implemented here, returns the table with no rows and returns the column with a masked value. Queries succeed and schemas stay stable, which is usually what downstream tools want, but auditors should know the difference.

### Approach 1: one policy pair per dataspace group

Several translation approaches are possible. The notebooks implement the most direct one. Each row of the matrix becomes exactly two UC ABAC policies targeting that row's group.

**Table visibility** is a row-filter policy. The filter function always returns `FALSE`, and the policy applies it only to tables that fail the row's table-tag condition.

```sql
CREATE OR REPLACE POLICY af_1_table_access_policy
ON CATALOG aa_catalog
ROW FILTER aa_catalog.af_schema.filter_all
TO abac_group_af1
FOR TABLES
WHEN NOT((hasTagValue("abac_sublob","AF")
       OR hasTagValue("abac_sublob","CB")
       OR hasTagValue("abac_sublob","CCB"))
      AND hasTagValue("abac_data_use_category","false"));
```

**Column visibility** is a column-mask policy. It applies to tables that pass the table-tag condition, and masks every column that fails the row's column-tag condition.

```sql
CREATE OR REPLACE POLICY af_1_col_access_policy
ON CATALOG aa_catalog
COLUMN MASK aa_catalog.af_schema.mask_col
TO abac_group_af1
FOR TABLES
WHEN ((hasTagValue("abac_sublob","AF")
    OR hasTagValue("abac_sublob","CB")
    OR hasTagValue("abac_sublob","CCB"))
   AND hasTagValue("abac_data_use_category","false"))
MATCH COLUMNS
  NOT(hasTagValue("abac_protection_group","INTL")
  AND hasTagValue("abac_sensitive","false")
  AND hasTagValue("abac_pci","false")) AS sec
ON COLUMN sec;
```

Translating another row means copying the pair, changing the group, and editing the two tag conditions. No table names appear anywhere.

### What is in this folder

| Notebook | What it does |
|---|---|
| `01_setup.py` | Creates the five governed tags that mirror the LF-Tag keys and allowed values, creates three tables in `af_schema` and three in `bb_schema`, stamps table and column tags, loads sample rows, and verifies via `information_schema.table_tags` and `column_tags`. |
| `02_functions.py` | Creates the SQL UDFs the policies reference: `filter_all` for table visibility and `mask_col` for masking, including a VARIANT-typed version that returns a type-appropriate masked value so one function serves every column type. |
| `03_abac_policies.py` | Implements Approach 1 for the AF01, AF02, and BB01 rows: six catalog-level policies with test queries between them. Markdown cells tell you when to move the test user between groups. |
| `04_abac_glue_fed.py` | Applies the AF01 policy pair to a Glue-federated foreign catalog, demonstrating that tables still living in Glue inherit the same ABAC rules, and contrasts with a classic `ALTER COLUMN ... SET MASK`. |

### Sample data and expected results

The six sample tables are tagged so that every group sees a different slice.

| Table | `abac_sublob` | `abac_data_use_category` |
|---|---|---|
| `af_schema.accounts` | AF | false |
| `af_schema.transactions` | CB | true |
| `af_schema.customers` | CCB | false |
| `bb_schema.commercial_accounts` | BB | true |
| `bb_schema.corporate_transactions` | CB | false |
| `bb_schema.corporate_clients` | CCB | true |

Running `03_abac_policies.py` as a member of each group should show:

| Group | Tables returning rows | Columns in clear text |
|---|---|---|
| `abac_group_af1` | `accounts`, `customers`, `corporate_transactions` | Only INTL columns that are not sensitive and not PCI |
| `abac_group_af2` | Same three tables | INTL and CNFD columns that are not sensitive and not PCI |
| `abac_group_bb1` | Same three tables | Any protection group, as long as not sensitive and not PCI |

The `transactions`, `commercial_accounts`, and `corporate_clients` tables return no rows for every group because their Data Use Category is `true`. Columns tagged HIGHPI, HICNFD, PIDRID, GOVTID, CNFDPI, or RISKSC are masked for `af1` and `af2` but visible to `bb1`. Any column tagged sensitive or PCI is masked for everyone.

### Prerequisites

Derived from what the notebooks actually do.

- Databricks Runtime 16.1 or later, or a serverless SQL warehouse, in a Unity Catalog enabled workspace.
- Account admin or metastore admin, because governed tags are created through `POST /api/2.1/tag-policies` and there is no SQL equivalent.
- A catalog named `aa_catalog` with two empty schemas, `af_schema` and `bb_schema`. The notebooks reference these but do not create them.
- `CREATE TABLE` and `MODIFY` on the schemas, `APPLY TAG` on the tables, `ASSIGN` on each governed tag, and `MANAGE` on the catalog to create policies.
- Three account-level groups named `abac_group_af1`, `abac_group_af2`, and `abac_group_bb1`, and a test user you can move between them.
- For `04_abac_glue_fed.py` only: a Lakehouse Federation connection to AWS Glue surfaced as foreign catalog `aa_fed_glue`, whose tables and columns already carry the same five tag keys.

The only Python dependency is `requests`, which ships with Databricks Runtime.

### How to run

1. Import the four notebooks into a workspace folder.
2. If you want a different catalog, search and replace `aa_catalog` across all four notebooks.
3. Run `01_setup.py` top to bottom. The two `information_schema` queries at the end should list tags on every table and column.
4. Run `02_functions.py`.
5. Run `03_abac_policies.py` cell by cell. Before each policy pair, add your test user to the group named in the markdown heading and remove it from the previous one. Compare the `SELECT` results against the expected results table above.
6. Optionally run `04_abac_glue_fed.py` against the Glue foreign catalog and confirm the same behaviour on federated tables.

### Translating a customer's own matrix

1. Export the LF-Tag keys and their allowed values. Each becomes one governed tag in the first cell of `01_setup.py`.
2. Export the LF-Tag assignments on tables and columns. These become `SET TAGS` statements, or an automated tagging job if the estate is large.
3. For each LF grant row, create one row-filter policy and one column-mask policy following the Approach 1 pattern, targeting the matching UC group.
4. Decide on masking functions per column type. The VARIANT-typed `mask_col` in `02_functions.py` handles strings, numerics, booleans, dates, and timestamps with one function.
5. Verify from each group's point of view, as `03_abac_policies.py` does, before removing Lake Formation grants.

### GTM Signals — When to Engage

- Customer governs an S3 lake with Lake Formation LF-Tags and is evaluating or adopting Unity Catalog.
- Customer wants Glue tables and Databricks tables under one access model during a phased migration.
- Customer has a dataspace, domain, or data-product model where entitlements are expressed by classification rather than by table.
- Security or audit team asks how the LF entitlement matrix will be preserved after the move.

### Technical Readiness Criteria

- Unity Catalog enabled with a metastore admin available for the session.
- An exportable list of LF-Tag keys, allowed values, and current grants.
- Account groups that correspond to the LF principals.
- Optional: a Lakehouse Federation connection to the Glue Data Catalog.

### Customer Requirements

- Metastore admin or account admin for governed tag creation.
- Ability to create account groups and move test users between them.
- Read access to the Lake Formation console or CLI to export the current tag model.

### Known gaps and limitations

- Only the AF01, AF02, and BB01 rows of the matrix are implemented. The BB02 row, including its `AGE` sensitivity value, is not yet covered.
- Names are hard-coded. Catalog, schema, group, and foreign catalog names live inline rather than in widgets.
- There is no teardown notebook. Dropping the policies, functions, tables, and governed tags is manual.
- Unlike Lake Formation, excluded tables return zero rows and excluded columns return masked values rather than disappearing.
- ABAC column masks cannot be applied to `ARRAY<STRUCT<...>>` columns through a VARIANT-typed mask function because the implicit cast is unsupported. Store such columns as VARIANT if they need masking.
- Governed tag creation has no SQL DDL today. If the Tag Policies API changes, update the first cell of `01_setup.py`.

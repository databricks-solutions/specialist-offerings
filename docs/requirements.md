# HDFS → UC Managed Iceberg Migration Utility — Requirements

**Status:** draft v0.2 · 2026-10-30 (v0.1: 2026-10-20; v0.2 adds ARC-08/09, DDL-15/16, INC-16/18, VAL-12/13, SEC-10, NFR-11, backlog #19–24)
**Scope:** on-prem Hadoop (HDFS + Hive Metastore; Hive tables and OSS Iceberg tables) → Databricks
Unity Catalog **managed Iceberg** (MI), one-time historical load + incremental sync until cutover.

This document is the input to the technical design, process flow, implementation plan and code
that live in this repository (`h2mi`). It consolidates (a) what we have already **tested** in our labs, (b)
customer statements and decisions from discovery and follow-up calls, and (c) new asks from the
most recent call. It supersedes nothing: the earlier requirement set
(lab evidence (Appendix C),
IDs `R-01`…`R-55`) is mapped into this one in [Appendix A](#appendix-a--traceability-to-legacy-r-ids).

---

## Conventions

**IDs** are stable and area-prefixed. Reference them instead of restating requirements.

| Prefix | Area | Prefix | Area |
|---|---|---|---|
| `SCP` | scope, constraints | `INC` | incremental sync |
| `ARC` | architecture baseline | `VAL` | validation |
| `SRC` | source formats & SerDes | `AUD` | audit & identity |
| `TYP` | data-type mapping | `SEC` | security & network |
| `DT` | Hive date/time types | `VER` | Spark / Iceberg versions |
| `DDL` | target design & registration | `OPS` | operations & reliability |
| `INI` | initial (historical) load | `NFR` | non-functional |
| `ADF` | distcp + `add_files` path | `Q` | open questions for the customer |

**Priority:** **M** = Must · **S** = Should · **C** = Could.

**Status:** ✅ tested in lab · 🟡 partially tested · 🔵 test needed · ❓ open with the customer · 🔮 roadmap-dependent ·
⚠️ risk.

**Evidence tags** (full list in [Appendix B](#appendix-b--references)): `[MA]` Migration Approaches doc ·
`[QA]` Follow-up Questions & Test Results · `[A13]` Aug-13 call follow-ups · `[FA]` Follow-up Answers ·
`[TM]` Hive → Databricks Type Mapping Reference · `Lab YYYY-MM-DD` = a hands-on lab test on that date.
Repo files are linked relatively.

**Terms:** *MI* = UC managed Iceberg · *IRC* = UC Iceberg REST Catalog
(`https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest`) · *SP* = Databricks service principal ·
*HMS* = Hive Metastore · *DX* = AWS Direct Connect · *PO* = Predictive Optimization ·
*CoW / MoR* = copy-on-write / merge-on-read.

---

## 0. Purpose, scope, non-goals

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| SCP-01 | The utility shall migrate on-prem **Hive tables** and **OSS Iceberg tables** into UC **managed Iceberg**. | M | ✅ confirmed | R-01, R-02 |
| SCP-02 | The utility shall support a **one-time historical load** followed by **incremental sync** until per-table cutover. | M | ✅ confirmed | R-03 |
| SCP-03 | Source and target shall be **eventually consistent**. Reconciliation is on **data**, never on snapshot or version IDs. | M | ✅ confirmed | R-04, [MA] §5 |
| SCP-04 | All targets shall be **managed Iceberg**, regardless of source format. | M | ✅ confirmed | R-02, R-20 |
| SCP-05 | Non-goals (below) are out of scope for this utility. | — | ✅ agreed 2026-10-20 | — |

**Non-goals** (each has existing material elsewhere):

| Non-goal | Where it is covered |
|---|---|
| Glue-registered Iceberg tables → UC | lab evidence (Appendix C) |
| Reverse / exit sync (MI → on-prem or → Glue; `rewrite_table_path`) | `[FA]` #5, exit runbook |
| Cutover runbook (repointing writers/consumers, retiring on-prem copy) | later artifact |
| Managed **Delta** targets | lab evidence (Appendix C) |
| Approach C — federation / foreign tables (on-prem HDFS not supported; roadmap) | `[MA]` §6 |
| AWS-native Iceberg estate (separate track) | R-05 |
| Migrating Hive **views**, UDF code, and downstream jobs (HiveQL → Spark SQL) | separate track |

---

## 1. Context & hard constraints

| ID | Constraint | Pri | Status | Evidence |
|---|---|---|---|---|
| SCP-10 | **No inbound connectivity** into the data centre. All traffic is initiated **outbound** from on-prem. | M | ✅ ⚠️ | R-35, [FA] #7 |
| SCP-11 | On-prem HDFS and HMS are **Kerberized**. Kerberos stays on-prem; nothing in the cloud authenticates to it. | M | ✅ ⚠️ | R-36 |
| SCP-12 | Estate is **PB scale (tens of PB)**. Bandwidth, not compute or format, sets the timeline. | M | ✅ | R-13, R-38 |
| SCP-13 | Scaling problem is **breadth**: many tables × many daily partitions. A typical candidate table grows by **tens of GB/day**. Commit and metadata overhead matter more than throughput. | M | ✅ | R-14, R-15, R-16 |
| SCP-14 | The **HMS transaction/event log is disabled** (space cost). HMS notifications cannot be used for change detection. | M | ✅ | [FA] #3 |
| SCP-15 | On-prem Iceberg is **format v2**, with both CoW and MoR tables. Hive tables are mostly append-only daily partitions. | M | ✅ | R-31, R-39, [MA] scope |
| SCP-16 | The current on-prem stack is older than required. A **newer-stack migration cluster** (Iceberg ≥ 1.11) is a prerequisite. | M | ✅ ⚠️ | [A13] prerequisite |
| SCP-17 | Snapshot IDs cannot be kept in sync across two catalogs. Version and snapshot counts legitimately diverge. | M | ✅ | [MA] §4.1, §5 |

---

## 2. Architecture baseline

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| ARC-01 | **Default path = Approach B.** On-prem OSS Spark reads the source (HMS / HDFS) and writes MI through the **IRC** with UC-vended credentials. No Databricks compute sits in the on-prem write path, and no inbound port is needed. | M | ✅ | [MA] §3, R-34, R-48 |
| ARC-02 | **Approach A** (distcp + PyIceberg `add_files`) is supported only for the eligible subset defined in §8. | S | ✅ narrow | [MA] §2, §8 |
| ARC-03 | The load engine shall be **pluggable**: **`onprem-push`** (on-prem Spark → IRC; the default for incremental and most initial loads) and **`cloud-landing`** (raw files bulk-copied to an S3 landing location, then a Databricks job writes MI; for PB-scale history). | M | 🟡 push ✅ / landing 🔵 | decision 2026-10-20 |
| ARC-04 | Both engines shall share one set of components: **inventory & pre-flight scanner**, **DDL generator/applier**, **planner**, **control tables**, **validator**, **audit recorder**. Engines differ only in where data is read and written. | M | — | — |
| ARC-05 | Every external write is a **UC catalog commit**: UC generates the Delta log server-side, and target versions and snapshot IDs are independent of the source. The design shall not assume ID parity. | M | ✅ | R-49, R-50, Lab 2026-09-14 |
| ARC-06 | **Single writer per table** until cutover: only the utility's SP writes migrated tables. | M | ✅ | [A13] resolved items |
| ARC-07 | Control state (control tables, §9/§10) shall be **durable and queryable from both sides**. Decided: **append-only OSS Iceberg event tables in the on-prem HMS** (`h2mi_ctl`), **mirrored to UC** (managed Iceberg) for DBSQL dashboards and the audit join. Single-writer leases are atomic HDFS files. | S | 🟡 IRC write ✅ · 🔵 HMS load | [technical_design.md](technical_design.md) D8, §7 |
| ARC-08 | **Upload only.** Data crosses the network in one direction. Any step that must read **target data** (MERGE, slice restates on clustered targets, target fingerprints) runs on **Databricks compute** through the Statement API; an on-prem MERGE over IRC is allowed only when the planner's metadata-based download estimate is under a configured budget, and the estimate is recorded per batch. | M | 🟡 push/replace ✅ · Statement-API MERGE 🔵 | design D11; the DX link is the migration's shared bottleneck |
| ARC-09 | **Writers use the DataFrameWriterV2 (`writeTo`) or SQL path only.** `DataFrameWriter.mode("overwrite").saveAsTable` is forbidden: Spark turns it into REPLACE TABLE AS SELECT, UC accepts it over IRC from a MODIFY-only SP, and it drops the target's partition spec and snapshot history. A unit test rejects the V1 API in the sync code. | M | ✅ failure observed | Lab 2026-10-30 (`overwrite_partitions_probe.py`), design D12 |

---

## 3. Source coverage — formats & SerDes

### 3.1 Format / SerDe matrix

"Read-through" = read with Spark via the Hive SerDe, keep the declared types, apply §4.
"Reparse" = read raw text and parse with native functions.

| Source format / SerDe | What Spark surfaces | Jar on classpath? | Utility handling | Status |
|---|---|---|---|---|
| **Parquet** (Hive) | declared types | bundled | read-through → §4 mapping → rewrite to MI | ✅ Lab 2026-09-03 (HMS → MI v2 and v3) |
| **ORC** (Hive) | declared types | bundled | read-through → rewrite to Parquet-backed MI | 🟡 ORC read ✅ (UNION test); full ORC → MI load 🔵 |
| **Avro** (Hive `AvroSerDe`) | schema from `avro.schema.literal` / `.url` | bundled | read-through; map logical types; `[null,T]` → nullable T; complex unions → §4 UNION rule | 🔵 |
| **TextFile / LazySimpleSerDe** (delimited / CSV) | **declared types honored** | bundled | read-through → §4 mapping | ✅ [TM] |
| **RegexSerDe** | **all STRING** (forced) | bundled | keep STRING by default; optional per-field typed reparse (`regexp_extract`) with sign-off | ✅ [TM] |
| **OpenCSVSerde** | **all STRING** (ignores DDL types) | bundled | keep STRING by default; optional typed reparse with sign-off | ✅ [TM] |
| **JSON SerDe** (hcatalog / openx) | declared types incl. nested | **not bundled** (`ClassNotFoundException`) | native reparse: `from_json` with the schema built from HMS | ✅ [TM] |
| **RCFile** (`ColumnarSerDe`, `LazyBinaryColumnarSerDe`) | declared types (same as Parquet) | bundled | read-through; same mapping as Parquet | ✅ Lab 2026-09-28 |
| **OSS Iceberg, Parquet data files** | Iceberg schema | Iceberg runtime | Iceberg read → MI | ✅ [MA] §3.3, tests `d_`, `f_` |
| **OSS Iceberg, ORC data files** (incl. **mixed** Parquet + ORC in one table) | Iceberg schema | Iceberg runtime | Iceberg read → Parquet-backed MI | 🔵 R-08a |
| SequenceFile / other | — | — | ❓ in scope? | ❓ Q-12 |
| Hive **ACID / transactional** tables | — | — | ❓ presence unknown; Spark has no native full-ACID Hive read | ❓ Q-11 |

### 3.2 Requirements

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| SRC-01 | **Inventory.** The utility shall scan HMS and HDFS per table and record: database/table, table type (managed / external / view / ACID), input/output format, SerDe class + properties, location, partition keys, partition count, file count, bytes, **file-size histogram**, last-modified; for Iceberg: format-version, `write.{delete,update,merge}.mode`, snapshot count, **presence of delete files**, partition spec history. | M | — | R-11, R-18 |
| SRC-02 | The utility shall handle every format in §3.1 as specified, and **refuse** (fail closed, with a report) any format/SerDe it does not recognize. | M | per row | §3.1 |
| SRC-03 | **Default = read-through, no added casts.** Preserve the SerDe's declared types. Casts beyond the declared type are per-field enrichment that needs explicit config + sign-off. A failed cast must never silently become NULL (see VER-08). | M | ✅ | [TM] "read through, don't cast" |
| SRC-04 | JSON SerDe tables shall be reparsed natively (`from_json`), with the schema derived from HMS, because the SerDe jar may be unavailable on the executing compute. | M | ✅ | [TM] |
| SRC-05 | All non-Parquet sources (Hive ORC/Avro/text/RCFile and Iceberg ORC data files) shall be **read and rewritten** to Parquet-backed MI. | M | ✅ | R-21, R-21a, R-27 |
| SRC-06 | Hive **partition columns** that exist only in the directory path shall be materialized as real columns in the target. | M | ✅ | [QA] Q9, [FA] #4 |
| SRC-07 | For **text tables**, the HMS schema may be stale or all-STRING. The scanner shall sample files and flag HMS-vs-file schema disagreements for review before DDL generation. | S | 🟡 | [TM] migration solution step 1 |
| SRC-08 | Iceberg sources with **partition evolution** shall be migrated by compute-copy (not `CONVERT`/register). | S | 🟡 (evolved source built; `CONVERT TO DELTA` fails as expected) | lab evidence (Appendix C) |
| SRC-09 | Iceberg sources with **ORC or mixed Parquet+ORC** data files shall be read through the Iceberg reader and written as Parquet. | M | 🔵 | R-08a, R-26a |
| SRC-10 | The scanner shall **detect Iceberg delete files** (position / equality) and record them, because they drive the sync mode (§9). | M | 🔵 | — |
| SRC-11 | Hive **views** shall be inventoried (definition captured) but not migrated as data. | C | — | non-goal |

---

## 4. Data-type mapping

### 4.1 Mapping matrix (non-date types; date/time types are in §5)

Verified against MI format-version 2 and 3 unless noted. Sources: [TM],
lab evidence (Appendix C),
lab evidence (Appendix C).

| Hive type | Spark reads as | MI target (DDL) | Class | Note | Status |
|---|---|---|---|---|---|
| `TINYINT` | `byte` | **`INT`** | AUTO_MAP | MI **rejects** byte at CREATE (IcebergWriterCompat V1 and V3). OSS Iceberg silently upcasts, so never rely on engine widening. | ✅ Lab 2026-09-09 |
| `SMALLINT` | `short` | **`INT`** | AUTO_MAP | same as TINYINT | ✅ |
| `INT`, `BIGINT` | int, long | `INT`, `BIGINT` (Iceberg `long`) | PASS | | ✅ |
| `FLOAT`, `DOUBLE` | float, double | same | PASS | | ✅ |
| `DECIMAL(p,s)` | decimal(p,s) | `DECIMAL(p,s)` | PASS / AUTO_MAP | **Unqualified Hive `DECIMAL` = (10,0)**, so always pin p,s; p ≤ 38 | ✅ |
| `STRING` | string | `STRING` | PASS | | ✅ |
| `VARCHAR(n)` | string | `STRING` | AUTO_MAP | no padding; external readers see `string` either way | ✅ |
| `CHAR(n)` | string, **padded to n** (OSS Spark, `readSideCharPadding=true`) | `STRING` | AUTO_MAP + policy | ⚠️ value semantics: see TYP-05 | ✅ Lab 2026-09-28 |
| `BOOLEAN`, `BINARY` | same | same | PASS | | ✅ |
| `ARRAY<T>` / `MAP<K,V>` | array / map | same, element types mapped recursively | PASS | Iceberg spec calls array `list` | ✅ |
| `STRUCT<…>` | struct | `STRUCT<…>`, **all fields nullable** | PASS | required nested fields are not supported | ✅ |
| `VOID` / null-type | void | **`STRING`** (cast) | AUTO_MAP | MI rejects void. The cast is lossless (column holds only NULLs). | ✅ |
| `UNIONTYPE<…>` | ❌ **unreadable** (`CANNOT_RECOGNIZE_HIVE_TYPE`) | `STRUCT<tag, field0, …>` | REDESIGN | No SQL bridge. Needs a Hive GenericUDF union → struct, **materialized** (CTAS), or a producer fix. Lives only in ORC/Avro. | ✅ Lab 2026-09-24 |
| any unrecognized type | — | — | BLOCK | fail closed | — |

### 4.2 Requirements

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| TYP-01 | **Pre-flight scanner.** Before any DDL or data movement, classify every column of every table as **PASS / AUTO_MAP / REDESIGN / BLOCK** and emit a per-table report. Tables with REDESIGN/BLOCK columns are not loaded until resolved. | M | — | — |
| TYP-02 | Apply the §4.1 and §5 matrices in the generated DDL **and** in the read projection (explicit casts where the class is AUTO_MAP). | M | ✅ | [TM] |
| TYP-03 | `TINYINT`/`SMALLINT` shall be declared `INT` in the target DDL. | M | ✅ | Lab 2026-09-09 |
| TYP-04 | `DECIMAL` precision/scale shall always be explicit. Unqualified Hive DECIMAL maps to `(10,0)`, and the scanner flags it for review. | M | ✅ | [TM] |
| TYP-05 | **CHAR(n) policy** is configurable per table: `preserve_padded` (Hive semantics; Spark default) or `trim`. The migration engine's `spark.sql.readSideCharPadding` shall be set explicitly and recorded per run. The default is ❓ (Q-10). | M | ✅ behavior | Lab 2026-09-28 |
| TYP-06 | `VOID` columns shall be cast to `STRING`. | M | ✅ | [TM] |
| TYP-07 | STRUCT fields shall be declared nullable. Complex types are mapped recursively. | M | ✅ | [TM] |
| TYP-08 | For the **metastore read-barrier** types (UNIONTYPE here; LTZ and INTERVAL in §5), the utility shall generate a **bridge artifact**: a Hive-side CTAS script (and, for UNION, the GenericUDF jar + registration SQL) that materializes a Spark-readable copy. **A view never works as a bridge**, because Spark re-resolves the base table's types. | M | ✅ | Lab 2026-09-24, [TM] addendum §4 |
| TYP-09 | Unknown or unsupported types shall **BLOCK** (fail closed), never pass through. | M | — | — |

---

## 5. Hive date/time types — unavailable in MI or needing conversion

| Hive type | Spark reads as | Available in MI? | Required conversion | Class | Status |
|---|---|---|---|---|---|
| `DATE` | `date` | ✅ `date` | none | PASS | ✅ |
| `TIMESTAMP` (zone-less) | `timestamp` (**LTZ instant**; the displayed value **shifts with the session time zone**) | ✅ as `timestamp` (no zone) **via `TIMESTAMP_NTZ`** | Declare `TIMESTAMP_NTZ`; cast in the read projection. Plain `TIMESTAMP` would land as `timestamptz`. | AUTO_MAP | ✅ Lab 2026-09-24 (LA vs Tokyo shift reproduced; NTZ stable) |
| `TIMESTAMP WITH LOCAL TIME ZONE` (Hive 3) | ❌ **unreadable** (`CANNOT_RECOGNIZE_HIVE_TYPE`, Spark 3.5.2 and 4.1.1) | ✅ as `timestamptz` | Hive-side **materialized** CTAS `CAST(col AS TIMESTAMP)`, UTC-normalized, then map to `timestamptz`. It is a true instant, so do **not** use NTZ. Text-only storage. | REDESIGN | ✅ Lab 2026-09-24 |
| `INTERVAL_YEAR_MONTH` / `INTERVAL_DAY_TIME` | ❌ unreadable (metastore type string) | ❌ **no Iceberg interval type** | Redesign to `BIGINT` (months, or seconds / micros) for aggregation, or `STRING` for display, materialized Hive-side. Text-only storage. | REDESIGN | ✅ Lab 2026-09-24 |
| Nanosecond `TIMESTAMP` (Hive ns / Parquet INT96) | `timestamp`, **silently truncated to µs** | ❌ `timestamp_ns` rejected today | Detect sub-µs values. Accept µs truncation (sign-off) or hold the table until the roadmap lands (VER-04). | REDESIGN / hold | ✅ truncation · 🔮 ns |

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| DT-01 | Hive zone-less `TIMESTAMP` shall map to `TIMESTAMP_NTZ` (Iceberg `timestamp`), with an explicit cast in the read projection. | M | ✅ | Lab 2026-09-24, [TM] |
| DT-02 | Every utility job shall **pin `spark.sql.session.timeZone`** (default `UTC`) and record it in the run record. | M | ✅ behavior | Lab 2026-09-24 |
| DT-03 | `TIMESTAMP WITH LOCAL TIME ZONE` shall be bridged by a generated Hive-side materialization script (TYP-08) and land as `timestamptz`. | M | ✅ | [TM] addendum §3a |
| DT-04 | `INTERVAL_*` columns require a per-column redesign decision (`BIGINT` unit or `STRING`) in table config. The scanner blocks until it is provided, then generates the bridge script. | M | ✅ | [TM] addendum §3c |
| DT-05 | **Sub-µs audit.** For timestamp columns whose source can carry ns (INT96 Parquet, text with 9 fractional digits), the scanner shall count rows with non-zero sub-µs digits and apply the table policy `accept_truncation` or `hold`. | M | ✅ truncation · 🔵 INT96 audit method | [TM] |
| DT-06 | When the ns roadmap gates in VER-04 are met, the utility shall be able to re-map ns columns to `timestamp_ns` / `timestamptz_ns` without redesign. | S | 🔮 | VER-04 |
| DT-07 | Watch item: **calendar rebase** for pre-1582 `DATE`/`TIMESTAMP` and INT96 written by legacy Hive/Spark (`spark.sql.parquet.datetimeRebaseModeInRead`, `int96RebaseModeInRead`). Set explicitly; verify. | C | 🔵 | — |
| DT-08 | Watch item: **string-typed date partition columns** (`dt STRING`) stay `STRING` by default. A typed `DATE` column is an opt-in enrichment. | C | — | — |

---

## 6. Target design & DDL / registration API

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| DDL-01 | **Generate** target DDL per table from the inventory (SRC-01) + type mapping (§4, §5) + table config. Write it as a **reviewable file** (one per table, version-controlled). | M | — | decision 2026-10-20 |
| DDL-02 | **Apply** DDL through the **Databricks SQL Statement Execution API / SDK** on a SQL warehouse. Apply is **idempotent**: create if absent; if present, diff against the expected DDL and **refuse silent drift**. | M | ✅ API path used in lab | `_run_sql.py` pattern in lab evidence (Appendix C) |
| DDL-03 | Every DDL operation shall support **dry-run** (print plan and diff, change nothing). | M | — | — |
| DDL-04 | **Format version per table**, driven by its consumers: **v2** if any v2-only reader must read it; **v3** otherwise (deletion vectors, row lineage). Set it explicitly (`'format-version'='3'`, numeric). | M | ✅ | [MA] §3.3, R-22, R-42 |
| DDL-05 | **Layout per table config:** `CLUSTER BY` (replacing source Z-ORDER columns) or identity `PARTITIONED BY`. With `CLUSTER BY`, set row-tracking / DV state explicitly (a bare CLUSTER BY is rejected). Rules for partitioning vs liquid clustering, and whether Iceberg partition transforms are accepted on MI, are 🔵. | M | ✅ CLUSTER BY rules · 🔵 transforms | lab evidence (Appendix C), R-17, R-23 |
| DDL-06 | Never set **prohibited / locked properties**: `schema.name-mapping.default` (prohibited); `delta.enableChangeDataFeed` on v3 (rejected); `gc.enabled`, `history.expire.max-snapshot-age-ms` (locked). | M | ✅ | lab evidence (Appendix C) |
| DDL-07 | **Retention per table class:** set `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` ≥ max(validation lag, rollback window, required time-travel look-back). Do **not** rely on `history.expire.min-snapshots-to-keep` (UC reverts it to 100 on any ALTER-family DDL). | M | ✅ | snapshot_retention.md, R-46, Q-04 |
| DDL-08 | **Two SP roles:** `ddl-admin` (DDL, GRANT, RESTORE via the Statement API) and a per-domain `sync` SP (data writes over IRC; no CREATE). **Grants** in the same apply step: sync SP gets `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `EXTERNAL USE SCHEMA`; all other principals read-only. Owner = a group, not the SP. | M | ✅ grants · — owner rule | [A13] resolved items |
| DDL-09 | **Registration metadata:** column/table comments carried over from HMS; UC **tags** (source system, source `db.table`, data domain, migration wave, sync mode). Custom table properties for lineage are 🔵 (confirm allowed on MI). | S | 🔵 | — |
| DDL-10 | **Predictive Optimization** enabled at catalog/schema level for migrated tables. External engines cannot run maintenance (`gc.enabled=false`). | S | ✅ | [QA] Q7 |
| DDL-11 | **Namespace mapping** HMS `db.table` → `<catalog>.<schema>.<table>` is rule-based and config-driven, with identifier normalization (lowercase) and **collision detection**. | M | — | — |
| DDL-12 | Persist an **applied-state manifest** (DDL hash, applied at/by, warehouse, result) to detect later drift. | S | — | — |
| DDL-13 | Tables are **not** created through IRC `createTable`: it offers less control over Databricks properties, and IRC-born tables get the `catalogManaged` feature (a different commit path from SQL-created tables). | M | ✅ observed | Lab 2026-09-14 |
| DDL-14 | Schema changes (ALTER) use the same API path when driven proactively (INC-13). In-sync additive ALTERs from the external client are explicit (INC-12). | M | ✅ | [FA] #2 |
| DDL-15 | **Layout rules** (partitioning and liquid clustering are mutually exclusive on MI): keyed MERGE targets → `CLUSTER BY (<change-locality column>, <keys…>)` with merge-on-read write modes; keyless and `hive-partition` targets → identity `PARTITIONED BY` the source slice columns (the only layout on which `overwritePartitions()` works); staging tables → `PARTITIONED BY (_h2mi_batch_id)` so clearing a batch is a metadata-only partition drop. Clustering on a surrogate/UUID key alone is rejected: per-file bounds overlap every batch and nothing prunes. `format-version` is always explicit (MI defaults to 2). | M | ✅ partition replace · 🔵 pruning (test #19) | Lab 2026-10-22, 2026-10-30 |
| DDL-16 | **Spec guard.** Before every cycle, compare the live target's partition spec / `CLUSTER BY` / format-version with the registry; on any difference set the table to `BLOCKED(table_replaced)` and alert. Recovery is `RESTORE TABLE` to the Delta version before the replace (within DDL-07 retention). | M | 🔵 restore path | ARC-09, Lab 2026-10-30 |

---

## 7. Initial (historical) load, including PB scale

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| INI-01 | **Pin the source state** before copying: Iceberg → record snapshot **S0** in the control table; Hive → record the partition list + per-partition **fingerprint** (file count, bytes, max mtime). | M | ✅ Iceberg pattern | [MA] §3.3, test `d_` |
| INI-02 | **Partition-batched commits**: batch size is configurable (partitions or bytes); never one commit per file; bounded commits per table. | M | 🔵 scale | R-14, R-28, R-29 |
| INI-03 | **Resumable and idempotent per batch.** Checkpoint each committed batch; on restart resume from the first uncommitted batch. Partitioned targets use `overwritePartitions()`. Appends carry a batch tag (INC-08). | M | ✅ building blocks | [FA] #3 |
| INI-04 | **Parallelism by table**, with configurable concurrency limits and single writer per table (ARC-06). | M | — | — |
| INI-05 | **Wave planning** from the inventory: the utility emits a wave plan (tables, bytes, partitions, estimated duration at a given bandwidth). | S | — | R-18 |
| INI-06 | **Engine selection per table** (`onprem-push` or `cloud-landing`) is a config rule driven by size, available bandwidth, on-prem capacity and format. | M | — | ARC-03 |
| INI-07 | **`cloud-landing` engine:** bulk-copy raw files (distcp or equivalent) to an S3 landing external location; a Databricks job reads them with the **inventory schema** (not HMS) and the §4/§5 mapping, writes MI, and cleans the landing area after VALIDATED. Hive path partitions are injected by Spark path discovery. | M | 🔵 end-to-end · ✅ path discovery | [QA] Q9 |
| INI-08 | **Offline transfer** (e.g. physical appliance) for cold history: ❓ acceptable to the customer? | C | ❓ | Q-09 |
| INI-09 | **Bandwidth model.** The utility shall report estimated transfer time per wave from measured throughput (reference below). DX sizing is the critical path. | S | — | R-38, Q-02 |
| INI-10 | **Target layout on write:** right-sized files, zstd, clustering per DDL-05, so no post-load rewrite is needed. | S | ✅ | [MA] §3.4 |
| INI-11 | **Hand-off to incremental:** the first incremental cycle starts from S0 (Iceberg) or from the pinned partition fingerprints (Hive), so changes made during a long copy are captured. | M | ✅ Iceberg | [MA] §3.3 |
| INI-12 | ⚠️ A long initial load must finish before the source **expires S0** (on-prem snapshot retention). The utility checks S0 still exists before every batch and fails clearly if it does not. | M | — | INC-11 |

**Bandwidth reference** (decimal PB, link fully saturated; real throughput is lower):

| Volume | 10 Gbps | 40 Gbps | 100 Gbps |
|---|---|---|---|
| 1 PB | ≈ 9.3 days | ≈ 2.3 days | ≈ 22 hours |
| 40 PB | ≈ 1.0 year | ≈ 93 days | ≈ 37 days |
| 80 PB | ≈ 2.0 years | ≈ 185 days | ≈ 74 days |

---

## 8. distcp + `add_files` — applicability and benchmark

`add_files` registers existing Parquet files into MI without rewriting them. It saves the **rewrite**,
not the **transfer**.

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| ADF-01 | **Eligibility.** A table may use Approach A only if **all** of these hold: Hive **Parquet**; **append-only**; **unpartitioned, or the partition column is present inside the data files**; stable schema; acceptable file layout (size histogram within thresholds). Everything else uses Approach B. | M | ✅ rules | [MA] §2, [FA] #4, [A13] #4 |
| ADF-02 | Use **PyIceberg** `add_files` committing through the IRC. **Never** the Spark `CALL system.add_files` procedure: it commits the prohibited `schema.name-mapping.default` and always fails on MI, regardless of file location or field IDs. | M | ✅ | Lab 2026-09-01, test `a_` |
| ADF-03 | Files shall be copied **under the table's `write.data.path`** first, because UC vends credentials only inside the table path. | M | ✅ | ADF doc (lab evidence (Appendix C)) |
| ADF-04 | Each file must contain **exactly one value per partition column**. A path-only Hive partition cannot be inferred, so the table is ineligible. | M | ✅ | [MA] §2, [QA] Q9 |
| ADF-05 | ⚠️ The working PyIceberg path relies on **in-memory name-mapping injection** (PyIceberg internals; undocumented). Pin the PyIceberg version and keep a regression test. | M | ✅ ⚠️ | test `a_addfiles_pyiceberg.py` |
| ADF-06 | Keep a **registration ledger** of source file paths. Idempotency is by path only: a re-extract written to a new filename would duplicate rows. | M | ✅ | [MA] §2 |
| ADF-07 | Append-only: restated or late partitions go through Approach B (`overwritePartitions`). | M | ✅ | [MA] §2 |
| ADF-08 | Parquet compatibility checks before registration (physical types written by Hive, e.g. INT96 timestamps, int8/int16 annotations vs the MI `INT` column). | S | 🔵 | — |
| ADF-09 | **Benchmark A vs B (sample test) before building A.** Use a representative Hive Parquet table, e.g. **~1 TB, ≥ 30 daily partitions, realistic file-size histogram**. Measure: wall-clock, on-prem CPU-hours, bytes over the network, resulting file count and average size, commit count, **post-PO rewrite cost** (A inherits the layout), query latency before/after `OPTIMIZE`. Output: a go/no-go rule for A, with the break-even point. | M | 🔵 | user ask 2026-10-20, R-28, R-29 |
| ADF-10 | Account for **PO rewriting registered files** afterwards (new snapshots, changed layout, compute cost). | S | ✅ | [MA] §2 |

---

## 9. Incremental sync

### 9.1 Sync modes (chosen per table from inventory + config)

| Mode | Source | Method | Status |
|---|---|---|---|
| `iceberg-append` | Iceberg, append-only history | incremental snapshot scan (`start-snapshot-id` exclusive, `end-snapshot-id` inclusive) → append | ✅ test `d_` |
| `iceberg-upsert` | Iceberg with updates/deletes **and a key**, no delete files | `create_changelog_view` → collapse to the **net change per key** → `MERGE` | ✅ test `f_`, [MA] §3.3 |
| `iceberg-mor` | Iceberg whose sync range contains **delete files** (MoR), or with **no key** | `create_changelog_view` **rejects snapshots with delete manifests** (still true in Iceberg 1.12.0, `BaseIncrementalChangelogScan`), so use a **snapshot-diff**: read the touched partitions at the previous and current snapshot (reads apply deletes), `EXCEPT` both ways → net changes → MERGE (with a key) or `overwrite(<partition filter>)` (no key). Do **not** use `overwritePartitions()` on a CLUSTER BY (unpartitioned) target, because that replaces the whole table. | 🔵 design · ✅ limitation confirmed |
| `hive-partition` | Hive (no snapshots) | partition **high-water mark** + optional per-partition fingerprint → `overwritePartitions()` (dynamic) | ✅ foundation, [FA] #3 |

### 9.2 Requirements

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| INC-01 | Choose the sync mode per table from the inventory (delete files, key presence, snapshot operations) plus config overrides. | M | — | §9.1 |
| INC-02 | **Append guard:** before running `iceberg-append`, verify that the range contains only `append` snapshots. If any `overwrite`/`delete` snapshot is found, fail or escalate to `iceberg-upsert`. An append over such a range silently duplicates updates and misses deletes. | M | ✅ failure mode | [MA] §3.3, §3.5 |
| INC-03 | `iceberg-upsert` shall collapse to one net row per key before MERGE (else MERGE cardinality errors occur), using the corrected ordering in [MA] §3.3. | M | ✅ | [MA] §3.3 |
| INC-04 | `iceberg-mor` mode: snapshot-diff over the touched partitions (from the manifests added in the range) → MERGE (keyed) or **partition replace** `overwritePartitions()` on a `PARTITIONED BY` target (keyless); a clustered keyless target is restated with a Databricks-side `REPLACE WHERE` (or `MERGE … WHEN NOT MATCHED BY SOURCE`). Note: source CoW/MoR does **not** affect the full-snapshot reads (initial load, partition re-reads), only the changelog mechanism. | M | ✅ snapshot-diff + MERGE · partition replace · REPLACE WHERE (all lab) | Lab 2026-10-22, 2026-10-30; Iceberg 1.12.0 `BaseIncrementalChangelogScan` |
| INC-05 | `hive-partition`: track a per-table partition high-water mark. Each run restates **every** partition past it, so a **backlog of N partitions** takes the same idempotent path as the normal one-partition case. Advance the mark per partition. | M | ✅ foundation | [FA] #3, [A13] #3 |
| INC-06 | **Late data** in already-synced Hive partitions: within a configurable look-back window, compare source fingerprints with the last synced fingerprint and restate the partitions that changed. | S | — | [FA] #3 |
| INC-07 | **Manual backfill** for an explicit range or partition list, without moving the forward checkpoint. | M | — | [FA] #3 |
| INC-08 | **Atomic watermark.** Append path: write the source watermark as an Iceberg **snapshot-summary property** on the same commit (✅). MERGE does **not** carry snapshot properties (✅ tested v2 and v3), so use **staging-append + MERGE**, with these invariants: one batch staged at a time; never clear staging before the MERGE is confirmed; **unconditional re-MERGE from staging on every start**. | M | ✅ | [QA] Q2, [A13] |
| INC-09 | **One atomic target commit per cycle per table** wherever the mode allows. | S | ✅ | [MA] §3.3 |
| INC-10 | **Control table**, one row per table per cycle: `table, cycle_id, mode, from_watermark, to_watermark, target_version, target_snapshot_id, rows_src, rows_tgt, checksums, status (STARTED / COMMITTED / VALIDATED / FAILED), run_id, started_at, finished_at, error`. The **watermark** is the `to_watermark` of the latest VALIDATED row. | M | ✅ design · RESTORE ✅ | [FA] #1 |
| INC-11 | **Sync lag < source snapshot retention.** If the start snapshot was expired on-prem, fail clearly and fall back to a partition restate or re-sync for that table. | M | — | — |
| INC-12 | **Reactive schema-drift check** at the start of each cycle: auto-apply **additive** changes (new nullable columns) with an explicit ALTER (auto schema-merge is Databricks-only and does not work over IRC); **halt and alert** on breaking changes (drop, narrowing, incompatible rename, type change outside the §4 mapping). | M | ✅ | [QA] Q6, [FA] #2 |
| INC-13 | **Proactive DDL hook:** expose an `apply-ddl` entry point that the on-prem change-management / CI-CD pipeline can call when on-prem DDL changes. | S | — | [FA] #2 |
| INC-14 | Sync **cadence** configurable per table. The staleness SLA is ❓ (Q-03). | M | ❓ | R-04 |
| INC-15 | **Version ↔ snapshot mapping** for audit: record target version and snapshot ID per cycle, and expose the `metadata_log_entries` mapping query. External snapshot history **is** visible over IRC. | S | ✅ | [MA] §4.1, test `e_`, tests README (correction 2026-08-06) |
| INC-16 | **MERGE executor.** The net change set is appended to the tagged staging table over IRC (upload only), then `MERGE INTO target USING staging WHERE batch = :b` runs on the **SQL warehouse via the Statement API** by default. `merge_executor: onprem` (MERGE from on-prem Spark over IRC) is allowed only when the planner's estimate of the target bytes it would download (candidate files from `<tgt>.files` bounds vs the batch's key/locality ranges) is under `onprem_merge_budget_bytes`. The estimate and the chosen executor are recorded in the control table. | M | 🔵 | ARC-08, design §8.4/§8.6 |
| INC-18 | **Apply ladder; partition replace is chosen, not defaulted.** For a range or partition with row changes the planner picks the cheapest exact method: keyed → MERGE (partition replace only on a partitioned target with dense change); keyless Iceberg → row-value replace from the local snapshot-diff; keyless Hive with only new files → file-delta append; keyless Hive rewritten with a sparse change → **bucketed hash-diff** (4096-bucket digests both sides, exchange hashes of mismatched buckets only, then row-value replace on Databricks); dense change (≥ `replace_density_threshold`) or partition < `small_partition_bytes` → `overwritePartitions()` (Databricks `REPLACE WHERE` on clustered targets). Rewriting GBs for a few changed rows is a planner defect, not a mode. | M | 🟡 row-value replace ✅ · REPLACE WHERE ✅ · hash-diff 🔵 (test #24) | Lab 2026-10-22, 2026-10-30; design §8.6 |
| INC-17 | **Change-locality column.** Keyed MERGE targets declare the column that correlates with where changes land (typically the event/business date). It leads the `CLUSTER BY`, and the planner appends its range for the batch to the MERGE `ON` clause and to the scoped validation predicate, so clustering actually prunes. Tables with no such column are flagged (near-full scans per cycle) and get a full-reload cadence recommendation. | S | 🔵 (test #19) | DDL-15 |

---

## 10. Validation

### 10.1 Levels

| Level | Check | Cost | Catches |
|---|---|---|---|
| L0 | `count(*)` overall and **per partition** | trivial | row loss or duplication, missed partitions |
| L1 | aggregates per column: sum/min/max of numerics, `count(distinct key)`, null counts | low | value drift, precision/type issues, key duplicates |
| L2 | bidirectional row-hash diff (full for small/medium tables; sampled partitions for large ones) | high | any row-level discrepancy |
| L3 | continuous reconciliation during the parallel run (watermark + periodic L1) | ongoing | drift while both systems are live |

✅ L0–L2 exact match verified (16 = 16; (16, 16, 4278.00) both sides; L2 diff 0 / 0) — [QA] Q3.

### 10.2 Where validation runs

| Option | How it works | Pros | Cons |
|---|---|---|---|
| **On-prem only** | on-prem Spark reads the source locally and the target over IRC | no new cloud compute; one engine computes both sides, so hashes are deterministic | **reads the target data back** over DX (bandwidth + S3 egress); competes with the migration for on-prem capacity; L2 at PB scale is impractical |
| **Cloud only** | Databricks reads the target locally | target reads are cheap and fast; elastic compute | **cannot read the source** without an inbound port (SCP-10), so it works only for `cloud-landing` tables, and then it validates the landed copy, not the live source |
| **Hybrid (recommended)** | each side computes **per-partition fingerprints locally** (counts, aggregates, hash sums); only the digests are compared in the control table | no bulk data crosses the network; each read is local; scales with partitions; works for both engines | needs **hash determinism across OSS Spark and Databricks** (VAL-09); two jobs to coordinate |

v0.2 refines the hybrid option further (VAL-12, VAL-13): the gating tier needs **no scan at all** on the target (commit
metadata only), the source fingerprint rides the write pass, and the DBSQL row-hash is scoped to the batch's files or keys
and runs asynchronously. A full target scan happens twice per table: after the historical load and at cutover.

### 10.3 Requirements

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| VAL-01 | Implement L0–L3. The validation tier is configured per table by criticality (bulk: L0+L1 per partition; financial/high-value: +L2). | M | ✅ L0–L2 | [QA] Q3 |
| VAL-02 | Validate **per partition** so that a mismatch localizes to a partition. | M | ✅ | [QA] Q3 |
| VAL-03 | **Type-aware checks** for the silent risks: decimal precision/scale, timestamp spot checks (NTZ vs LTZ), CHAR length distribution, null counts per column, the sub-µs truncation count (DT-05). | M | — | [TM] step 5 |
| VAL-04 | The watermark advances **only after VALIDATED** (the metadata tier of VAL-12). The asynchronous L2 result is recorded separately as VERIFIED; an L2 mismatch triggers VAL-05 to the last VERIFIED version and re-plans the cycles after it. | M | ✅ gate · 🔵 async tier | [A13] resolved items |
| VAL-05 | **On failure:** `RESTORE TABLE … TO VERSION AS OF <target_version of last VALIDATED cycle>` (metadata-only, non-destructive, visible over IRC immediately), reset the checkpoint, re-sync forward. RESTORE is a Databricks-side step invoked through the Statement API. | M | ✅ Lab (v3, 4 batches) | [FA] #1 |
| VAL-06 | Two-tier: validate the **staged delta** before MERGE (cheap, internal consistency) and reconcile source vs target **after commit**. | S | — | [FA] #1 |
| VAL-07 | **Hybrid execution** (§10.2) is the default. Each side writes its fingerprints to the control schema, and a comparator sets the status. | M | — | user ask 2026-10-20 |
| VAL-08 | Never reconcile on snapshot IDs or version numbers. | M | ✅ | SCP-17 |
| VAL-09 | **Hash determinism:** define a canonical row encoding (decimal scale, timestamp µs/UTC, float normalization, NULL sentinel, ordering of complex types) and prove that OSS Spark and DBR produce identical digests. | M | 🔵 | — |
| VAL-10 | Validation results are persisted and queryable (DBSQL) per table/cycle/partition. | M | — | — |
| VAL-11 | Target retention (DDL-07) must exceed the maximum lag between a write and its validation, or RESTORE cannot reach the last good version. | M | ✅ | [FA] #1 |
| VAL-12 | **Tiered target validation by cost.** Tier **M** (every cycle, gates the watermark): the commit's own metadata over IRC (snapshot summary `added-records` / `added-files-size`, per-file `record_count`, null counts, lower/upper bounds of the files added) against the source-side figures, with no data scan on either side. **L2-scoped** (asynchronous): DBSQL row-hash sum over the files added by the commit or over the batch's changed keys within the locality range, never over the whole partition or table; every cycle on `financial`, a rotating partition sample on `standard`. **L2-full**: once after the historical load and as the **cutover gate** on the frozen source. The target queries always carry the cycle's partition / locality predicate. | M | 🟡 file-scoped L2 ✅ (`_metadata.file_path` on MI, 2026-10-30) · tier M / async 🔵 | ARC-08; rationale: transport integrity is covered by S3 checksums and Iceberg file sizes, the residual risk is systematic projection logic |
| VAL-13 | **One source pass.** The source fingerprint of a batch is computed in the same Spark action as the write (`Observation` / `df.observe` on the projected DataFrame), not by a second read at `VERSION AS OF`. Fallback: `persist()` + two actions, still one source read. | S | ✅ Lab 2026-10-30 (observation fires on the V2 append, equals a separate aggregation) | NFR-02 |

---

## 11. Audit & identity — who on-prem used the SP

**Problem.** UC records the **service principal** as the actor of every IRC call. The customer needs to know
**which on-prem identity** (person or service account) ran the job that used the SP.

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| AUD-01 | Every UC write made by the utility shall be **attributable to an on-prem identity** (Kerberos principal / OS user) and to a specific **run**. | M | — | user ask 2026-10-20 |
| AUD-02 | **Run record** written at the start of every run: `run_id`, on-prem **Kerberos principal**, OS user, host, scheduler job/run ID, YARN application ID, SP application ID, utility version, config hash, start/end, outcome. | M | ✅ feasible (control-table writes over IRC) | — |
| AUD-03 | **Commit tagging:** carry `run_id` (and on-prem principal) as **snapshot-summary properties** on every append and staging commit. MERGE commits cannot carry them (✅ tested), so the MERGE is attributed through its staging commit + run record. | M | ✅ append · ❌ MERGE | [QA] Q2 |
| AUD-04 | **Request tagging:** send `run_id` in a custom IRC header and/or the HTTP User-Agent. Test whether UC audit logs capture it. | S | 🔵 | — |
| AUD-05 | **Audit join:** provide a view joining `system.access.audit` (IRC events: SP, source IP, user agent, time) with the run record on run_id / time window / source IP. Confirm the IRC service name and fields. | M | 🔵 | — |
| AUD-06 | **Secret access trail:** the SP secret lives in a vault whose access log records who retrieved it (complements AUD-02). | S | — | [QA] Q8 |
| AUD-07 | **SP per data domain**, with on-prem ACLs restricting which principals can launch the job / read that SP's secret. | S | — | — |
| AUD-08 | Control and run tables are writable **only by the sync SP**; their history (table versions) acts as a tamper-evident log. | S | — | — |
| AUD-09 | DDL / grant / RESTORE operations through the Statement API record the invoking identity in the run record. | S | — | — |

---

## 12. Security & network

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| SEC-01 | Catalog auth with OAuth M2M using the **`credential` form** (`<client_id>:<secret>`, auto-refresh), with **`token-exchange-enabled=false`** so the refresh re-runs the client-credentials grant. Databricks OIDC rejects the default token-exchange refresh, which breaks any job running past ~60 min (VER-01). Never a static `token` for jobs longer than a few minutes. | M | ✅ | [QA] Q1, Q8; Lab 2026-10-22 |
| SEC-02 | The SP secret is **injected from a vault** at session start. Never put it in `spark-defaults`, Spark conf files or the command line (the Spark UI Environment tab exposes it; CLI args get logged). | M | ✅ | [QA] Q8, [A13] |
| SEC-03 | **Least privilege** for the sync SP (DDL-08). Rotate secrets on a schedule; respect the SP secret-count and lifetime limits. | M | ✅ | [QA] Q8 |
| SEC-04 | **Outbound-only network:** HTTPS 443 from the on-prem driver **and every executor** to (a) the workspace host (IRC + `/oidc/v1/token`) and (b) the regional S3 endpoint. | M | ✅ | [FA] #7 |
| SEC-05 | Workspace reachability through **either** the IP access list (allow-listed on-prem egress IPs) **or front-end PrivateLink** over DX. **Back-end PrivateLink alone does not serve IRC.** | M | ✅ analysis | lab evidence (Appendix C) |
| SEC-06 | **Storage credentials:** UC-vended (default for `onprem-push`) or an IAM role (for `cloud-landing` and any non-table staging path). | M | ✅ | [FA] #5, #7 |
| SEC-07 | If buckets are SSE-KMS, grant `kms:GenerateDataKey/Decrypt/Encrypt` to the writing identities. Proxies and TLS inspection must allow the S3 and workspace endpoints. | M | ✅ checklist | [QA] Q10, [FA] #7 |
| SEC-08 | On-prem reads use the existing **Kerberos** setup (job principal + keytab). No Kerberos dependency in the cloud. | M | — | SCP-11 |
| SEC-09 | Tolerate **IP-ACL propagation flaps** (intermittent 403s for minutes after an allow-list change): retry with backoff. | M | ✅ observed | Lab 2026-09 |
| SEC-10 | The per-domain sync SP additionally gets **`CAN USE` on the migration SQL warehouse** (for the Statement-API MERGE, REPLACE WHERE and scoped L2 of INC-16 / VAL-12), nothing else beyond DDL-08. `ddl-admin` is unchanged. | M | — | ARC-08 |

---

## 13. Platform versions — Spark & Iceberg

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| VER-01 | **Iceberg ≥ 1.11.0** on every utility client. It is the single floor covering both needs: (a) **v3 target writes** need ≥ 1.9.2 (1.5.2–1.7.1 fail to parse a v3 `LoadTableResponse`; 1.5.2 works for v2 targets only); (b) **jobs longer than ~1 h** need 1.11+ **and `token-exchange-enabled=false`** (SEC-01). In the lab (2026-10-22), one 70-min write against UC **failed on both 1.11.0 and 1.12.0** with default settings. Databricks OIDC rejects the catalog's token-exchange refresh (`requested_token_type is empty`), so the OAuth token expires at ~60 min and the commit fails. With `token-exchange-enabled=false`, **both versions pass**. S3FileIO's vended-credential refresh does **not** engage against UC: loadTable omits `client.refresh-credentials-endpoint`, giving `Invalid credentials endpoint: null`. Executor data writes still succeeded after 70 min, even though a held vended credential died at ~60 min in an earlier probe (2026-09-03), and why is open (§16 #9). 1.11 also provides `rewrite_table_path`. Latest release: 1.12.0 (2026-09-29). | M | ✅ (b) partial | Lab 2026-08-27, 2026-09-03, 2026-10-22; [QA] Q1 |
| VER-02 | **Avoid Iceberg 1.8.x**: its OAuth client-credentials flow returns 401 against UC. | M | ✅ | Lab 2026-08-27 |
| VER-03 | **Interim Spark baseline = Spark 4.1 + Iceberg ≥ 1.11** (verified: Spark 4.1.3 / Iceberg 1.11.0 → MI v3; Hive 3.1.3 metastore reads on Spark 4.1.1). **Fallback:** Spark 3.5 + Iceberg 1.11 if the customer cannot run 4.x on-prem (3.5.2 verified). Never pair the Iceberg 4.1 runtime with Spark 4.2 (reported `IncompatibleClassChangeError`). | M | ✅ | [MA] intro, Lab 2026-09-24 |
| VER-04 | **Target Spark = 4.3**, the first OSS Spark with **nanosecond timestamps** (`TIMESTAMP_NTZ(9)` / `TIMESTAMP_LTZ(9)`, SPARK-56822, behind `spark.sql.types.framework.enabled` and `spark.sql.timestampNanosTypes.enabled`). It closes the silent µs truncation (§5). **Gated on:** (1) Spark 4.3 GA (not released as of 2026-10-21; latest is 4.2.0, 2026-07-14); (2) an Iceberg `iceberg-spark-runtime-4.3` (none yet; Iceberg 1.12 ships runtimes for Spark 3.5/4.0/4.1, and 4.2 is still under validation, apache/iceberg#17959); (3) MI `timestamp_ns` GA and the connector mapping `TIMESTAMP_*(9)` → v3 `timestamp_ns`/`timestamptz_ns`. | S | 🔮 | public release info 2026-10-21 |
| VER-05 | The **4.1 → 4.3 upgrade must be a config/jar swap**: Scala 2.13 and Java 17+ from day one; no `.option("snapshot-id")` (removed in Spark 4, so use `VERSION AS OF`); version-pinned jars isolated from production jobs. | M | ✅ API change | [MA] §4.2 |
| VER-06 | Spark's Hive metastore client supports **Hive ≤ 3.1.x** (`spark.sql.hive.metastore.version` + jars). The three read-barrier types fail identically on 3.5 and 4.1; re-check on 4.3. | M | ✅ · 🔵 4.3 | Lab 2026-09-24 |
| VER-07 | Catalog configuration follows the verified template ([MA] §3.2): `type=rest`, `rest.auth.type=oauth2`, `oauth2-server-uri`, `scope=all-apis`, `io-impl=S3FileIO`, header `X-Iceberg-Access-Delegation=vended-credentials`, `client.region`. Use the `/iceberg-rest` endpoint, not the deprecated `/iceberg`. | M | ✅ | [MA] §3.2 |
| VER-08 | **Spark 4 ANSI mode** (default on): invalid casts **throw** instead of returning NULL. Reparse/enrichment paths shall catch them and route bad rows to a quarantine table with a count in the run record. Never disable ANSI to "make it pass". | M | — | SRC-03 |
| VER-09 | ❓ the customer's current on-prem Spark / Hadoop / Hive / Java versions. Can a Spark 4.x job (Hadoop 3.4 client) read their Kerberized HDFS? Is a dedicated newer-stack migration cluster available? | M | ❓ | Q-06 |

---

## 14. Operations & reliability

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| OPS-01 | **Scheduler-agnostic CLI** with meaningful exit codes and a JSON result per run. The scheduler is ❓ (Q-08). | M | ❓ | — |
| OPS-02 | Run in an **isolated resource pool / queue**, separate from production jobs. | S | — | — |
| OPS-03 | **Single-writer enforcement:** a per-table lease in the control schema prevents two concurrent runs on the same table. | M | — | ARC-06 |
| OPS-04 | **Commit retry:** retry commit conflicts after a metadata refresh. On an **unknown-outcome** commit (timeout / 5xx), do not retry blindly: resolve by checking the target for the batch tag (AUD-03) before re-running. | M | — | INC-08 |
| OPS-05 | **Failed writes** leave only uncommitted orphan files (Iceberg commits are atomic). Recovery = re-run the batch. **Never** run external `remove_orphan_files` / `expire_snapshots` on MI: UC and PO own the lifecycle. | M | ✅ | [QA] Q7 |
| OPS-06 | **Restart from durable state.** Every run first reconciles the control table (re-MERGE staging, validate COMMITTED rows) before advancing. No "was I restarted?" branches. | M | ✅ design | [QA] Q2 |
| OPS-07 | **PO coexistence:** validation and the version mapping tolerate PO-created versions and snapshots (`replace` snapshots with an unchanged row count). | M | ✅ | [MA] §4.1 |
| OPS-08 | **Monitoring:** DBSQL views/dashboard over the control tables (lag per table vs SLA, failures, throughput, bytes moved, backlog partitions). | S | — | — |
| OPS-09 | **Alerting** on FAILED cycles, breaking schema drift, lag > SLA, expired source snapshot, and validation mismatches. | S | — | — |
| OPS-10 | **Structured logs** per run, keyed by `run_id`. | S | — | — |
| OPS-11 | **HMS load:** throttle metastore calls (partition listing at scale). | S | 🔵 | — |

---

## 15. Non-functional

| ID | Requirement | Pri | Status | Evidence |
|---|---|---|---|---|
| NFR-01 | Meet the per-table **staleness SLA** for "eventually". | M | ❓ | R-04, Q-03 |
| NFR-02 | Throughput is bounded by the network, not by the utility. Aggregate throughput scales by running tables in parallel. | M | — | SCP-12 |
| NFR-03 | Scale to the full inventory (tables × partitions) with bounded commits and metadata calls per table. | M | 🔵 | SCP-13 |
| NFR-04 | **Idempotent** at every step (DDL, batch, cycle, backfill, validation). | M | — | — |
| NFR-05 | **Restartable** from any failure point without data loss or duplication. | M | — | OPS-06 |
| NFR-06 | **Config-driven onboarding:** adding a table requires config only, no code change. | M | — | — |
| NFR-07 | **Dry-run** available for every mutating command. | M | — | DDL-03 |
| NFR-08 | **Portable:** no environment identifiers in code. Workspace, catalog, buckets and SPs come from per-environment config. | M | — | — |
| NFR-09 | **Reproducible in the lab** (on-prem simulated on EC2 + Hive 3.1.3 metastore) for regression tests. | S | ✅ lab exists | lab evidence (Appendix C) |
| NFR-10 | **Cost visibility:** bytes moved, compute hours and commits per table and per wave. | S | — | — |
| NFR-11 | **Download visibility.** Bytes expected and actually read from the target per batch (`expected_download_bytes`, candidate files / bytes) are recorded and dashboarded; a batch over `onprem_merge_budget_bytes` is routed to Databricks (INC-16) and an upload-only cycle with non-zero download raises an alert. | S | — | ARC-08, SCP-12 |

---

## 16. Test backlog

Ranked by impact on design decisions.

| # | Test | Drives | Requirement |
|---|---|---|---|
| 1 | **Benchmark A vs B** on a representative ~1 TB Hive Parquet table | whether Approach A is built at all | ADF-09, R-28, R-29 |
| 2 | **`iceberg-mor` mode:** snapshot-diff over touched partitions (keyed MERGE / keyless filter-overwrite), incl. a CLUSTER BY target | sync design for MoR tables | INC-04, SRC-10 |
| 3 | **Hash determinism** OSS Spark vs DBR (canonical encoding) | hybrid validation | VAL-09 |
| 4 | **Audit capture** of a custom header / User-Agent; confirm IRC audit fields | on-prem identity attribution | AUD-04, AUD-05 |
| 5 | **Hive ORC and Avro** end-to-end into MI (incl. Avro logical types, enums, fixed, complex unions) | source coverage | §3.1 |
| 6 | **Iceberg ORC / mixed Parquet+ORC** data files | source coverage | SRC-09 |
| 7 | **`cloud-landing` engine** end-to-end (landed files → Databricks job → MI) | PB-scale path | INI-07 |
| 8 | **Scale:** commit and metadata overhead across many partitions and tables; HMS listing load | batching parameters | INI-02, NFR-03, OPS-11 |
| 9 | **> 1 h write**: lab ✅ on 1.11.0 and 1.12.0 with `token-exchange-enabled=false` (70 min). Open: (a) why executor S3 writes outlive the ~60 min vended-credential TTL without refresh, which needs a loadTable TTL probe plus a > 2 h hold; (b) the same on the production-like runtime; (c) UC to return `client.refresh-credentials-endpoint` (raise with the UC IRC team) | VER-01 confirmation | VER-01, SEC-01 |
| 10 | **Credential vending in isolation** (host without direct S3 access) | onprem-push security posture | SEC-06 |
| 11 | **Partition transforms vs liquid clustering** on MI; partitioning rules | DDL layout rules | DDL-05 |
| 12 | **Custom table properties** allowed on MI (lineage tags) | registration metadata | DDL-09 |
| 13 | **Sub-µs audit** on INT96 Parquet; re-test on Spark 4.3 when available | ns policy | DT-05, VER-04, VER-06 |
| 14 | **Calendar rebase** (pre-1582 / INT96) | date correctness | DT-07 |
| 15 | **add_files Parquet compatibility** (INT96, int8/int16 annotations) | Approach A safety | ADF-08 |
| 16 | **HMS lock/commit load** of the `h2mi_ctl` control tables at target parallelism | state-store viability | ARC-07 |
| 17 | **HDFS lease atomicity** (create/rename under contention, clock skew) | single-writer enforcement | OPS-03 |
| 18 | **Snapshot properties on `overwritePartitions()` / filter-`overwrite`** commits — ✅ Lab 2026-10-22 and 2026-10-30: the tag rides both on a `PARTITIONED BY` target; filter-overwrite fails on `CLUSTER BY` targets; the V1 `mode("overwrite")` path is REPLACE TABLE (ARC-09) | reconcile-by-tag for hive/mor modes | INC-05, INC-08 |
| 19 | **Pruning efficiency** of `CLUSTER BY (locality, key)` vs key-only under local vs scattered updates: measure candidate bytes (from `<tgt>.files`) and DBSQL bytes scanned per MERGE and per scoped L2 | layout rules, download budget default | DDL-15, INC-17, ARC-08 |
| 20 | **`INSERT INTO … REPLACE WHERE` / `MERGE … WHEN NOT MATCHED BY SOURCE`** on MI for slice restates on clustered targets — ✅ Lab 2026-10-30: both restate one slice atomically on a v3 `CLUSTER BY` table via the Statement API | keyless restate on `CLUSTER BY` targets | INC-04 |
| 21 | **Tagging an external MERGE commit** (`CommitMetadata.withCommitProperties`, `spark.wap.id`) | whether staging stays a correctness mechanism or becomes a pure upload buffer | INC-08 |
| 22 | **`Dataset.observe()` on Iceberg V2 writes** and `_metadata.file_path` filtering on MI — ✅ Lab 2026-10-30: observation fires and matches; the commit's added files scoped via `_file` (on-prem) and `_metadata.file_path` (DBSQL) give the same digest | one-pass source fingerprint, file-scoped L2 | VAL-13, VAL-12 |
| 23 | **Spec guard + RESTORE after a REPLACE TABLE over IRC** (replace observed and characterised 2026-10-30; the RESTORE path is open) | integrity guard | DDL-16, ARC-09 |
| 24 | **Bucketed hash-diff vs partition replace** on a TB-class rewritten Hive partition with 1–2 changed rows: bytes uploaded, DBSQL bytes scanned, wall time, CDF rows seen downstream | keyless apply ladder, threshold calibration | INC-18 |

---

## 17. Open questions for the customer

| # | Question | Blocks |
|---|---|---|
| Q-01 | **Inventory:** table counts by source type and format; size distribution; **file-size histogram**; partition counts | wave plan, Approach A share (R-11, R-18) |
| Q-02 | **Bandwidth** available on-prem → AWS (DX capacity, dedicated vs shared) | timeline (R-38) |
| Q-03 | **Staleness SLA** for "eventually" (minutes / hours / a day) | cadence (R-04) |
| Q-04 | **Time-travel look-back window** required on the target | retention (R-46, R-47) |
| Q-05 | **Compression codec(s)** on-prem | Approach A layout (R-12) |
| Q-06 | On-prem **Spark / Hadoop / Hive / Java** versions; newer-stack cluster available? | VER-03, VER-09 (R-41) |
| Q-07 | **Consumers per table** (Athena / other v2-only readers?) → v2 vs v3; MoR mandate | DDL-04 (R-22, R-32, R-42) |
| Q-08 | **Scheduler / orchestrator** to integrate with | OPS-01 |
| Q-09 | Is **offline transfer** acceptable for cold history? | INI-08 |
| Q-10 | **CHAR(n) policy:** keep padding or trim? | TYP-05 |
| Q-11 | Any **Hive ACID / transactional** tables? | §3.1 |
| Q-12 | Any **SequenceFile** or other formats beyond §3.1? | §3.1 |
| Q-13 | **Primary / identifier keys** for Iceberg tables that take updates | INC-01, INC-03 |
| Q-14 | Prevalence of **partition evolution** in Iceberg sources | SRC-08 (R-19) |
| Q-15 | "**Land the table on-prem for initial load**": push or pull? | INI-06 (R-33) |
| Q-16 | **Data domains** → SP-per-domain mapping; who may run each | AUD-07 |
| Q-17 | **On-prem identity standard** for audit (Kerberos principal format, service accounts, IdP mapping) | AUD-01, AUD-02 |
| Q-18 | **Source snapshot retention** on-prem Iceberg tables | INC-11, INI-12 |
| Q-19 | Approval for a dedicated **`h2mi_ctl` HMS database + HDFS path** for utility state; **HMS version** (does it include HIVE-26882 for lock-free Iceberg commits?) | ARC-07 |

---

## Appendix A — Traceability to legacy R-IDs

From lab evidence (Appendix C).

| Legacy | New ID(s) | Note |
|---|---|---|
| R-01 | SCP-01 | |
| R-02 | SCP-01, SCP-04 | |
| R-03 | SCP-02 | |
| R-04 | SCP-03, INC-14, NFR-01, Q-03 | |
| R-05 | SCP-05 (non-goal) | |
| R-06, R-07, R-09 | §3.1, SRC-02 | |
| R-08, R-08a, R-08b | §3.1, SRC-05, SRC-09 | |
| R-11 | SRC-01, Q-01 | |
| R-12 | INI-10, Q-05 | |
| R-13 | SCP-12, INI-09 | |
| R-14, R-15, R-16 | SCP-13, INI-02, INC-05 | |
| R-17 | DDL-05 | Z-ORDER → liquid clustering |
| R-18 | SRC-01, INI-05, Q-01 | |
| R-19 | SRC-08, Q-14 | |
| R-20 | SCP-04 | |
| R-21, R-21a | SRC-05 | |
| R-22 | DDL-04, Q-07 | |
| R-23 | DDL-05 | |
| R-24 | INI-10 | |
| R-25 | ADF-01 | narrowed: partitioned Hive with path-only partition values is ineligible |
| R-26 | §9.1, INC-01, INC-02, INC-03 | |
| R-26a | SRC-09 | |
| R-27 | SRC-05, INC-05 | |
| R-28, R-29 | ADF-09, INI-02, test #1 / #8 | |
| R-30 | INC-08, INC-10 | watermark = control table + snapshot property |
| R-31 | §9.1, INC-04 | refined: MoR with delete files needs its own mode |
| R-32 | DDL-04, Q-07 | |
| R-33 | Q-15 | |
| R-34 | ARC-01, test #8 | |
| R-35 | SCP-10, SEC-04 | |
| R-36 | SCP-11, SEC-08 | |
| R-37 | SCP-05 | HMS federation = Approach C, non-goal |
| R-38 | INI-09, Q-02 | |
| R-39 | SCP-15 | |
| R-40, R-41 | VER-09, Q-06 | |
| R-42 | DDL-04 | |
| R-43 | INC-15 | **corrected:** external snapshot history is visible over IRC (tests README, 2026-08-06) |
| R-44, R-45 | INC-15 | ✅ mapping characterized |
| R-46, R-47 | DDL-07, Q-04 | |
| R-48 | ARC-01 | ✅ |
| R-49, R-50 | ARC-05 | |
| R-51 | — | demo, not a utility requirement |
| R-52, R-53, R-54, R-55 | SCP-05 (non-goal) | metadata rewrite = exit path; `rewrite_table_path` verified over IRC on Iceberg 1.11 ([FA] #5) |

---

## Appendix B — References

**Engagement documents** (internal, not included in this repository; cited by tag in the Evidence columns)
- `[MA]` Migration Approaches: Approaches A/B/C, the verified IRC configuration, the changelog → MERGE ordering
- `[QA]` Follow-up Questions & Test Results
- `[A13]` POC follow-up questions (August call)
- `[FA]` Follow-up Answers
- `[TM]` Hive → Databricks Type Mapping Reference

**Lab evidence**: Appendix C. **Legacy requirement IDs** `R-01`…`R-55`: Appendix A.

## Appendix C — Lab evidence

Every ✅ in this document comes from a hands-on test on an internal lab rig: an EC2 simulation of the on-prem side (HDFS,
YARN, Hive 3.1.3 metastore on Postgres, Spark 4.1 and 3.5, Iceberg 1.11 and 1.12) writing to a Unity Catalog sandbox
catalog over the Iceberg REST catalog and the SQL Statement API. The probe scripts live in that internal repository and are
not part of `h2mi`; the results they established are:

| Date | Versions | Result |
|---|---|---|
| 2026-08-06 | Iceberg 1.9 → UC | external snapshot history is visible over IRC; version ↔ snapshot mapping characterized (INC-15) |
| 2026-09-14 | OSS Spark → UC | every external write is a UC catalog commit; IRC `createTable` yields a `catalogManaged` table on a different commit path (ARC-05, DDL-13) |
| 2026-10 (various) | Spark 4.1 + Iceberg 1.12 | append read exact on append-only ranges, including insert-only MoR ranges; changelog → net per key → MERGE in sync (L0/L1/L2); the MERGE source must be materialized on Spark 4.1; keyless full-row MERGE fails on duplicates, row-value replace keeps the multiset exact; `create_changelog_view` rejects any range with live delete files; snapshot-diff → MERGE reconciles MoR sources; compaction does not unlock the changelog for earlier ranges |
| 2026-10-22 | Iceberg 1.11.0 and 1.12.0 | a 70-minute write commits only with `token-exchange-enabled=false`; `overwritePartitions()` on a `PARTITIONED BY` target carries snapshot tags, filter-`overwrite` on a `CLUSTER BY` target fails; MI v3 accepts merge-on-read write modes and external MERGE then writes deletion vectors; a partition replace shows in Databricks CDF as delete + insert of every row |
| 2026-10-30 | Spark 4.1 + Iceberg 1.12, MI format-version 2 | `writeTo().overwritePartitions()` and `.overwrite(<partition predicate>)` replace one partition; the V1 `mode("overwrite").saveAsTable` path is REPLACE TABLE AS SELECT over IRC and a MODIFY-only SP dropped the partition spec and history (ARC-09, R12); storage layout: hashed object-storage prefixes, `_delta_log` + `_iceberg` side by side (§7.5) |
| 2026-10-30 | same | `Dataset.observe()` fires on the Iceberg V2 append and equals a separate aggregation; the commit's `added-records` matches; the added files scoped via `_file` on-prem and `_metadata.file_path` on Databricks SQL return the same digest (VAL-12, VAL-13) |
| 2026-10-30 | Databricks SQL on a v3 `CLUSTER BY` managed table | `INSERT INTO … REPLACE WHERE` and `MERGE … WHEN NOT MATCHED BY SOURCE … DELETE` restate one slice atomically (INC-04, L-14) |

Type-mapping evidence (TYP-*, DT-*): Hive 3.1.3 `TINYINT`/`SMALLINT` are rejected by managed Iceberg at CREATE; zone-less
`TIMESTAMP` maps to `TIMESTAMP_NTZ`; `TIMESTAMP WITH LOCAL TIME ZONE`, `UNIONTYPE` and `INTERVAL` columns make Spark's
metastore read fail (`CANNOT_RECOGNIZE_HIVE_TYPE`) and need a materialized bridge copy; `VOID` cannot be CTAS'd in Hive 3.
SerDe evidence (SRC-*): Parquet / ORC / Avro / RCFile read through unchanged; text, CSV, JSON and Regex SerDes need a
reparse with explicit casts; Avro stores Hive `TINYINT` as `int`.

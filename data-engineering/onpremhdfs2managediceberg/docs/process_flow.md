# h2mi — Process Flow

**Status:** v0.1 · 2026-10-30 · companion to [`technical_design.md`](technical_design.md) v0.2 (section numbers below refer to it).

Six views of the same system: the per-table lifecycle, one sync cycle, how a batch is applied, the cycle state machine,
start-up reconciliation, and what crosses the network. Every arrow that crosses the network starts on-prem (D1); data moves
in one direction (D11).

---

## 1. Table lifecycle (onboarding → cutover gate)

```mermaid
flowchart TD
  A["Onboard: write tables/&lt;table&gt;.yaml<br/>(source, domain, keys, layout, locality column)"] --> B["h2mi inventory<br/>raw HMS types, partitions, files, Iceberg metadata (§8.1)"]
  B --> C["h2mi scan<br/>classify columns PASS / AUTO_MAP / REDESIGN / BLOCK, SerDe findings (§8.2)"]
  C -->|any REDESIGN or BLOCK| D["h2mi bridge<br/>emit Hive CTAS scripts (LTZ, INTERVAL, UNION)"]
  D --> E["the customer runs the bridge in Hive;<br/>table yaml points source at the materialized copy"]
  E --> B
  C -->|all PASS / AUTO_MAP| F["h2mi ddl plan<br/>generated DDL + diff vs live target: CREATE / NO-OP / ALTER / DRIFT (§8.3)"]
  F -->|DRIFT| X1["stop: human review"]
  F -->|CREATE / ALTER| G["h2mi ddl apply (ddl-admin SP)<br/>table, staging table, grants, tags, mirror tables"]
  F -->|NO-OP| H
  G --> H["h2mi load<br/>historical load at pinned S0, batched, one commit per batch (§8.4, §8.5)"]
  H --> I["validate: tier M per batch, L2-full once after the load (§8.8)"]
  I -->|mismatch| R["h2mi restore → re-plan (§8.10)"]
  R --> H
  I -->|VALIDATED / VERIFIED| J["scheduled h2mi sync<br/>incremental cycles until cutover (§8.6, §8.7)"]
  J --> J
  J -->|table frozen for cutover| K["cutover gate: L2-full on the frozen source vs target"]
  K -->|match| L["hand over to the cutover runbook (out of h2mi scope)"]
  K -->|mismatch| R
```

---

## 2. One sync cycle (sequence)

```mermaid
sequenceDiagram
  autonumber
  participant S as Scheduler (customer)
  participant D as h2mi driver (PySpark on YARN)
  participant H as HMS + HDFS (on-prem)
  participant ST as State store h2mi_ctl (HMS Iceberg) + HDFS leases
  participant IRC as UC Iceberg REST catalog
  participant SQL as Databricks SQL (Statement API)
  participant S3 as S3 (UC managed storage)

  S->>D: spark-submit h2mi sync --table X
  D->>ST: acquire lease (atomic HDFS create), exit 3 if held
  D->>ST: read v_cycle_state, v_watermark
  D->>IRC: loadTable(target), find the snapshot tagged h2mi.batch_id for any STARTED cycle
  Note over D,IRC: reconcile (§8.7): COMMITTED if the tag exists, FAILED if not, re-MERGE from staging if needed
  D->>SQL: spec guard: DESCRIBE target layout / spec / format-version == registry (DDL-16)
  D->>H: source schema at to_wm, drift check vs target (§8.9)
  D->>H: snapshot range or partition fingerprints since watermark
  D->>D: plan: mode (§8.4), apply method (INC-18 ladder), batches, executor budget
  loop per batch
    D->>ST: cycle_events STARTED (batch_id)
    D->>H: read source range / partitions, typed projection, observe() fingerprint
    alt append or partition replace
      D->>S3: data files (vended credentials)
      D->>IRC: commit with h2mi.* snapshot tags (writeTo append / overwritePartitions)
    else MERGE modes
      D->>S3: staging data files
      D->>IRC: staging append commit (tagged)
      D->>SQL: MERGE INTO target USING staging WHERE batch = :b AND locality range (D11)
    end
    D->>IRC: snapshot summary + added files (tier M metadata)
    D->>ST: COMMITTED, then VALIDATED when tier M matches the observation
  end
  D->>SQL: queue scoped L2 (added files / changed keys) → VERIFIED asynchronously
  D->>ST: run_events SUCCEEDED, spool → one append per control table
  D->>IRC: mirror new events to uc_catalog.h2mi_ctl (best effort)
  D->>ST: release lease
  D-->>S: exit code 0 / 2 / 3 / 4
```

---

## 3. Choosing how to apply a batch (planner, §8.4 + §8.6, INC-18)

```mermaid
flowchart TD
  A["range / partition with changes"] --> B{"source kind"}
  B -->|Hive| C{"only new files since<br/>last SYNCED fingerprint?"}
  C -->|yes| C1["file-delta append<br/>(read just those files)"]
  C -->|no, files rewritten| D{"keys?"}
  B -->|Iceberg| E{"all snapshots in range<br/>append / replace?"}
  E -->|yes| E1["iceberg-append<br/>incremental read → append"]
  E -->|no| F{"live delete files<br/>in the range?"}
  F -->|no, keys set| G["iceberg-upsert<br/>changelog → net per key"]
  F -->|yes, or no keys| H["iceberg-mor<br/>snapshot-diff over touched partitions"]
  G --> M
  H --> D
  D -->|keyed| M{"target layout"}
  M -->|CLUSTER BY| M1["stage + MERGE"]
  M -->|PARTITIONED BY| N{"change density ≥ threshold<br/>or partition small?"}
  N -->|yes| P["overwritePartitions()"]
  N -->|no| M1
  D -->|keyless| Q{"dense or small?"}
  Q -->|yes| P2{"target layout"}
  P2 -->|PARTITIONED BY| P
  P2 -->|CLUSTER BY| P3["stage + Databricks REPLACE WHERE"]
  Q -->|no| R{"source kind"}
  R -->|Iceberg| R1["row-value replace<br/>exact rows from the local snapshot-diff"]
  R -->|Hive| R2["bucketed hash-diff<br/>4096 digests both sides → changed rows → row-value replace"]
  M1 --> X{"executor"}
  R1 --> X
  R2 --> X
  P3 --> X
  X -->|"estimated download ≤ budget<br/>and merge_executor allows"| X1["on-prem MERGE over IRC"]
  X -->|otherwise| X2["Databricks SQL via Statement API<br/>(default, D11)"]
```

---

## 4. Cycle state machine (§8.7)

```mermaid
stateDiagram-v2
  [*] --> PLANNED
  PLANNED --> STARTED: lease held, cycle_id assigned, batch tagged
  STARTED --> COMMITTED: target commit with h2mi.batch_id found
  STARTED --> FAILED: error before commit (no tag in target)
  COMMITTED --> VALIDATED: tier M (commit metadata) matches the observation
  COMMITTED --> FAILED: tier M mismatch
  VALIDATED --> VERIFIED: scoped L2 matches (asynchronous)
  VALIDATED --> FAILED: scoped L2 mismatch (asynchronous)
  FAILED --> RESTORED: RESTORE TABLE to last VERIFIED target_version
  RESTORED --> PLANNED: re-plan from the last watermark
  FAILED --> PLANNED: pre-commit failure, same range retried
  PLANNED --> BLOCKED: scan finding, breaking drift, spec guard (table replaced)
  VERIFIED --> [*]
```

The watermark is the `to_wm` of the highest **VALIDATED** cycle (tier M gates it); `VERIFIED` is the RESTORE point when an
asynchronous L2 later fails.

---

## 5. Start-up reconciliation (every `load` / `sync`, under the lease)

```mermaid
flowchart TD
  A["read latest cycle for the table"] --> B{"status"}
  B -->|STARTED| C["look for a target (or staging) snapshot<br/>with summary h2mi.batch_id = cycle.batch_id"]
  C -->|found| D{"mode"}
  D -->|upsert / mor| E["re-run MERGE from staging (idempotent)"]
  D -->|append / partition| F["emit COMMITTED(snapshot)"]
  E --> F
  C -->|not found| G["emit FAILED(no_commit); same range is re-planned"]
  B -->|COMMITTED| H["run tier M validation → VALIDATED or FAILED"]
  B -->|FAILED after commit| I{"auto_restore?"}
  I -->|yes| J["RESTORE to last VERIFIED version → RESTORED"]
  I -->|no| K["BLOCKED + alert"]
  B -->|VALIDATED / VERIFIED / RESTORED| L
  F --> H
  H --> L["spec guard (DDL-16) → drift check (§8.9) → plan next cycle from v_watermark"]
  G --> L
  J --> L
```

---

## 6. What crosses the network (D1, D11)

```mermaid
flowchart LR
  subgraph ONPREM["On-prem (Kerberos boundary)"]
    DRV["h2mi driver + executors"]
    HMS[("HMS + HDFS<br/>sources, h2mi_ctl, leases")]
  end
  subgraph DBX["Databricks"]
    IRC["UC IRC"]
    SQL["SQL warehouse"]
    MIR[("h2mi_ctl mirror")]
  end
  subgraph AWS["customer AWS"]
    S3[("S3 managed storage")]
  end
  DRV <--> HMS
  DRV -- "OAuth token, loadTable, commits<br/>metadata.json + manifests (small, down)" --> IRC
  DRV -- "data + staging files<br/>(UP only, vended credentials)" --> S3
  DRV -- "DDL, MERGE, REPLACE WHERE,<br/>scoped L2, RESTORE (statements up; digests down)" --> SQL
  DRV -- "events (UP)" --> MIR
  DRV -. "on-prem MERGE: candidate files DOWN<br/>only when under the planner budget" .-> S3
```

Everything solid moves metadata or data **up**. The single dotted path is the only data download, and the planner must
estimate it under `onprem_merge_budget_bytes` before allowing it (§8.4). `expected_download_bytes` is recorded per batch.

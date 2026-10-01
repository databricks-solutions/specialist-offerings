# TCO App — TODO / Implementation Backlog

Running list of improvements to implement. Newest feedback at top of each section.

## UX / Feedback (reported 2026-09-30)

- [ ] **Action feedback is stale/ambiguous — "Initialize TCO Tables".**
  After entering catalog + schema and clicking *Initialize TCO Tables*, the page
  just shows a static `TCO tables ready.` — you can't tell whether it reflects
  *this* click or a previous run. No spinner, no timestamp, no change on re-click.
  - Fix: show a spinner while the call runs; on completion show a **timestamped,
    scoped** result, e.g. `Initialized profiler.visa_dpi at 14:15:30 — 10 tables
    created, 4 seeded`; clear/replace the message when catalog or schema changes
    so a stale "ready" never lingers.

- [ ] **"Save Assumptions" gives no clear confirmation; replace dropdown with a
  live panel.** After *Save Assumptions* you can't tell if it saved, and the
  dropdown doesn't visibly update.
  - Fix: success toast naming the saved set (name + id + timestamp); **replace/
    augment the dropdown with a panel/table** that lists saved assumption sets
    and *fills up as you add them* — show name + key params + created_at, make
    rows selectable, and add edit/delete. Dropdown alone hides the result.

- [ ] **After "Initialize TCO Tables" succeeds, auto-advance to the Workload
  Profile tab and load it.** Instead of leaving the user on a static
  `TCO tables ready.` message, navigate to the Workload Profile tab and trigger
  its load for the just-initialized catalog/schema — so the result is immediately
  visible and the next step is obvious. (Ties into the stale-feedback fix above.)

- [ ] **Apply consistent action feedback across ALL tabs.** Same ambiguity shows
  up elsewhere (e.g. Run TCO, pricing refresh).
  - Fix: standardize one pattern app-wide — disable the button + show a spinner
    during async ops; on success show a timestamped, context-bearing message
    (what acted on what); on error show the message inline; clear stale status
    when inputs change. Consider a small shared `status_banner` component.

## Known enhancements (from the sheet/profiler analysis)

- [ ] **Capacity-based DBU mode** (so the app can reproduce the sheet's $13.5M).
  Add `dbu_method: measured | capacity` toggle in `cost_engine.py`; capacity =
  nodes × vCores/node × %split × 24/7 util × (1−perf_gain) ÷ hyperthreading.
  Keep measured mode as the actual-workload cross-check.
- [ ] **T-Shirt sizing — only cost is modeled.** Add per-size **timeline**
  (6/8/11/24 mo) and **support FTEs** (3/5/7/9), and **auto-map node-count→size**
  (0-50 S / 50-150 M / 150-500 L / 500+ Custom) instead of defaulting to "medium".
- [ ] **HDP/Ambari + Ranger support** in the DuckDB path (new loaders + compat
  views + HDP dashboard datasets); wire Ambari HDFS stats → storage input so
  Ambari clusters (e.g. Visa DPI) auto-populate storage. See the (a) scoping plan.

## Canonical "why app ≠ Visa sheet" reconciliation (2026-09-30)

Ran the app on `profiler.visa_dpi` with the sheet's assumptions. **Hadoop matches
to the dollar ($8,377,028 ≈ sheet $8,377,031).** Databricks total looks close
($14.10M app vs $13.54M sheet, ~4%) **but that is a coincidence** — two ~$8–10M
differences nearly cancel. Line-by-line (app vs sheet):

| Component | Sheet | App (visa_dpi) | Diff |
|---|---|---|---|
| Storage | **$9,798,600** (34,000 TB @ ~$0.026/GB/mo) | **$0** | **−$9.80M** |
| DBU | $1,997,566 (capacity: 6% util × 75% perf) | $10,337,113 (measured workload) | **+$8.34M** |
| DBU Support (25%) | $499,392 | $2,584,278 | +$2.08M |
| VM Compute (Interactive/BI serverless → $0) | $119,623 | $51,605 | −$68K |
| Admin | $1,125,000 | $1,125,000 | $0 |
| **Total** | **$13,540,181** | **$14,097,997** | **+$0.56M** |

Two root causes, each masking the other:
1. **Storage $9.8M → $0**: Visa DPI is Ambari-managed; HDFS stats never reach the
   DuckDB path. Fixed by the **HDP/Ambari port** above.
2. **DBU $2.0M → $10.3M**: measured-workload DBUs vs the sheet's capacity formula.
   Fixed by the **capacity-based DBU mode** above.

To actually reproduce the sheet you need **both** fixes; today they offset and the
~4% total match is misleading. (Also: the app run used `hyperthreading_factor=2.0`
and `dev_test_uplift=0.2`, but the sheet uses 1 and 0.10 — a smaller secondary
source of drift on the DBU side.) The sheet's "Hardware/Cloud Compute & Storage"
line is ~99% storage, not EC2 — Interactive/BI run serverless so VM compute is tiny.

- [ ] **Kill the manual "Initialize TCO Tables" step — auto-provision tables.**
  The user should never click an init button, and certainly not feel like they
  must re-init per tab. The app should **ensure the tco_* tables exist
  automatically** (idempotent `CREATE TABLE IF NOT EXISTS` + seed-if-empty) the
  first time a catalog/schema is used — on app load for the configured schema,
  and lazily whenever the selected schema changes. Keep an explicit
  re-init/repair action only as an advanced/hidden option.
  - Note: tco_* tables are **per catalog.schema**, so switching to a new schema
    (e.g. visa_dpi_mar) currently requires a one-time init; that's exactly what
    should happen transparently instead of via a button.
  - This largely goes away once state moves to Lakebase (single managed DB,
    schema created once) — see Architecture & State below.

## Architecture & State (reported 2026-09-30)

- [ ] **List the assumptions.** Add a dedicated view/panel that enumerates all
  saved assumption sets with their key values (nodes, vCores, splits, cloud,
  tier, discounts, migration…), not just a name in a dropdown. Should let you
  see, pick, duplicate, edit, and delete sets. (Pairs with the "Save Assumptions"
  feedback item above — the panel is how you confirm a save landed.)

- [ ] **Standardize on an official Databricks app template. → DECISION
  (2026-09-30): adopt AppKit (React/TS), rewrite modeled on "Vacation Rentals Ops
  Console."** This rewrite ABSORBS UX items 1–5 and 9 (standardized feedback/nav/
  panels come natively) and item 11 (Lakebase is the native state layer); item 5
  is solved by construction. Items 6/7 get built into the new TS cost engine; item
  8 stays a Python pipeline (independent). Full phased plan tracked separately.
  Open sub-decision: cost engine in TS/Node vs a kept Python compute service
  (reco: port to TS — it's simple arithmetic; DuckDB pipeline stays Python).
  --- original note ---
  The current UI is ad-hoc (hand-rolled Dash layout, inconsistent feedback/nav).
  Reviewed https://developers.databricks.com/templates (2026-09-30): ~32 templates,
  **all built on "AppKit" (React/TypeScript + Node, Lakebase-backed)** — there is
  **no Dash/Python-UI template**. So this is a fork:
  - **Option A — adopt AppKit (React/TS):** the modern standard; bundles
    standardized UI/nav/auth + resource wiring **and Lakebase** out of the box, so
    it subsumes most of the UX/Feedback backlog AND the Lakebase statefulness item
    in one move. Cost: a **rewrite** from Dash/Python → TypeScript/React.
  - **Option B — stay on Dash:** these templates don't apply; standardize via the
    `databricks-app-python` skill's Dash conventions. Lower effort, no "official
    template" alignment.
  - **Recommendation (if rewriting is on the table):** model the app on the
    **"Vacation Rentals Ops Console"** template — AppKit + React dashboard that
    does **SQL Warehouse queries + Lakebase persistence + Genie**, the same shape
    as TCO (query profiler tables via the warehouse, persist assumptions/runs in
    Lakebase). Scaffold with **"Spin Up Databricks App"** (`databricks apps init`)
    and add the state layer via **"App with Lakebase" / "Lakebase Data
    Persistence"**. Secondary refs: Operational Data Analytics, Genie
    Conversational Analytics.
  - If no near-term rewrite: do Option B now, revisit AppKit when the Lakebase
    migration happens (same effort — combine them).

- [ ] **Make the app stateful — back it with Lakebase (managed Postgres).**
  Today the app's state (tco_assumptions / tco_runs / tco_run_details /
  tco_migration_timeline + lookups) lives in UC **Delta** managed tables written
  via the SQL warehouse — not an OLTP store, slow for interactive writes, and
  fragile (the whole schema was wiped once, taking all runs/assumptions with it).
  Move app state to a **Lakebase** (Databricks managed Postgres) instance so the
  app has proper transactional, durable, low-latency state and survives
  catalog/storage changes. Keep the profiler analysis tables (yarn/cm/etc.) in UC
  Delta; only the app's own read/write state goes to Lakebase.
  - Note: user has/will provision a Lakebase instance to serve the app.
  - Implementation refs: `lakebase-provisioned` / `lakebase-autoscale` skills;
    `db_connector.py` would gain a Postgres path alongside the DBSQL one.

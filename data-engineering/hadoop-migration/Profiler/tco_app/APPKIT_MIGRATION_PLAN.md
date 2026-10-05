# TCO App → AppKit Migration Plan

> 📋 **Working document — migration plan/progress log, not user reference.** The
> AppKit rewrite shipped: the current app is [`../tco-app-appkit/`](../tco-app-appkit/README.md).
> To use the TCO app, see its README. This file tracks the plan and may lag the code.

**Decision (2026-09-30):** Adopt **AppKit (React/TS)** for the TCO Calculator app,
rewrite modeled on the **"Vacation Rentals Ops Console"** template
(https://developers.databricks.com/templates — SQL Warehouse queries + Lakebase
persistence + Genie, the same shape as TCO). This supersedes the hand-rolled
Dash app (`hadoop-tco-calculator`).

This file is the resumable plan. The task backlog lives in `TODO.md`; the
app-vs-sheet reconciliation rationale is also in `TODO.md`.

## Cost-engine decision — RESOLVED (2026-10-01)
**Cost engine will be ported to TypeScript/Node, in-app** (one stack, no
cross-language service). `cost_engine.py`'s logic is reimplemented in TS during
Phase 2 (SA-B), including capacity-DBU (#6) and t-shirt sizing (#7).
The **DuckDB exporter + Ambari/Ranger port (item 8) stay Python** — separate data
pipeline, not in the app request path.

## How the 11 TODO items collapse under AppKit

| # | Item | Fate |
|---|---|---|
| 10 | Standardize on template | = THE REWRITE (AppKit, Vacation Rentals Ops Console) |
| 11 | Lakebase state | Native state layer — built in from day one |
| 1 | Init feedback | Absorbed → standardized UI feedback |
| 2 | Save-assumptions confirm | Absorbed → Assumptions page |
| 3 | Auto-advance to Workload Profile | Absorbed → nav/flow |
| 4 | Consistent feedback all tabs | Delivered by template conventions |
| 5 | Kill manual init | Solved by construction (one Lakebase DB, set up once) |
| 9 | List assumptions panel | Built into Assumptions page |
| 6 | Capacity DBU mode | Built into the new TS cost engine from the start |
| 7 | T-shirt sizing | Built into the new TS cost engine from the start |
| 8 | Ambari/Ranger port | STAYS Python pipeline — independent parallel track |

## Execution: parallel vs sequential

### Track P — independent, parallel, start NOW (worktree)
- **SA-A: Item 8 — Ambari/Ranger DuckDB port.** New loaders
  (`duckdb_exporter/loaders/ambari_loader.py`, `ranger_loader.py`), `schema.py`
  tables, compat views in `load_profiler_to_delta.py`, HDP dashboard datasets,
  and wire Ambari HDFS stats → storage input. Pure Python; feeds storage into the
  UC tables the new app reads. No dependency on the rewrite. Reference:
  `~/code/hdp_analysis/notebooks/Includes/6.Ambari Analysis.py` and `5.Ranger Analysis.py`.

### Rewrite program
| Phase | Mode | Work |
|---|---|---|
| 0 — Scaffold | sequential (foundation) | **Provision a Lakebase instance** (on `aws_sandbox`); `databricks apps init` modeled on Vacation Rentals Ops Console; wire Lakebase + SQL Warehouse resources + OAuth; base layout/nav/theme; CI/deploy; deploy hello-world to workspace. |
| 1 — State + data | sequential (after scaffold) | Lakebase schema + CRUD for assumptions/runs/run_details/migration_timeline (item 11); migrate seed lookups (SKU map, VM/DBSQL/storage); warehouse query layer to profiler tables. |
| 2 — Parallel build (worktrees) | 2 agents parallel | SA-B: TS cost engine (port `cost_engine.py` + build in 6 capacity-DBU + 7 t-shirt). SA-C: UI pages (nav/theme/feedback skeleton = 1/3/4; pages vs mocked data). |
| 3 — Integration | sequential | Wire UI↔engine; Assumptions list panel + save feedback (2, 9); embed Lakeview dashboard; parity test vs current app AND the Visa sheet; cut over. |

True parallelism: SA-A runs throughout; rewrite fan-out starts in Phase 2
(engine ‖ UI) once scaffold + state exist.

## Milestones
- **M1** (REVISED): Item 8 code is DONE + tested (42/42) in worktree
  `agent-ac6dd954d214e3ffc` — Ambari/Ranger loaders, 9 base tables + camelCase
  compat views, validate patterns, `hdfs_stats` with capacity_total_tb.
  **BUT the Visa DPI extract's Ambari/Ranger files are all HTTP errors**
  (404/500, `clusterName=anahprod` mismatch), so storage will NOT auto-populate
  from DPI_Output. The sheet's 34,000 TB is a MANUAL input anyway (Inputs tab
  note: "Default to 4.8 TB/Node"). → Real fix: **make storage a manual assumption
  input** in the app (like the sheet), with Ambari auto-fill only when data is
  valid. Item 8 worktree is ready to merge (additive; doesn't touch tco_app).
- **M2:** AppKit scaffold deployed on the workspace (resources + auth working).
- **M3:** State + TS engine → capacity-mode TCO reproduces the sheet; with M1 the Visa reconciliation closes end-to-end (storage ≈ $9.8M, DBU ≈ $2M).
- **M4:** UI parity with the Dash app → decommission Dash.

## Guardrail
Keep the current Dash app (`hadoop-tco-calculator`, aws_sandbox) running until the
AppKit app reaches parity (M4). The DuckDB→UC pipeline (incl. item 8) serves both.

## Reference context (for a cold restart)
- Cost model source of truth: the Google Sheet "Visa DPI Hadoop Migration Profile"
  (id `1MRyQ68Eoan7UWPWDt0AyPXKqzAAxbdKv_hLBY6N036k`), READ-ONLY.
- Capacity DBU formula (item 6): `nodes × vCores/node × %split × 24/7 util ×
  (1−perf_gain) ÷ hyperthreading`, priced per SKU; reproduces the sheet's ~$2M DBU.
- T-shirt sizing (item 7): 0-50 S / 50-150 M / 150-500 L / 500+ Custom →
  cost 500k/850k/1.75M/custom, timeline 6/8/11/24 mo, FTEs 3/5/7/9.
- UC data: catalog `profiler`, schemas `demo` (CDH sample), `visa_dpi` (Mar-19
  base, has node dump), `visa_dpi_mar` (Mar29-Apr02, 11-day workload). Warehouse
  `80bb9cfd8c6e5b05` on Databricks profile `aws_sandbox`.
- Current app URL: https://hadoop-tco-calculator-7474658366043447.aws.databricksapps.com

## Provisioned infra (2026-10-01)
- **Lakebase Autoscaling project**: `projects/hadoop-tco` (uid 60010b65-b006-438e-bea6-2badc039c312)
  on Databricks profile `aws_sandbox`. Default branch `projects/hadoop-tco/branches/production`,
  DB `databricks_postgres`, PG 17, compute 1 CU, suspend 86400s (TODO: shorten for cost).
  Connect via `databricks postgres generate-database-credential` (OAuth, 1h) + the branch endpoint host.
  (Fetch endpoint id via `databricks postgres list-endpoints projects/hadoop-tco/branches/production`.)

## Status / next action
- [x] Cost-engine sub-decision — **TS/Node, in-app** (confirmed 2026-10-01).
- [x] Lakebase flavor — **Option A: keep Autoscaling (`hadoop-tco`), wire manually**
      in server/ via SDK token flow (scale-to-zero; not the native analytics.database plugin).
- [x] Item 8 MERGED to feature/hadoop-migration (commit 180f9ab, 42/42 tests); worktree pruned.
- [x] SA-A (item 8) launched as a worktree sub-agent (running).
- [x] Lakebase Autoscaling project `hadoop-tco` provisioned.
- [x] CLI upgraded to v1.19.0 (`databricks apps init` now available; non-interactive mode works).
- [x] **Scaffold DONE** — `Profiler/tco-app-appkit/` (AppKit, React+Express+TS, 42 files),
      analytics plugin + warehouse `80bb9cfd8c6e5b05` wired via `sql-warehouse` resource.
      Created with: `databricks apps init --name tco-app-appkit --features=analytics
      --set analytics.sql-warehouse.id=80bb9cfd8c6e5b05 -p aws_sandbox`. Dash `tco_app/` untouched.
      Structure: client/ (React/Tailwind), server/ (Express server.ts), shared/,
      config/queries + config/metric-views (analytics SQL), app.yaml, databricks.yml (DAB),
      appkit.plugins.json, CLAUDE.md. Not yet deployed.
- [ ] (superseded) ~~Scaffold AppKit app — needs CLI upgrade + interactive init~~:
      prereqs OK (Node v22.20, git, npm 11.6). CLI v0.299.0 lacks `apps init`.
      Upgrade is gated: run `brew trust databricks/tap && brew upgrade databricks`
      (official Databricks tap). Then interactive `databricks apps init` (prompts
      for name + plugins). AppKit = Node/TS. Scaffold recipe:
      developers.databricks.com/templates/spin-up-databricks-app and /vacation-rentals.
      Target a NEW dir `Profiler/tco_app_appkit/`; leave Dash `tco_app/` live.
      Select plugins: Lakebase + analytics/SQL-warehouse (Genie/model-serving optional).
      Ends with `databricks apps deploy`. After scaffold, resume Phase 1 here.
- [~] Phase 1 IN PROGRESS. Foundation DONE:
      - Re-scaffolded `tco-app-appkit` with **analytics + lakebase** plugins (44 files).
        AppKit has a NATIVE `lakebase` plugin for Autoscaling (not manual wiring) —
        `server.ts` has `lakebase()`, app.yaml `LAKEBASE_ENDPOINT valueFrom: postgres`,
        databricks.yml binds the postgres resource, .env has real host (us-east-1).
      - Connection pattern (from scaffold's sample `server/routes/lakebase/todo-routes.ts`):
        `appkit.lakebase.query(sql, params)` — pooled, OAuth auto-refresh; setup via
        CREATE SCHEMA/TABLE IF NOT EXISTS in `onPluginsReady`; zod-validated Express routes;
        raw SQL (no ORM). Postgres schema namespace `app` (I'll use `tco`).
      - This auto-setup pattern SOLVES TODO #5 (no manual "Initialize TCO Tables" button).
      Phase 1 progress:
      - [x] **TCO Postgres schema + idempotent setup** — `server/db/schema.ts`:
            schema `tco` with tables assumptions / runs / run_details / migration_timeline /
            workload_sku_mapping / pricing_snapshot / lookup_vm_instances / lookup_dbsql_sizes /
            lookup_storage_tiers / vm_price_history. Base + V2 columns folded into greenfield
            CREATEs; seeds (10 SKU maps, 4 baseline assumptions, 10 VM instances, 27 DBSQL sizes,
            14 storage tiers) ported from seed_data.sql/seed_lookups.sql, seeded only when empty.
            `setupTcoSchema()` runs on onPluginsReady → **auto-provision (kills TODO #5)**.
      - [x] **CRUD + reference routes** — `server/routes/tco/`: assumptions (list/get/create/
            update/delete, zod-validated), reference (lookups read, SKU-mapping read+upsert,
            runs read with details+timeline). Wired in server.ts; sample todo-route removed.
      - [x] Typecheck clean; my code lints clean (10 lint errs are in auto-generated
            shared/appkit-types/* — regenerate with the query layer); `appkit doctor` auth OK.
      - [x] Runtime validation DONE (`npm run dev` on :8000, profile aws_sandbox):
            Lakebase pool initialized, `[tco] schema ready — provisioned + seeded`, 17 routes
            registered. Verified live: GET assumptions=4 seeded, sku-mapping=10, AWS VM=4;
            full CRUD (POST 643/79 nodes persisted → GET → DELETE 204 → back to 4).
            Auto-provision confirmed working → TODO #5 truly closed.
      - [x] **Warehouse query layer DONE** — `config/queries/`: profiler_catalogs,
            profiler_schemas, workload_by_type, observation_window, peak_workload,
            cluster_hosts. Dynamic `profiler.<schema>` via `IDENTIFIER(:catalog||'.'||:database||'.t')`
            with `-- @param ... = profiler/demo` sample values for typegen. All 6 validated
            live against profiler.demo (IDENTIFIER+bound-params pattern confirmed); typecheck clean.
            NOTE: `appkit generate-types` ran OFFLINE (warehouse-connect hiccup in that subprocess)
            so query types are degraded — re-run `DATABRICKS_CONFIG_PROFILE=aws_sandbox npm run typegen`
            when the warehouse is warm to get precise types (non-blocking).
      ===> PHASE 1 COMPLETE. <===

- [~] Phase 2 IN PROGRESS — TS cost engine (server/engine/) + UI.
      Engine core DONE + tested (11/11 vitest pass, typecheck clean):
      - [x] `shared/tco-types.ts` — Assumptions / HadoopCosts / TcoResult / CalculateRequest
            contract (adds `dbu_method: measured|capacity`).
      - [x] `server/engine/constants.ts` — ported constants.py + **T-shirt table with
            per-size timeline(6/8/11/24)/FTE(3/5/7/9) + `tshirtForNodeCount()` auto-map (#7)**.
      - [x] `server/engine/hadoop.ts` — faithful port; **reconciles to the sheet** (DC
            $3.215M + admin $3.75M exact, total within $10 of $8,377,031).
      - [x] `server/engine/migration.ts` — timeline + do-nothing + summary; T-shirt-driven
            duration/cost (#7); `resolveTshirt()`.
      - [x] `server/engine/engine.test.ts` — Hadoop reconciliation + T-shirt + timeline tests.
      - [x] `server/engine/databricks.ts` (+ databricks.test.ts) — **measured DBU** faithful
            port (window×util×overhead×(1+dev)×(1−perf)×annualization×price); VM + tiered storage
            (with **manual-TB override** for the Ambari storage gap); support + **dbx admin
            (matches sheet $1,125,000)**; **capacity DBU #6 SCAFFOLDED but PROVISIONAL** (needs
            sheet Run-Rate formula calibration — CSV gives values not formulas). 20/20 tests pass.
      REMAINING engine:
      - [ ] Capacity-DBU (#6) calibration — extract the sheet's exact per-stream vCPU→DBU
            chain (formula layer, not CSV values) and tune `computeCapacityDbu`.
      - [ ] Pricing snapshot (optional) — live system.billing.list_prices instead of SKU_DBU_RATE.
      - [x] **Orchestrator `cost-engine.ts` + POST /api/tco/calculate — DONE + VALIDATED
            end-to-end.** Server-side warehouse queries via `appkit.analytics.query(sql, params)`
            (result shape `{data:[...rows]}`, numeric cells are strings). Live run on
            `profiler.visa_dpi_mar`: hadoop $8,377,028 ✓, window 26.8d/annualize 13.6 (not floored),
            measured DBU Other→interactive $16.45M, VM $114k, support $4.11M, admin $1.125M,
            total dbx $21.8M; run persisted to tco.runs/run_details/migration_timeline. Typecheck
            + 20 tests green. (`dbu_method` defaults 'measured'; capacity path runs but see below.)
      REMAINING:
      - [x] **Capacity-DBU #6 DONE** — extracted the sheet's Run-Rate Calculations formula layer
            (via XLSX export + openpyxl) and ported the exact cluster chain into
            `computeCapacityDbu`: nodes×vCores×split×util×(1+devtest)×(1−perf) → clusters
            (6 workers+1 driver, 8 vCPU/worker) → ×8760×7×$DBU/node-hr. **ETL reconciles
            EXACTLY ($141,594)**; interactive non-serverless $716,109 (= sheet S74+S80).
            `dbu_method` wired through: tco.assumptions column (+ idempotent ADD COLUMN
            migration), zod enum, UI select; orchestrator branches measured|capacity.
            Live-validated: capacity DBU total $1.58M (vs sheet $2.0M; measured was $16M).
            Documented refinement: Visa's Interactive/BI run SERVERLESS (≈0.53 GC-benchmark
            ratio / DBSQL path) — capacity mode returns the non-serverless upper bound.
      - [ ] Pricing snapshot (optional).
      - [ ] (housekeeping) 2 stale empty runs in tco.runs from pre-fix probes — harmless.
      - [~] UI (React pages) — STARTED + validated in browser:
            - client/src/lib/api.ts (typed /api/tco client) + lib/catalog.tsx (catalog/schema
              context, localStorage-persisted, header selector).
            - CalculatorPage: assumption picker + Calculate → renders Hadoop/Databricks breakdown,
              savings, per-workload table; timestamped success (UX #1/#4), low-confidence window warning.
            - AssumptionsPage: live list panel (#9) + create/edit/delete + save confirmation (#2).
            - App.tsx rewired (nav + routes + header selector); sample pages removed. Typecheck clean.
            - Browser-validated on :8000: calculated profiler.visa_dpi → hadoop $8,377,028,
              databricks $13.02M (close to sheet $13.54M for this window); Assumptions "Saved sets (5)".
            - [x] WorkloadProfilePage (useAnalyticsQuery on the config queries — cluster/window
                  stats + workload-by-type). Browser-validated on visa_dpi (1,325 apps, 5 days).
            - [x] PricingSkuPage — editable SKU mapping (the DBU-allocation lever) + VM/DBSQL
                  lookups by cloud. Browser-validated (23 rows render).
            - [x] MigrationTimelinePage — run picker → 3-yr summary + quarterly ramp table (/api/tco/runs/:id).
            - [x] ScenarioComparisonPage — all-runs comparison table (hadoop/databricks/savings).
            ===> UI COMPLETE (6 pages: Calculator, Workload, Pricing&SKU, Migration, Scenarios,
                 Assumptions). Full production build passes (vite + tsdown). <===


## ===> M4 DEPLOYED (2026-10-01) <===
AppKit app **tco-app-appkit** is LIVE on Databricks Apps (aws_sandbox):
  https://tco-app-appkit-7474658366043447.aws.databricksapps.com  (status: RUNNING)
Deploy = `databricks apps deploy` (bundle: validate build/typecheck/lint -> upload -> build -> start).
Two deploy blockers fixed:
  1) App service principal (6be4ba48-...) lacked UC access -> GRANT USE CATALOG/USE SCHEMA/SELECT
     ON CATALOG profiler TO the SP (needed for typegen DESCRIBE at build + runtime queries).
  2) Repo-root .gitignore '*conf*.json' matched ts*conf*ig*.json, excluding tsconfigs from the
     bundle upload (Apps build TS5083). Fixed via sync.include in databricks.yml (+ committed tsconfigs).
Runtime confirmed: Lakebase SP pool initialized, server running (production). Dash app
`hadoop-tco-calculator` left running (M4 guardrail) pending parity sign-off.
Remaining (optional): capacity serverless-ratio refinement; retire Dash after sign-off; shorten
Lakebase scale-to-zero; cleanup stale runs.

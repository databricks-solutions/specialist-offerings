#!/usr/bin/env bash
#
# restore.sh — rebuild the profiler Delta tables after the sandbox clears them.
#
# The shared sandbox periodically wipes managed storage (drops the profiler
# schemas and their Delta data, and empties the upload volume). This script
# makes recovery one command:
#   1. re-upload the local DuckDB exports to the UC volume, then
#   2. run the saved "Restore Profiler Delta (visa)" job (DuckDB -> Delta), then
#   3. verify row counts.
#
# The DuckDB exports live locally (gitignored, confidential) under
# input_json_dont_check_in/<schema>/<schema>.duckdb and are the source of truth.
#
# Usage:   ./restore.sh
# Config:  override via env, e.g. DATABRICKS_PROFILE=aws_sandbox ./restore.sh
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-aws_sandbox}"
WAREHOUSE_ID="${WAREHOUSE_ID:-80bb9cfd8c6e5b05}"
VOL="/Volumes/profiler/demo/profiler_uploads"
JOB_NAME="Restore Profiler Delta (visa)"
SCHEMAS=(visa_dpi_mar visa_dpi)

HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="$HERE/input_json_dont_check_in"

echo "==> 1/3 uploading DuckDB exports to $VOL"
databricks fs mkdir "dbfs:$VOL" -p "$PROFILE" >/dev/null 2>&1 || true
for s in "${SCHEMAS[@]}"; do
  f="$SRC/$s/$s.duckdb"
  [ -f "$f" ] || { echo "   MISSING local export: $f" >&2; exit 1; }
  databricks fs cp "$f" "dbfs:$VOL/$s.duckdb" -p "$PROFILE" --overwrite
  echo "   uploaded $s.duckdb"
done

echo "==> 2/3 running job '$JOB_NAME'"
JOB_ID="$(databricks jobs list -p "$PROFILE" -o json \
  | python3 -c "import sys,json
d=json.load(sys.stdin)
jobs=d.get('jobs',d) if isinstance(d,dict) else d
print(next((j['job_id'] for j in jobs if j.get('settings',{}).get('name')=='$JOB_NAME'),''))")"
[ -n "$JOB_ID" ] || { echo "   job '$JOB_NAME' not found — create it (see RUNBOOK.md)" >&2; exit 1; }

RUN_ID="$(databricks jobs run-now "$JOB_ID" -p "$PROFILE" --no-wait -o json \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('run_id',''))")"
echo "   job_id=$JOB_ID run_id=$RUN_ID — waiting..."
while true; do
  STATE="$(databricks jobs get-run "$RUN_ID" -p "$PROFILE" -o json \
    | python3 -c "import sys,json;s=json.load(sys.stdin).get('state',{});print(s.get('life_cycle_state','?')+'/'+(s.get('result_state') or '-'))")"
  echo "   $STATE"
  case "$STATE" in
    TERMINATED/SUCCESS) break ;;
    TERMINATED/*|INTERNAL_ERROR/*|SKIPPED/*) echo "   restore FAILED: $STATE" >&2; exit 1 ;;
  esac
  sleep 30
done

echo "==> 3/3 verifying row counts"
for s in "${SCHEMAS[@]}"; do
  databricks api post /api/2.0/sql/statements -p "$PROFILE" \
    --json "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"wait_timeout\":\"50s\",\"statement\":\"SELECT '$s' AS schema, COUNT(*) AS yarn_apps FROM profiler.$s.yarn_applications\"}" \
    | python3 -c "import sys,json;r=json.load(sys.stdin).get('result',{}).get('data_array') or [['?','?']];print('   ',r[0][0],'yarn_applications =',r[0][1])"
done
echo "==> restore complete."

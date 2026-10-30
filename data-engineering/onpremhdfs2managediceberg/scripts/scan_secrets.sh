#!/usr/bin/env bash
# Pre-share gate: fail if the tree contains keys, tokens or environment identifiers.
#
#   scripts/scan_secrets.sh [path]       (default: repo root)   exit 0 = clean, 1 = findings
#
# Generic patterns live here. Lab-specific names (workspace host fragments, catalog / bucket / profile / user names)
# go one per line into scripts/.scan_denylist, which is git-ignored so the names themselves are never published.
# A line can opt out with a trailing marker comment: `scan-secrets: allow` (use sparingly, e.g. documented examples).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-$ROOT}"
SELF="scripts/scan_secrets.sh"

EXCLUDES=(--exclude-dir=.git --exclude-dir=.venv --exclude-dir=__pycache__ --exclude-dir=.pytest_cache
          --exclude-dir=out --exclude=.env --exclude=.scan_denylist --exclude=scan_secrets.sh
          --exclude=test_scan_secrets.py --exclude='*.png' --exclude='*.jpg' --exclude='*.zip' --exclude='*.jar')

# kind|extended-regex
PATTERNS=(
  'aws-access-key|\b(AKIA|ASIA)[A-Z0-9]{16}\b'
  'aws-secret-key|aws_secret_access_key[[:space:]]*[=:][[:space:]]*[A-Za-z0-9/+]{30,}'
  'databricks-pat|\bdapi[0-9a-f]{32}\b'
  'databricks-sp-secret|\bdose[0-9a-f]{20,}\b'
  'jwt|\beyJ[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{15,}'
  'private-key|-----BEGIN [A-Z ]*PRIVATE KEY-----'
  'aws-account-id|(^|[^0-9A-Za-z_.-])[0-9]{12}([^0-9A-Za-z_]|$)'
  'arn-with-account|arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:'
  'databricks-host|[a-z0-9-]+\.(cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com)'
  'aws-resource-id|\b(vpc|vpce|subnet|sg|igw|nat|eni|i|ami|rtb|acl)-[0-9a-f]{8,17}\b'
  'ec2-hostname|\bec2-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.'
  'ip-hostname|\bip-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.'
  'ipv4|\b([0-9]{1,3}\.){3}[0-9]{1,3}\b'
  'email|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
  'user-path|/Users/[A-Za-z0-9._-]+|/home/[a-z][a-z0-9._-]*/|~/Downloads'
  'warehouse-id|/sql/1\.0/warehouses/[0-9a-f]{16}|warehouse_id"?[[:space:]]*[:=][[:space:]]*"?[0-9a-f]{16}'
  'cluster-id|\b[0-9]{4}-[0-9]{6}-[a-z0-9]{8}\b'
)
# Allowed values that look like matches (documentation-safe placeholders and loopback/any addresses).
ALLOW='0\.0\.0\.0|127\.0\.0\.1|169\.254\.169\.254|example\.(com|org)|@databricks\.com|@<|<[a-z_-]+>@|scan-secrets: allow|/home/ec2-user/|/home/hadoop/|/home/glue_user/|1\.0\.0\.0'

findings=0
report() { printf '  %-22s %s\n' "$1" "$2"; findings=$((findings + 1)); }

echo "scan_secrets: scanning $TARGET"
for entry in "${PATTERNS[@]}"; do
  kind="${entry%%|*}"; rx="${entry#*|}"
  while IFS= read -r hit; do
    [ -z "$hit" ] && continue
    line="${hit#*:*:}"
    echo "$line" | grep -Eq "$ALLOW" && continue
    loc="${hit%%:*}:$(echo "$hit" | cut -d: -f2)"
    report "$kind" "${loc#"$TARGET"/}  $(echo "$line" | sed -E 's/^[[:space:]]+//' | cut -c1-110)"
  done < <(grep -rnIE "${EXCLUDES[@]}" -- "$rx" "$TARGET" 2>/dev/null)
done

DENY="$ROOT/scripts/.scan_denylist"
if [ -f "$DENY" ]; then
  while IFS= read -r name || [ -n "$name" ]; do
    case "$name" in ''|'#'*) continue ;; esac
    while IFS= read -r hit; do
      [ -z "$hit" ] && continue
      report "denylisted-name" "$(echo "${hit#"$TARGET"/}" | cut -d: -f1-2)  (matches an entry in .scan_denylist)"
    done < <(grep -rnIiF "${EXCLUDES[@]}" -- "$name" "$TARGET" 2>/dev/null)
  done < "$DENY"
else
  echo "  (no scripts/.scan_denylist; add lab-specific names there for a stricter scan)"
fi

# Files that must never exist in the tree, whatever their content.
while IFS= read -r f; do report "forbidden-file" "${f#"$TARGET"/}"; done < <(
  find "$TARGET" \( -name .git -o -name .venv \) -prune -o -type f \( -name '*.pem' -o -name '*.p12' \
    -o -name '.lab_state.env' -o -name '.ec2_state.env' -o -name '.state.env' -o -name '.reverse_state.env' \
    -o -name '.sp_creds.env' -o -name 'spcreds.txt' -o -name '*-trust.json' \) -print 2>/dev/null)

# Notebooks must be committed without outputs.
while IFS= read -r nb; do
  if python3 - "$nb" <<'PY'
import json, sys
nb = json.load(open(sys.argv[1]))
sys.exit(0 if any(c.get("outputs") for c in nb.get("cells", []) if c.get("cell_type") == "code") else 1)
PY
  then report "notebook-outputs" "${nb#"$TARGET"/}"; fi
done < <(find "$TARGET" -name '*.ipynb' -not -path '*/.ipynb_checkpoints/*' -not -path '*/.venv/*' 2>/dev/null)

if [ "$findings" -gt 0 ]; then
  echo "scan_secrets: $findings finding(s). Replace values with \${VARS} / cfg.<FIELD>, or remove the file."
  exit 1
fi
echo "scan_secrets: clean"

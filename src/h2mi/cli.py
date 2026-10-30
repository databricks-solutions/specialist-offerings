"""`h2mi` command line (design §10). Exit codes: 0 ok, 1 error, 2 validation mismatch, 3 lease held, 4 blocked,
5 config invalid. Every mutating command takes --dry-run (NFR-07).

Implemented in the scaffold: `version`, `config check`, `ddl plan --columns` (offline DDL from a column list).
Everything else prints the design section it will implement and exits 1.
"""
from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .util.errors import EXIT_ERROR, EXIT_OK, H2miError

NOT_YET = {
    "doctor": "§9.1 preflight: DNS, TLS, token, loadTable, vended-credential S3 HEAD",
    "inventory": "§8.1", "scan": "§8.2", "bridge": "§8.2", "load": "§8.5 / §8.6", "sync": "§8.6 / §8.7",
    "backfill": "§8.6 hive-partition", "validate": "§8.8", "restore": "§8.10", "reconcile": "§8.7",
    "status": "§7.1 views", "mirror": "§7.4", "state": "§7.2 maintain",
}


def _config_check(a) -> int:
    from .config import effective, load_env, load_table

    env = load_env(a.env)
    out = {"env": env.env, "uc_catalog": env.databricks.uc_catalog, "tables": []}
    for t in a.table:
        e = effective(env, load_table(t))
        out["tables"].append({"source": e.table.source, "target": e.target_fq, "mode": e.table.mode,
                              "engine": e.engine, "format_version": e.format_version, "layout": e.table.layout.model_dump(),
                              "merge_executor": e.merge_executor, "validation_tier": e.validation_tier,
                              "config_hash": e.config_hash, "findings": list(e.findings)})
    print(json.dumps(out, indent=2) if a.json else _pretty(out))
    return EXIT_OK


def _pretty(out: dict) -> str:
    lines = [f"env={out['env']} uc_catalog={out['uc_catalog']}"]
    for t in out["tables"]:
        lines.append(f"- {t['source']} -> {t['target']}  mode={t['mode']} engine={t['engine']} v{t['format_version']} "
                     f"layout={t['layout']} merge_executor={t['merge_executor']} tier={t['validation_tier']} "
                     f"hash={t['config_hash']}")
        lines += [f"    finding: {f}" for f in t["findings"]]
    return "\n".join(lines)


def _ddl_plan(a) -> int:
    """Offline: generate the target + staging DDL for a comma-separated `name:type` column list."""
    from .config import effective, load_env, load_table
    from .ddl.generate import create_table_ddl, grants, staging_table_ddl
    from .scanner.classify import classify_columns, findings, loadable
    from .scanner.typemap import split_top

    env, t = load_env(a.env), load_table(a.table)
    e = effective(env, t)
    cols = [tuple(c.split(":", 1)) for c in split_top(a.columns)]      # commas inside <> and () belong to the type
    plans = classify_columns(cols, a.source_kind, e.char_policy)
    for f in findings(plans):
        print(f"-- [{f.klass}] {f.column} {f.hive_type}: {f.message}")
    if not loadable(plans):
        print("-- table is NOT loadable: resolve the REDESIGN / BLOCK findings first (exit 4)")
        return 4
    print(create_table_ddl(e.target_fq, plans, cluster_by=t.layout.cluster_by, partitioned_by=t.layout.partitioned_by,
                           format_version=e.format_version, retention=e.retention) + ";")
    if t.keys:
        print(staging_table_ddl(e.target_fq, plans, e.format_version) + ";")
    sync_sp = env.principals.sync.get(t.domain)
    if sync_sp:
        print(";\n".join(grants(e.target_fq, sync_sp.client_id)) + ";")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="h2mi", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("version")
    c = sub.add_parser("config", help="validate configuration").add_subparsers(dest="sub", required=True)
    cc = c.add_parser("check")
    cc.add_argument("--env", required=True)
    cc.add_argument("--table", nargs="+", required=True)
    d = sub.add_parser("ddl", help="generate / apply target DDL (§8.3)").add_subparsers(dest="sub", required=True)
    dp = d.add_parser("plan")
    dp.add_argument("--env", required=True)
    dp.add_argument("--table", required=True)
    dp.add_argument("--columns", required=True, help="offline column list, e.g. id:bigint,qty:tinyint,dt:string")
    dp.add_argument("--source-kind", default="hive", choices=["hive", "iceberg"])
    d.add_parser("apply").add_argument("--dry-run", action="store_true")
    for name in NOT_YET:
        p = sub.add_parser(name)
        p.add_argument("--env")
        p.add_argument("--table", nargs="*")
        p.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "version":
            print(__version__)
            return EXIT_OK
        if a.cmd == "config" and a.sub == "check":
            return _config_check(a)
        if a.cmd == "ddl" and a.sub == "plan":
            return _ddl_plan(a)
        where = NOT_YET.get(a.cmd, "§8.3 apply")
        print(f"h2mi {a.cmd}: not implemented in this scaffold (design {where})", file=sys.stderr)
        return EXIT_ERROR
    except H2miError as e:
        print(f"h2mi: {e}", file=sys.stderr)
        return e.exit_code


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""D12 / ARC-09 lint: `DataFrameWriter.mode("overwrite")` and `insertInto(..., overwrite=True)` are forbidden.

Spark turns the V1 overwrite path into REPLACE TABLE AS SELECT on a V2 catalog. UC accepts it over IRC from a
MODIFY-only SP and it drops the target's partition spec and snapshot history (lab 2026-10-30). Partition replace is
`df.writeTo(t).overwritePartitions()` / `.overwrite(<partition predicate>)`, or SQL `INSERT OVERWRITE`.

    python scripts/lint_no_v1_overwrite.py <dir> [<dir> ...]      exit 0 = clean, 1 = findings
"""
import pathlib
import re
import sys

PATTERNS = [
    re.compile(r"""\.mode\(\s*["']overwrite["']\s*\)"""),
    re.compile(r"""\.mode\(\s*SaveMode\.Overwrite\s*\)"""),
    re.compile(r"""\.insertInto\([^)]*overwrite\s*=\s*True"""),
]
ALLOW = "lint: allow-v1-overwrite"


def main(paths: list[str]) -> int:
    findings = 0
    for root in paths:
        for py in sorted(pathlib.Path(root).rglob("*.py")):
            for n, line in enumerate(py.read_text(errors="replace").splitlines(), 1):
                if ALLOW in line:
                    continue
                if any(p.search(line) for p in PATTERNS):
                    print(f"{py}:{n}: V1 overwrite is REPLACE TABLE over IRC (D12): {line.strip()[:100]}")
                    findings += 1
    if findings:
        print(f"lint_no_v1_overwrite: {findings} finding(s). Use writeTo(...).overwritePartitions() / .overwrite(pred).")
        return 1
    print("lint_no_v1_overwrite: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["src"]))

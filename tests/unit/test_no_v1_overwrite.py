"""D12 / ARC-09: writers use DataFrameWriterV2 or SQL only. The V1 overwrite path is REPLACE TABLE over IRC and a
MODIFY-only SP dropped a target's partition spec and history with it (lab 2026-10-30)."""
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def test_lint_passes_on_repo():
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "lint_no_v1_overwrite.py"), str(ROOT / "src")],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


def test_lint_catches_the_v1_api(tmp_path):
    bad = tmp_path / "bad.py"
    bad.write_text('df.write.format("iceberg").mode("overwrite").saveAsTable(t)\n')
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "lint_no_v1_overwrite.py"), str(tmp_path)],
                       capture_output=True, text=True)
    assert r.returncode == 1 and "bad.py:1" in r.stdout

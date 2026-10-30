.PHONY: setup test lint scan-secrets lint-writers check build clean

PY ?= python3

setup:            ## create .venv and install h2mi with dev + spark extras
	$(PY) -m venv .venv && .venv/bin/pip install -q -U pip && .venv/bin/pip install -q -e '.[dev,spark,aws]' && .venv/bin/pre-commit install

test:             ## unit tests (no Spark, no cloud)
	.venv/bin/python -m pytest -q tests/unit

lint:
	.venv/bin/ruff check src tests scripts

lint-writers:     ## D12 / ARC-09: no DataFrameWriter V1 overwrite anywhere in src/
	.venv/bin/python scripts/lint_no_v1_overwrite.py src

scan-secrets:     ## pre-share gate: keys, tokens, environment identifiers
	bash scripts/scan_secrets.sh .

check: lint lint-writers test scan-secrets   ## everything CI runs on a PR

build:            ## wheel + sdist into dist/ (ship with spark-submit --py-files / --archives, design §5.1)
	.venv/bin/python -m build

clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache

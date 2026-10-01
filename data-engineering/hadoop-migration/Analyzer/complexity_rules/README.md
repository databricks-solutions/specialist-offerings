# Code complexity rules

Rule definitions for migration effort scoring. Consumed by `analyzer.scoring`.

## MVP status

PySpark scoring is implemented. Enable via config or `--score-complexity` on analyze commands.

## Usage

```yaml
# analyzer.conf.yaml
complexity:
  enabled: true
  rules_dir: "./complexity_rules"
  local_code_dir: "/path/to/exported/py/files"
```

```bash
python -m analyzer analyze --config analyzer.conf.yaml --score-complexity
```

Outputs:
- `workload_inventory_*.json` — includes `complexity`, `complexity_signals`, `convert_command`
- `workload_inventory_*.csv` — adds complexity columns
- `conversion_queue_*.csv` — prioritized `/convert spark` commands (sorted by tier, then `memory_seconds`)

## Files

| File | Scope |
|------|--------|
| `pyspark.yaml` | PySpark / Python Spark jobs |

## Tier model

| Tier | Lakebridge equivalent | Meaning |
|------|----------------------|---------|
| `easy` | LOW | DataFrame/SQL only; light conversion |
| `medium` | MEDIUM | DataFrame + Hadoop coupling (paths, legacy init, Py2) |
| `hard` | HIGH | RDD, UDF, JVM/Hadoop APIs — rewrite likely |
| `very_hard` | VERY HIGH | DStreams, GraphX/MLlib RDD, custom Hadoop I/O |

## Scoring flow

1. Inventory lists `code_artifacts` with `.py` paths (from Oozie / YARN).
2. Scorer resolves basename under `local_code_dir`.
3. AST + regex detectors from `pyspark.yaml` produce tier + signals.
4. `convert_command` is set for Converter handoff (`/convert spark <path>`).

## Tests

```bash
cd Analyzer && PYTHONPATH=. python3 -m unittest tests.test_pyspark_scorer -v
```

Fixtures: `tests/fixtures/code/` (includes cluster-setup PySpark samples).

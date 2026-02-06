# Verification Strategy: Dual-Run Validation

Ensure data correctness during migration.

## Verification Levels

| Level | Method | Effort | Confidence |
|-------|--------|--------|------------|
| 1 | Row counts | Low | Basic |
| 2 | Aggregates (SUM, AVG) | Medium | Good |
| 3 | Hash fingerprinting | High | Excellent |

## Files

- `level1_*.sql/.py` - Volumetric checks
- `level2_*.sql/.py` - Aggregate profiling
- `level3_*.py` - Hash-based comparison
- `reconciliation_framework.py` - Automated validation suite
- `audit_table.sql` - Store and query results

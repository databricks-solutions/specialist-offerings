---
name: emr-migration-validation
description: "Validate and test EMR to Databricks migration — data comparison, performance benchmarking, regression testing. Use when: (1) 'validate migration', (2) 'compare EMR and Databricks output', (3) 'migration testing', (4) 'benchmark Databricks vs EMR', (5) 'data reconciliation after migration'."
---

# EMR to Databricks Migration Validation

## Overview

Validation is the gate between migration and cutover. All validation checks must pass before decommissioning EMR workloads and switching to Databricks in production. This skill covers the three pillars of migration validation: data, performance, and functional.

## Validation Framework (3 Pillars)

### Pillar 1: Data Validation

Ensure data produced by Databricks matches EMR output exactly (or within acceptable tolerance).

- **Row counts**: Total rows match between EMR and Databricks output
- **Checksums**: MD5/SHA256 hash of key columns matches
- **Schema comparison**: Column names, types, and nullability are identical
- **Sample data diff**: Random sample of rows compared field-by-field
- **Null analysis**: Null counts per column match
- **Aggregate validation**: SUM, AVG, MIN, MAX on numeric columns match

See `data-validation.md` for SQL templates and detailed techniques.

### Pillar 2: Performance Validation

Ensure Databricks performance is acceptable relative to EMR baseline.

- **Execution time**: Wall clock time for equivalent workloads
- **Resource utilization**: CPU, memory, shuffle, I/O metrics
- **Cost comparison**: $/run normalized across platforms
- **Autoscaling behavior**: Cluster scales appropriately under load

See `performance-benchmarking.md` for methodology and templates.

### Pillar 3: Functional Validation

Ensure business logic produces identical results and downstream systems work correctly.

- **Business rule testing**: Same inputs produce same outputs
- **Edge case testing**: Nulls, empty strings, special characters, boundary values
- **End-to-end testing**: Downstream consumers validated with Databricks output
- **Regression testing**: Automated test suite comparing expected vs actual output

See `regression-testing.md` for strategy and templates.

## Quick Start Checklist

Run through this checklist for each migrated workload:

- [ ] Row counts match between EMR output and Databricks output
- [ ] Schema is identical (column names, types, nullable)
- [ ] Checksum/hash on key columns matches
- [ ] Sample of 1000 rows compared field-by-field
- [ ] Edge cases tested (nulls, empty strings, special characters, max/min values)
- [ ] Performance within acceptable range (within 2x of EMR baseline)
- [ ] Downstream consumers validated with Databricks output
- [ ] Streaming workloads: lag and throughput within acceptable range
- [ ] Error handling produces same behavior (bad records, schema mismatches)
- [ ] Logging and monitoring operational in Databricks

## Cutover Criteria

Proceed to production cutover when ALL of the following are true:

1. **Data validation**: All checks pass for at least 3 consecutive runs
2. **Performance**: Within 2x of EMR baseline (or cost-equivalent)
3. **Functional**: All regression tests pass
4. **Monitoring**: Alerting and dashboards are configured in Databricks
5. **Rollback plan**: Documented and tested procedure to revert to EMR if needed
6. **Stakeholder sign-off**: Data owners and downstream consumers have approved

## Related Skills

- **emr-migration-orchestrator**: Master orchestrator for the full migration process

# Phase 2: ETL/Transformation

How data is processed through the Medallion Architecture (Bronze → Silver → Gold).

## Delta Live Tables (DLT)

Replaces: Informatica Mapping Designer + Workflow Manager

| Aspect | Traditional Spark | DLT |
|--------|-------------------|-----|
| Style | Imperative | Declarative |
| Dependencies | Manual | Automatic |
| Quality Checks | Custom code | Built-in expectations |
| Error Handling | Manual retry | Automatic |
| Lineage | Manual tracking | Automatic |

## Medallion Layers

| Layer | Purpose | Informatica Equivalent |
|-------|---------|------------------------|
| Bronze | Raw, append-only | Staging tables |
| Silver | Cleaned, validated | ODS |
| Gold | Business aggregates | DW fact/dim tables |

## Files

- `dlt-pipelines/` - Python and SQL DLT examples
- `medallion-architecture/` - Bronze/Silver/Gold patterns
- `data-quality/` - Expectations and quarantine
- `transformation-patterns/` - Dedup, lookup, SCD, MERGE
- `audit-lineage/` - Lineage queries and monitoring

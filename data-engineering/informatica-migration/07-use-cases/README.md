# Migration Use Cases

End-to-end examples showing all three phases.

## Use Case 1: File → ETL → File

**Scenario:** Mainframe fixed-width → Transform → CSV for SAP

- `file-to-file/01_ingestion.py` - Auto Loader
- `file-to-file/02_etl.py` - DLT transformation
- `file-to-file/03_export.py` - CSV export

## Use Case 2: File → ETL → Power BI

**Scenario:** Daily CSVs → Gold tables → Power BI (no Oracle!)

- `file-to-powerbi/01_ingestion.py` - Auto Loader
- `file-to-powerbi/02_etl.sql` - DLT Medallion

## Use Case 3: Oracle CDC → Application Serving

**Scenario:** Inventory from Oracle OLTP → Apps via Lakebase

- `oracle-cdc-to-apps/01_lakeflow_config.yaml` - CDC setup
- `oracle-cdc-to-apps/02_etl_merge.sql` - APPLY CHANGES
- `oracle-cdc-to-apps/03_consumption.py` - Lakebase or JDBC

# Glue Data Catalog Export Workflow

## Overview

This document provides a detailed boto3-based workflow for exporting all metadata from the AWS Glue Data Catalog, then generating Unity Catalog DDL statements for migration.

## Enumerating All Glue Databases

```python
import boto3
import json
from typing import List, Dict, Any

def get_all_databases(glue_client) -> List[Dict[str, Any]]:
    """Retrieve all databases from Glue Data Catalog."""
    databases = []
    paginator = glue_client.get_paginator('get_databases')
    for page in paginator.paginate():
        databases.extend(page['DatabaseList'])
    return databases

# Usage
glue = boto3.client('glue', region_name='us-east-1')
databases = get_all_databases(glue)

for db in databases:
    print(f"Database: {db['Name']}")
    print(f"  Description: {db.get('Description', 'N/A')}")
    print(f"  Location URI: {db.get('LocationUri', 'N/A')}")
    print(f"  Parameters: {db.get('Parameters', {})}")
```

## Getting All Tables Per Database

```python
def get_all_tables(glue_client, database_name: str) -> List[Dict[str, Any]]:
    """Retrieve all tables for a given Glue database."""
    tables = []
    paginator = glue_client.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        tables.extend(page['TableList'])
    return tables

# Usage
for db in databases:
    tables = get_all_tables(glue, db['Name'])
    for table in tables:
        print(f"  Table: {table['Name']}")
        print(f"    Type: {table.get('TableType', 'UNKNOWN')}")
        print(f"    Format: {table.get('Parameters', {}).get('classification', 'unknown')}")
        sd = table.get('StorageDescriptor', {})
        print(f"    Location: {sd.get('Location', 'N/A')}")
        print(f"    InputFormat: {sd.get('InputFormat', 'N/A')}")
        print(f"    OutputFormat: {sd.get('OutputFormat', 'N/A')}")
        print(f"    SerDe: {sd.get('SerdeInfo', {}).get('SerializationLibrary', 'N/A')}")
        print(f"    Columns: {len(sd.get('Columns', []))}")
        print(f"    PartitionKeys: {[p['Name'] for p in table.get('PartitionKeys', [])]}")
```

## Getting Partitions

```python
def get_all_partitions(glue_client, database_name: str, table_name: str) -> List[Dict[str, Any]]:
    """Retrieve all partitions for a given table."""
    partitions = []
    paginator = glue_client.get_paginator('get_partitions')
    for page in paginator.paginate(DatabaseName=database_name, TableName=table_name):
        partitions.extend(page['Partitions'])
    return partitions

# Usage
partitions = get_all_partitions(glue, 'my_database', 'my_table')
for part in partitions:
    print(f"    Partition: {part['Values']}")
    print(f"      Location: {part['StorageDescriptor']['Location']}")
```

## Extracting Storage Descriptors

The storage descriptor contains critical information for migration:

```python
def extract_storage_info(table: Dict[str, Any]) -> Dict[str, Any]:
    """Extract storage-related metadata from a Glue table definition."""
    sd = table.get('StorageDescriptor', {})
    serde = sd.get('SerdeInfo', {})

    return {
        'table_name': table['Name'],
        'database_name': table['DatabaseName'],
        'table_type': table.get('TableType', 'EXTERNAL_TABLE'),
        'location': sd.get('Location', ''),
        'input_format': sd.get('InputFormat', ''),
        'output_format': sd.get('OutputFormat', ''),
        'serde_library': serde.get('SerializationLibrary', ''),
        'serde_params': serde.get('Parameters', {}),
        'columns': [
            {'name': col['Name'], 'type': col['Type'], 'comment': col.get('Comment', '')}
            for col in sd.get('Columns', [])
        ],
        'partition_keys': [
            {'name': pk['Name'], 'type': pk['Type'], 'comment': pk.get('Comment', '')}
            for pk in table.get('PartitionKeys', [])
        ],
        'table_parameters': table.get('Parameters', {}),
        'compressed': sd.get('Compressed', False),
        'num_buckets': sd.get('NumberOfBuckets', -1),
        'bucket_columns': sd.get('BucketColumns', []),
        'sort_columns': sd.get('SortColumns', []),
        'stored_as_sub_directories': sd.get('StoredAsSubDirectories', False),
    }
```

## Mapping Hive SerDe to Spark Data Source Format

```python
SERDE_TO_FORMAT = {
    # Parquet
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe': 'PARQUET',
    'parquet.hive.serde2.ParquetHiveSerDe': 'PARQUET',

    # ORC
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde': 'ORC',

    # Avro
    'org.apache.hadoop.hive.serde2.avro.AvroSerDe': 'AVRO',

    # CSV
    'org.apache.hadoop.hive.serde2.OpenCSVSerde': 'CSV',
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe': 'CSV',  # may also be TSV

    # JSON
    'org.openx.data.jsonserde.JsonSerDe': 'JSON',
    'org.apache.hive.hcatalog.data.JsonSerDe': 'JSON',
    'org.apache.hadoop.hive.serde2.JsonSerDe': 'JSON',

    # Text
    'org.apache.hadoop.hive.serde2.RegexSerDe': 'TEXT',
}

INPUT_FORMAT_TO_FORMAT = {
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat': 'PARQUET',
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat': 'ORC',
    'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat': 'AVRO',
    'org.apache.hadoop.mapred.TextInputFormat': 'CSV',  # or TEXT
    'org.apache.hadoop.hive.ql.io.HiveInputFormat': None,  # generic, check SerDe
}

def detect_format(storage_info: Dict[str, Any]) -> str:
    """Determine Spark data source format from Glue storage descriptor."""
    # Try SerDe first
    serde = storage_info.get('serde_library', '')
    if serde in SERDE_TO_FORMAT:
        return SERDE_TO_FORMAT[serde]

    # Try InputFormat
    input_fmt = storage_info.get('input_format', '')
    if input_fmt in INPUT_FORMAT_TO_FORMAT and INPUT_FORMAT_TO_FORMAT[input_fmt]:
        return INPUT_FORMAT_TO_FORMAT[input_fmt]

    # Try table parameters
    classification = storage_info.get('table_parameters', {}).get('classification', '').lower()
    format_map = {'parquet': 'PARQUET', 'orc': 'ORC', 'avro': 'AVRO', 'csv': 'CSV', 'json': 'JSON'}
    if classification in format_map:
        return format_map[classification]

    return 'UNKNOWN'
```

## Generating CREATE TABLE DDL for Unity Catalog

```python
from typing import Optional

def map_hive_type_to_spark(hive_type: str) -> str:
    """Map Hive data types to Spark/Delta types."""
    # See schema-mapping.md for the full type mapping table
    type_lower = hive_type.lower().strip()

    # VARCHAR(n) and CHAR(n) become STRING
    if type_lower.startswith('varchar') or type_lower.startswith('char'):
        return 'STRING'

    # UNIONTYPE is not supported -- must be handled manually
    if type_lower.startswith('uniontype'):
        return 'STRING  /* WARNING: UNIONTYPE not supported, needs manual conversion */'

    # All other types map directly (including complex types like array, map, struct)
    return hive_type.upper()


def generate_column_ddl(columns: List[Dict], indent: str = '  ') -> str:
    """Generate column definitions for DDL."""
    col_defs = []
    for col in columns:
        spark_type = map_hive_type_to_spark(col['type'])
        comment_clause = f" COMMENT '{col['comment']}'" if col.get('comment') else ''
        col_defs.append(f"{indent}`{col['name']}` {spark_type}{comment_clause}")
    return ',\n'.join(col_defs)


def generate_external_table_ddl(
    storage_info: Dict[str, Any],
    uc_catalog: str,
    uc_schema: str,
    data_format: str,
) -> str:
    """Generate CREATE EXTERNAL TABLE DDL for Unity Catalog."""
    table_name = storage_info['table_name']
    location = storage_info['location']
    columns = storage_info['columns']
    partition_keys = storage_info['partition_keys']

    all_columns = columns + partition_keys
    col_ddl = generate_column_ddl(all_columns)

    partition_clause = ''
    if partition_keys:
        part_cols = ', '.join(f'`{pk["name"]}`' for pk in partition_keys)
        partition_clause = f'\nPARTITIONED BY ({part_cols})'

    comment = storage_info.get('table_parameters', {}).get('comment', '')
    comment_clause = f"\nCOMMENT '{comment}'" if comment else ''

    # Build format-specific OPTIONS
    options_clause = ''
    if data_format == 'CSV':
        serde_params = storage_info.get('serde_params', {})
        sep = serde_params.get('separatorChar', serde_params.get('field.delim', ','))
        header = 'true' if serde_params.get('skip.header.line.count', '0') != '0' else 'false'
        options_clause = f"\nOPTIONS (header '{header}', delimiter '{sep}')"

    ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{uc_catalog}`.`{uc_schema}`.`{table_name}` (
{col_ddl}
)
USING {data_format}{options_clause}{partition_clause}{comment_clause}
LOCATION '{location}'
TBLPROPERTIES (
  'migrated_from' = 'glue:{storage_info["database_name"]}.{table_name}',
  'migration_date' = current_date()
);"""

    return ddl


def generate_managed_table_ddl(
    storage_info: Dict[str, Any],
    uc_catalog: str,
    uc_schema: str,
    data_format: str,
) -> str:
    """Generate CREATE TABLE ... AS SELECT DDL for managed Delta tables."""
    table_name = storage_info['table_name']
    location = storage_info['location']

    # For managed tables, always convert to Delta via CTAS
    ddl = f"""CREATE TABLE IF NOT EXISTS `{uc_catalog}`.`{uc_schema}`.`{table_name}`
TBLPROPERTIES (
  'migrated_from' = 'glue:{storage_info["database_name"]}.{table_name}',
  'migration_date' = current_date()
)
AS SELECT * FROM {data_format.lower()}.`{location}`;"""

    return ddl
```

## Complete Migration Script Generator

```python
def generate_migration_script(
    glue_client,
    uc_catalog: str,
    strategy: str = 'external',  # 'external', 'managed', or 'hybrid'
    databases: Optional[List[str]] = None,
    exclude_databases: Optional[List[str]] = None,
) -> str:
    """
    Generate a complete SQL migration script from Glue to Unity Catalog.

    Args:
        glue_client: boto3 Glue client
        uc_catalog: Target Unity Catalog catalog name
        strategy: 'external' (register in place), 'managed' (CTAS to Delta), or 'hybrid'
        databases: Optional list of databases to include (None = all)
        exclude_databases: Optional list of databases to exclude
    """
    exclude_databases = exclude_databases or ['default', 'temp', 'tmp']
    script_lines = []

    script_lines.append(f"-- Migration script: Glue Data Catalog -> Unity Catalog")
    script_lines.append(f"-- Generated by export_glue_catalog.py")
    script_lines.append(f"-- Strategy: {strategy}")
    script_lines.append(f"-- Target catalog: {uc_catalog}")
    script_lines.append("")
    script_lines.append(f"CREATE CATALOG IF NOT EXISTS `{uc_catalog}`;")
    script_lines.append(f"USE CATALOG `{uc_catalog}`;")
    script_lines.append("")

    all_databases = get_all_databases(glue_client)

    for db in all_databases:
        db_name = db['Name']

        if databases and db_name not in databases:
            continue
        if db_name in exclude_databases:
            continue

        schema_name = db_name
        db_comment = db.get('Description', f'Migrated from Glue database: {db_name}')

        script_lines.append(f"-- === Schema: {schema_name} (from Glue DB: {db_name}) ===")
        script_lines.append(
            f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{schema_name}` "
            f"COMMENT '{db_comment}';"
        )
        script_lines.append("")

        tables = get_all_tables(glue_client, db_name)

        for table in tables:
            storage_info = extract_storage_info(table)
            data_format = detect_format(storage_info)

            if data_format == 'UNKNOWN':
                script_lines.append(
                    f"-- WARNING: Unknown format for {db_name}.{table['Name']}. "
                    f"SerDe={storage_info['serde_library']}, "
                    f"InputFormat={storage_info['input_format']}. Skipping."
                )
                script_lines.append("")
                continue

            # Determine if external or managed based on strategy
            use_managed = False
            if strategy == 'managed':
                use_managed = True
            elif strategy == 'hybrid':
                # Hybrid: managed for non-raw databases
                raw_indicators = ['raw', 'bronze', 'landing', 'staging', 'ingestion']
                use_managed = not any(ind in db_name.lower() for ind in raw_indicators)

            if use_managed:
                ddl = generate_managed_table_ddl(storage_info, uc_catalog, schema_name, data_format)
            else:
                ddl = generate_external_table_ddl(storage_info, uc_catalog, schema_name, data_format)

            script_lines.append(ddl)
            script_lines.append("")

        # Handle views
        views = [t for t in tables if t.get('TableType') == 'VIRTUAL_VIEW']
        for view in views:
            view_text = view.get('ViewOriginalText', view.get('ViewExpandedText', ''))
            if view_text:
                script_lines.append(f"-- View: {view['Name']} (original text below, needs manual update)")
                script_lines.append(f"-- Original: {view_text}")
                script_lines.append(
                    f"-- TODO: Recreate as CREATE VIEW `{uc_catalog}`.`{schema_name}`.`{view['Name']}` AS ..."
                )
                script_lines.append("")

    return '\n'.join(script_lines)
```

## Running the Export

The helper script at `/Users/kishore.mannava/cursorprojects/umlaut-poc-emr-claude/scripts/export_glue_catalog.py` wraps the above functions into a CLI tool:

```bash
# Export Glue metadata to JSON
python scripts/export_glue_catalog.py \
  --region us-east-1 \
  --output catalog_export.json

# Generate UC migration SQL script
python scripts/export_glue_catalog.py \
  --region us-east-1 \
  --generate-ddl \
  --uc-catalog migrated_from_emr \
  --strategy hybrid \
  --output migration.sql

# Export specific databases only
python scripts/export_glue_catalog.py \
  --region us-east-1 \
  --databases raw_data,processed,analytics \
  --generate-ddl \
  --uc-catalog migrated_from_emr \
  --output migration.sql
```

## Handling Edge Cases

### Tables with no columns in StorageDescriptor

Some Glue tables (especially those created by Crawlers) store columns in the SerDe parameters rather than the `Columns` field. Check both locations:

```python
def get_columns(table: Dict) -> List[Dict]:
    sd = table.get('StorageDescriptor', {})
    columns = sd.get('Columns', [])
    if not columns:
        # Try to infer from SerDe params or table parameters
        schema_literal = sd.get('SerdeInfo', {}).get('Parameters', {}).get('columns', '')
        type_literal = sd.get('SerdeInfo', {}).get('Parameters', {}).get('columns.types', '')
        if schema_literal and type_literal:
            names = schema_literal.split(',')
            types = type_literal.split(':')
            columns = [{'Name': n.strip(), 'Type': t.strip()} for n, t in zip(names, types)]
    return columns
```

### Tables pointing to non-existent S3 paths

Validate S3 paths before generating DDL:

```python
def validate_s3_path(s3_client, location: str) -> bool:
    """Check if an S3 path exists and has data."""
    if not location.startswith('s3://'):
        return False
    bucket, prefix = location.replace('s3://', '').split('/', 1)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get('KeyCount', 0) > 0
```

### Symlink-based tables

Some Hive tables use symlink manifests. These need special handling -- convert to Delta or standard external tables:

```python
def is_symlink_table(storage_info: Dict) -> bool:
    return 'SymlinkTextInputFormat' in storage_info.get('input_format', '')
```

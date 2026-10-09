"""Databricks SQL connector wrapper for the TCO Calculator App.

Handles connection lifecycle, query execution, and schema initialization.
Auth: PAT for local dev, M2M OAuth (client_credentials) for Databricks Apps.
"""

import os
import logging
import pandas as pd
import requests
from databricks import sql as dbsql
from contextlib import contextmanager

logger = logging.getLogger(__name__)

_WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")


def _get_access_token() -> str:
    """Obtain an access token: PAT for local dev, M2M OAuth for Apps."""
    # 1. PAT (local dev / DATABRICKS_TOKEN set)
    token = os.getenv("DATABRICKS_TOKEN")
    if token:
        logger.info("Auth: using PAT from DATABRICKS_TOKEN")
        return token

    # 2. M2M OAuth client_credentials (Databricks Apps)
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")

    if not all([host, client_id, client_secret]):
        raise ValueError(
            "No DATABRICKS_TOKEN and missing CLIENT_ID/SECRET for M2M OAuth. "
            f"host={host}, client_id={'SET' if client_id else 'NOT SET'}, "
            f"client_secret={'SET' if client_secret else 'NOT SET'}"
        )

    resp = requests.post(
        f"https://{host}/oidc/v1/token",
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        auth=(client_id, client_secret),
    )
    resp.raise_for_status()
    logger.info("Auth: M2M OAuth token acquired from %s", host)
    return resp.json()["access_token"]


def _get_connection_params():
    """Build connection parameters for databricks-sql-connector."""
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
    http_path = f"/sql/1.0/warehouses/{_WAREHOUSE_ID}" if _WAREHOUSE_ID else ""
    token = _get_access_token()

    logger.info("Connection: host=%s, path=%s, has_token=%s",
                host, http_path, bool(token))

    return {
        "server_hostname": host,
        "http_path": http_path,
        "access_token": token,
    }


@contextmanager
def get_connection():
    """Yield a Databricks SQL connection. Auto-closes on exit."""
    params = _get_connection_params()
    conn = dbsql.connect(**params)
    try:
        yield conn
    finally:
        conn.close()


def execute_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Execute SQL and return results as a DataFrame."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            cols = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)


def execute_statement(statement: str):
    """Execute a DDL/DML statement (no result set expected)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement)


def execute_multi(statements: str):
    """Execute multiple semicolon-separated SQL statements."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in statements.split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)


def _parse_sql_file(path: str) -> list[str]:
    """Parse a SQL file into executable statements, stripping comments."""
    with open(path) as f:
        lines = f.readlines()
    # Remove full-line comments, keep inline content
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        cleaned.append(line)
    sql = "".join(cleaned)
    # Split on semicolons and return non-empty statements
    return [s.strip() for s in sql.split(";") if s.strip()]


def init_schema(catalog: str, schema: str):
    """Create tables + seed defaults in the given catalog.schema."""
    db_dir = os.path.join(os.path.dirname(__file__), "..", "db")
    schema_sql_path = os.path.join(db_dir, "schema.sql")
    schema_v2_sql_path = os.path.join(db_dir, "schema_v2.sql")
    seed_sql_path = os.path.join(db_dir, "seed_data.sql")
    seed_lookups_path = os.path.join(db_dir, "seed_lookups.sql")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            cur.execute(f"USE CATALOG {catalog}")
            cur.execute(f"USE SCHEMA {schema}")

            # Run base schema DDL
            for stmt in _parse_sql_file(schema_sql_path):
                cur.execute(stmt)

            # Run V2 schema (new tables + ALTER TABLE)
            if os.path.exists(schema_v2_sql_path):
                for stmt in _parse_sql_file(schema_v2_sql_path):
                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        # ALTER TABLE ADD COLUMNS may fail if columns exist
                        if "already exists" in str(e).lower():
                            logger.info("V2 column already exists, skipping: %s", str(e)[:100])
                        else:
                            raise
                logger.info("Schema V2 applied.")

            # Seed SKU mappings
            cur.execute("SELECT COUNT(*) FROM tco_workload_sku_mapping")
            count = cur.fetchone()[0]
            if count == 0:
                for stmt in _parse_sql_file(seed_sql_path):
                    cur.execute(stmt)
                logger.info("Seed data loaded.")
            else:
                logger.info("Seed data already exists (%d rows), skipping.", count)

            # Seed lookup tables
            if os.path.exists(seed_lookups_path):
                cur.execute("SELECT COUNT(*) FROM tco_lookup_vm_instances")
                vm_count = cur.fetchone()[0]
                if vm_count == 0:
                    for stmt in _parse_sql_file(seed_lookups_path):
                        cur.execute(stmt)
                    logger.info("Lookup seed data loaded.")
                else:
                    logger.info("Lookup data already exists (%d VM rows), skipping.", vm_count)


def qualified_table(table_name: str, catalog: str, schema: str) -> str:
    """Return fully-qualified table name: catalog.schema.table."""
    return f"{catalog}.{schema}.{table_name}"

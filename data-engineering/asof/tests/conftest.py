"""Local SparkSession for the unit tests (no cluster, no Databricks Connect)."""

import os
import sys
from pathlib import Path

import pytest

# Make `asof_spark` importable exactly as the notebooks import it (same folder).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Spark workers must run the same interpreter as the driver; pin it to this process's Python so a
# different `python3` on PATH cannot cause PYTHON_VERSION_MISMATCH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("asof-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()

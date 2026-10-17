"""Tests for Spark data loaders."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.spark_loader import load_spark_applications

PROFILER_OUTPUT = os.path.expanduser("~/cloudera-profiler-output/Output-2026-03-25/Output")


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestSparkLoader(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_load_spark_applications(self):
        rows = load_spark_applications(self.conn, PROFILER_OUTPUT)
        # 4 apps, each with 1 attempt = 4 rows
        self.assertEqual(rows, 4)

    def test_spark_app_names(self):
        load_spark_applications(self.conn, PROFILER_OUTPUT)
        result = self.conn.execute(
            "SELECT DISTINCT name FROM spark_applications ORDER BY name"
        ).fetchall()
        names = [r[0] for r in result]
        self.assertIn("Spark Pi", names)
        self.assertIn("PySpark Sales Analysis", names)
        self.assertIn("PySpark Employee Analysis", names)

    def test_duration_computed(self):
        load_spark_applications(self.conn, PROFILER_OUTPUT)
        result = self.conn.execute(
            "SELECT duration_ms FROM spark_applications "
            "WHERE name = 'Spark Pi' LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(result[0])
        self.assertGreater(result[0], 0)

    def test_missing_directory(self):
        rows = load_spark_applications(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)


if __name__ == "__main__":
    unittest.main()

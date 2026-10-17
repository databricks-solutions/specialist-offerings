"""Tests for Impala data loaders."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.impala_loader import load_impala_queries

PROFILER_OUTPUT = os.path.expanduser("~/cloudera-profiler-output/Output-2026-03-25/Output")


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestImpalaLoader(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_load_impala_queries(self):
        # No Impala queries in the March 25 run (Impala extraction disabled)
        rows = load_impala_queries(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 0)

    def test_empty_table_exists(self):
        load_impala_queries(self.conn, PROFILER_OUTPUT)
        result = self.conn.execute("SELECT COUNT(*) FROM impala_queries").fetchone()
        self.assertEqual(result[0], 0)

    def test_missing_directory(self):
        rows = load_impala_queries(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)


if __name__ == "__main__":
    unittest.main()

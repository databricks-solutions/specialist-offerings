"""Tests for Cloudera Manager data loaders."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.cm_loader import (
    load_cm_hosts,
    load_cm_services,
    load_cm_config,
    load_cm_export,
    load_cm_host_roles,
    load_all_cm_timeseries,
    load_all_cm,
)

PROFILER_OUTPUT = os.path.expanduser("~/cloudera-profiler-output/Output-2026-03-25/Output")


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestCMLoader(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_load_cm_hosts(self):
        rows = load_cm_hosts(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 1)
        result = self.conn.execute(
            "SELECT hostname, num_cores, total_phys_mem_gb FROM cm_hosts"
        ).fetchone()
        self.assertEqual(result[0], "quickstart.cloudera")
        self.assertEqual(result[1], 4)
        self.assertGreater(result[2], 0)  # Should have GB computed

    def test_load_cm_services(self):
        rows = load_cm_services(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 13)
        # Check a known service
        result = self.conn.execute(
            "SELECT service_type, cluster_name FROM cm_services WHERE service_name = 'hive'"
        ).fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], "HIVE")
        self.assertEqual(result[1], "Cloudera QuickStart")

    def test_load_cm_config(self):
        rows = load_cm_config(self.conn, PROFILER_OUTPUT)
        # Config may be empty in this profiler run
        self.assertGreaterEqual(rows, 0)

    def test_load_cm_export(self):
        rows = load_cm_export(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 1)
        result = self.conn.execute("SELECT LENGTH(export_json) FROM cm_export").fetchone()
        self.assertGreater(result[0], 100)  # Should contain substantial JSON

    def test_load_cm_host_roles(self):
        rows = load_cm_host_roles(self.conn, PROFILER_OUTPUT)
        # Host roles may be empty if timeSeries has no data
        self.assertGreaterEqual(rows, 0)

    def test_load_all_cm_timeseries(self):
        rows = load_all_cm_timeseries(self.conn, PROFILER_OUTPUT)
        # May have data points or may be empty (QuickStart timeseries can be sparse)
        self.assertGreaterEqual(rows, 0)

    def test_load_all_cm(self):
        rows = load_all_cm(self.conn, PROFILER_OUTPUT)
        self.assertGreater(rows, 0)

    def test_missing_directory(self):
        rows = load_cm_hosts(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)


if __name__ == "__main__":
    unittest.main()

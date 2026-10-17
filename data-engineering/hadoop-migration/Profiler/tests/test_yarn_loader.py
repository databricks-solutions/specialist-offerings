"""Tests for YARN data loaders."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.yarn_loader import (
    load_yarn_applications,
    load_yarn_metrics,
    load_yarn_nodes,
    load_yarn_scheduler,
)

PROFILER_OUTPUT = os.path.expanduser("~/cloudera-profiler-output/Output-2026-03-25/Output")


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestYarnLoader(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_load_yarn_applications(self):
        rows = load_yarn_applications(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 156)
        # Verify all apps are MAPREDUCE
        result = self.conn.execute(
            "SELECT DISTINCT application_type FROM yarn_applications"
        ).fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "MAPREDUCE")
        # Verify all apps are by user 'cloudera'
        result = self.conn.execute(
            'SELECT DISTINCT "user" FROM yarn_applications'
        ).fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "cloudera")

    def test_load_yarn_metrics(self):
        rows = load_yarn_metrics(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 1)
        result = self.conn.execute("SELECT apps_submitted, total_mb, active_nodes FROM yarn_cluster_metrics").fetchone()
        self.assertEqual(result[0], 156)  # appsSubmitted
        self.assertEqual(result[1], 3072)  # totalMB
        self.assertEqual(result[2], 1)  # activeNodes

    def test_load_yarn_nodes(self):
        rows = load_yarn_nodes(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 1)
        result = self.conn.execute("SELECT node_host_name, state, avail_memory_mb FROM yarn_nodes").fetchone()
        self.assertEqual(result[0], "quickstart.cloudera")
        self.assertEqual(result[1], "RUNNING")
        self.assertEqual(result[2], 3072)

    def test_load_yarn_scheduler(self):
        rows = load_yarn_scheduler(self.conn, PROFILER_OUTPUT)
        self.assertEqual(rows, 2)  # root.cloudera and root.default
        result = self.conn.execute(
            "SELECT queue_name, scheduler_type FROM yarn_scheduler_queues ORDER BY queue_name"
        ).fetchall()
        self.assertEqual(result[0][0], "root.cloudera")
        self.assertEqual(result[0][1], "fairScheduler")

    def test_missing_directory(self):
        rows = load_yarn_applications(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)


if __name__ == "__main__":
    unittest.main()

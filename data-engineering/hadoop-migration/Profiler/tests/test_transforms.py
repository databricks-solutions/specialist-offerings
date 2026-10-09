"""Tests for derived analysis tables."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.yarn_loader import load_yarn_applications
from duckdb_exporter.transforms.yarn_analysis import (
    create_yarn_analysis_vw,
    create_oozie_analysis_vw,
    create_hourly_yarn_view,
    create_all_yarn_analysis,
)
from duckdb_exporter.transforms.summary_tables import (
    create_workload_summary_by_user,
    create_workload_summary_by_queue,
    create_workload_summary_by_type,
    create_all_summary_tables,
)

PROFILER_OUTPUT = os.path.expanduser("~/cloudera-profiler-output/Output-2026-03-25/Output")


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestYarnAnalysis(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)
        load_yarn_applications(self.conn, PROFILER_OUTPUT)

    def tearDown(self):
        self.conn.close()

    def test_yarn_analysis_vw(self):
        rows = create_yarn_analysis_vw(self.conn, dbu_rate=0.15, vm_rate=0.10)
        self.assertEqual(rows, 156)  # Same as yarn_applications

    def test_job_type_classification(self):
        create_yarn_analysis_vw(self.conn)
        result = self.conn.execute(
            "SELECT job_type, COUNT(*) as cnt FROM yarn_analysis_vw GROUP BY job_type ORDER BY cnt DESC"
        ).fetchall()
        job_types = {r[0]: r[1] for r in result}
        # All 156 apps are MAPREDUCE — classified by name patterns into
        # Sqoop (Oozie), Sqoop, Hive (Oozie), Hive, Oozie Launcher
        total = sum(job_types.values())
        self.assertEqual(total, 156)

    def test_cost_columns(self):
        create_yarn_analysis_vw(self.conn, dbu_rate=0.15, vm_rate=0.10)
        result = self.conn.execute(
            "SELECT dollar_dbus, dollar_vm, total_cost FROM yarn_analysis_vw "
            "WHERE memory_seconds > 0 LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(result)
        self.assertGreater(result[0], 0)
        self.assertGreater(result[1], 0)
        self.assertAlmostEqual(result[2], result[0] + result[1], places=6)

    def test_oozie_analysis_vw(self):
        create_yarn_analysis_vw(self.conn)
        rows = create_oozie_analysis_vw(self.conn)
        # March 25 run has 92 Oozie launcher apps
        self.assertEqual(rows, 92)

    def test_hourly_yarn_view(self):
        create_yarn_analysis_vw(self.conn)
        rows = create_hourly_yarn_view(self.conn)
        self.assertGreater(rows, 0)

    def test_create_all_yarn_analysis(self):
        total = create_all_yarn_analysis(self.conn, dbu_rate=0.15, vm_rate=0.10)
        self.assertGreater(total, 0)


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestSummaryTables(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)
        load_yarn_applications(self.conn, PROFILER_OUTPUT)
        create_yarn_analysis_vw(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_summary_by_user(self):
        rows = create_workload_summary_by_user(self.conn)
        self.assertGreater(rows, 0)
        result = self.conn.execute(
            'SELECT "user", total_jobs FROM workload_summary_by_user'
        ).fetchall()
        # CDH QuickStart has 'cloudera' user
        users = {r[0] for r in result}
        self.assertIn("cloudera", users)

    def test_summary_by_queue(self):
        rows = create_workload_summary_by_queue(self.conn)
        self.assertGreater(rows, 0)

    def test_summary_by_type(self):
        rows = create_workload_summary_by_type(self.conn)
        self.assertGreater(rows, 0)

    def test_create_all_summary_tables(self):
        total = create_all_summary_tables(self.conn)
        self.assertGreater(total, 0)


if __name__ == "__main__":
    unittest.main()

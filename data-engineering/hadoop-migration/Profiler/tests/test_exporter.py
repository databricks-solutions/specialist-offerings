"""End-to-end tests for the DuckDB exporter."""
import os
import tempfile
import unittest

import duckdb

from duckdb_exporter.config import ExporterConfig, ProfilerOutputConfig, OutputConfig, CostRatesConfig
from duckdb_exporter.exporter import run_export

PROFILER_OUTPUT = os.path.expanduser("~/cloudera-profiler-output/Output-2026-03-25/Output")


@unittest.skipUnless(os.path.isdir(PROFILER_OUTPUT), "Profiler output not available")
class TestExporter(unittest.TestCase):
    def test_full_export(self):
        """Test complete export pipeline against real profiler output."""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
            db_path = f.name

        try:
            config = ExporterConfig(
                profiler_output=ProfilerOutputConfig(base_dir=PROFILER_OUTPUT),
                output=OutputConfig(db_path=db_path, overwrite=True),
                cost_rates=CostRatesConfig(dbu_rate=0.15, vm_rate=0.10),
            )
            run_export(config)

            # Verify the DuckDB file exists and has tables
            conn = duckdb.connect(db_path, read_only=True)
            try:
                tables = conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
                table_names = [t[0] for t in tables]

                # Should have 24 tables (17 base + 6 derived + 1 metadata)
                self.assertGreaterEqual(len(table_names), 20)

                # Verify core tables exist
                self.assertIn("yarn_applications", table_names)
                self.assertIn("yarn_cluster_metrics", table_names)
                self.assertIn("spark_applications", table_names)
                self.assertIn("impala_queries", table_names)
                self.assertIn("cm_hosts", table_names)
                self.assertIn("yarn_analysis_vw", table_names)
                self.assertIn("export_metadata", table_names)

                # Verify YARN app count (baseline: 156)
                yarn_count = conn.execute("SELECT COUNT(*) FROM yarn_applications").fetchone()[0]
                self.assertEqual(yarn_count, 156)

                # Verify Spark app count (baseline: 4 apps, 1 attempt each)
                spark_count = conn.execute("SELECT COUNT(*) FROM spark_applications").fetchone()[0]
                self.assertEqual(spark_count, 4)

                # Verify Impala query count (0 — Impala extraction disabled)
                impala_count = conn.execute("SELECT COUNT(*) FROM impala_queries").fetchone()[0]
                self.assertEqual(impala_count, 0)

                # Verify derived table
                analysis_count = conn.execute("SELECT COUNT(*) FROM yarn_analysis_vw").fetchone()[0]
                self.assertEqual(analysis_count, 156)

                # Verify CM hosts
                cm_hosts_count = conn.execute("SELECT COUNT(*) FROM cm_hosts").fetchone()[0]
                self.assertEqual(cm_hosts_count, 1)

                # Verify CM services
                cm_services_count = conn.execute("SELECT COUNT(*) FROM cm_services").fetchone()[0]
                self.assertEqual(cm_services_count, 13)

                # Verify export metadata
                meta = conn.execute("SELECT * FROM export_metadata").fetchone()
                self.assertIsNotNone(meta)

            finally:
                conn.close()
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_export_invalid_directory(self):
        """Test that export raises error for invalid directory."""
        config = ExporterConfig(
            profiler_output=ProfilerOutputConfig(base_dir="/nonexistent/path"),
            output=OutputConfig(db_path="/tmp/test.duckdb"),
        )
        with self.assertRaises(FileNotFoundError):
            run_export(config)


if __name__ == "__main__":
    unittest.main()

"""Tests for Ambari (HDP) data loaders."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.ambari_loader import (
    load_ambari_hosts,
    load_ambari_host_components,
    load_ambari_stack,
    load_ambari_services,
    load_ambari_yarn_and_hbase_allocation,
    load_hdfs_stats,
)

# Test fixture: Ambari profiler extract from visa_dpi (may contain HTTP errors)
TEST_DATA_DIR = "/Users/akshay.amin/code/feip_hadoop_migration/specialist-offerings/data-engineering/hadoop-migration/Profiler/input_json_dont_check_in/visa_dpi/Output"


class TestAmbariLoader(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ambari_hosts_graceful_degradation(self):
        """Test that ambari_hosts loader handles HTTP errors gracefully."""
        # The test fixture contains HTTP error responses (404, 403)
        # Loader should not crash and return 0 rows
        rows = load_ambari_hosts(self.conn, TEST_DATA_DIR)
        # Should be 0 because fixture only has error responses
        self.assertEqual(rows, 0)
        # Table should exist but be empty
        result = self.conn.execute("SELECT COUNT(*) FROM ambari_hosts").fetchone()
        self.assertEqual(result[0], 0)

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ambari_host_components_graceful_degradation(self):
        """Test that ambari_host_components loader handles HTTP errors gracefully."""
        rows = load_ambari_host_components(self.conn, TEST_DATA_DIR)
        self.assertEqual(rows, 0)
        result = self.conn.execute("SELECT COUNT(*) FROM ambari_host_components").fetchone()
        self.assertEqual(result[0], 0)

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ambari_stack_graceful_degradation(self):
        """Test that ambari_stack loader handles HTTP errors gracefully."""
        rows = load_ambari_stack(self.conn, TEST_DATA_DIR)
        self.assertEqual(rows, 0)
        result = self.conn.execute("SELECT COUNT(*) FROM ambari_stack").fetchone()
        self.assertEqual(result[0], 0)

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ambari_services_graceful_degradation(self):
        """Test that ambari_services loader handles HTTP errors gracefully."""
        rows = load_ambari_services(self.conn, TEST_DATA_DIR)
        self.assertEqual(rows, 0)
        result = self.conn.execute("SELECT COUNT(*) FROM ambari_services").fetchone()
        self.assertEqual(result[0], 0)

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ambari_yarn_and_hbase_allocation_graceful_degradation(self):
        """Test that YARN/HBase allocation loader handles HTTP errors gracefully."""
        yarn_rows, hbase_rows = load_ambari_yarn_and_hbase_allocation(self.conn, TEST_DATA_DIR)
        self.assertEqual(yarn_rows, 0)
        self.assertEqual(hbase_rows, 0)

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_hdfs_stats_graceful_degradation(self):
        """Test that HDFS stats loader handles HTTP errors gracefully."""
        rows = load_hdfs_stats(self.conn, TEST_DATA_DIR)
        self.assertEqual(rows, 0)
        result = self.conn.execute("SELECT COUNT(*) FROM hdfs_stats").fetchone()
        self.assertEqual(result[0], 0)

    def test_missing_directory(self):
        """Test loaders handle missing directory gracefully."""
        rows = load_ambari_hosts(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)


if __name__ == "__main__":
    unittest.main()

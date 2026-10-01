"""Tests for Ranger security data loaders."""
import os
import unittest

import duckdb

from duckdb_exporter.schema import create_all_base_tables
from duckdb_exporter.loaders.ranger_loader import load_ranger_policies, load_ranger_repos

# Test fixture: Ranger profiler extract from visa_dpi (may contain HTTP errors)
TEST_DATA_DIR = "/Users/akshay.amin/code/feip_hadoop_migration/specialist-offerings/data-engineering/hadoop-migration/Profiler/input_json_dont_check_in/visa_dpi/Output"


class TestRangerLoader(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        create_all_base_tables(self.conn)

    def tearDown(self):
        self.conn.close()

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ranger_policies_graceful_degradation(self):
        """Test that ranger_policies loader handles HTTP errors gracefully."""
        # The test fixture may contain HTTP error responses
        # Loader should not crash and return 0 or more rows
        rows = load_ranger_policies(self.conn, TEST_DATA_DIR)
        self.assertGreaterEqual(rows, 0)
        result = self.conn.execute("SELECT COUNT(*) FROM ranger_policies").fetchone()
        self.assertEqual(result[0], rows)

    @unittest.skipUnless(os.path.isdir(TEST_DATA_DIR), "Test data directory not available")
    def test_load_ranger_repos_graceful_degradation(self):
        """Test that ranger_repos loader handles HTTP errors gracefully."""
        rows = load_ranger_repos(self.conn, TEST_DATA_DIR)
        self.assertGreaterEqual(rows, 0)
        result = self.conn.execute("SELECT COUNT(*) FROM ranger_repos").fetchone()
        self.assertEqual(result[0], rows)

    def test_missing_directory(self):
        """Test loaders handle missing directory gracefully."""
        rows = load_ranger_policies(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)
        rows = load_ranger_repos(self.conn, "/nonexistent/path")
        self.assertEqual(rows, 0)


if __name__ == "__main__":
    unittest.main()

"""Tests for Oozie REST API client (mocked HTTP)."""

import json
import unittest
from unittest.mock import MagicMock, patch

from analyzer.connectors.oozie_client import OozieClient


class TestOozieClient(unittest.TestCase):

    def setUp(self):
        self.client = OozieClient("http://oozie-test:11000")

    @patch("analyzer.connectors.oozie_client.requests.Session.get")
    def test_list_workflows(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "workflows": [
                {"id": "0000001-test-W", "appName": "test-wf", "appPath": "/user/oozie/wf1"},
                {"id": "0000002-test-W", "appName": "test-wf2", "appPath": "/user/oozie/wf2"},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        workflows = self.client.list_workflows(max_jobs=100)
        self.assertEqual(len(workflows), 2)
        self.assertEqual(workflows[0]["appName"], "test-wf")

    @patch("analyzer.connectors.oozie_client.requests.Session.get")
    def test_list_coordinators(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "coordinatorjobs": [
                {"id": "0000001-test-C", "coordJobName": "daily-etl"},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        coords = self.client.list_coordinators(max_jobs=100)
        self.assertEqual(len(coords), 1)

    @patch("analyzer.connectors.oozie_client.requests.Session.get")
    def test_get_workflow_definition(self, mock_get):
        xml_content = '<workflow-app name="test"><start to="end"/><end name="end"/></workflow-app>'
        mock_resp = MagicMock()
        mock_resp.text = xml_content
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        definition = self.client.get_workflow_definition("0000001-test-W")
        self.assertIn("workflow-app", definition)

    @patch("analyzer.connectors.oozie_client.requests.Session.get")
    def test_pagination_stops_on_empty(self, mock_get):
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            if call_count == 1:
                mock_resp.json.return_value = {
                    "workflows": [{"id": f"wf-{i}", "appName": f"wf{i}"} for i in range(1000)]
                }
            else:
                mock_resp.json.return_value = {"workflows": []}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        mock_get.side_effect = side_effect

        workflows = self.client.list_workflows(max_jobs=5000)
        self.assertEqual(len(workflows), 1000)
        self.assertEqual(call_count, 2)


if __name__ == "__main__":
    unittest.main()

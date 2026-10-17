"""Oozie REST API client for retrieving workflow/coordinator definitions."""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)


class OozieClient:
    """Client for the Oozie REST API (v1/v2)."""

    def __init__(self, base_url: str, auth: str = "simple",
                 kerberos_principal: str = "", timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

        if auth == "kerberos" and kerberos_principal:
            try:
                from requests_kerberos import HTTPKerberosAuth
                self.session.auth = HTTPKerberosAuth()
            except ImportError:
                logger.warning("requests-kerberos not installed; falling back to simple auth")

    def _get(self, path: str, params: Optional[Dict] = None) -> Any:
        """Make a GET request to the Oozie API."""
        url = f"{self.base_url}/{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _get_text(self, path: str, params: Optional[Dict] = None) -> str:
        """Make a GET request and return raw text (for XML definitions)."""
        url = f"{self.base_url}/{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def list_workflows(self, max_jobs: int = 5000) -> List[Dict]:
        """List workflow jobs with pagination."""
        all_jobs = []
        offset = 1  # Oozie uses 1-based offset

        while True:
            params = {
                "jobtype": "wf",
                "len": min(max_jobs - len(all_jobs), 1000),
                "offset": offset,
            }
            data = self._get("oozie/v2/jobs", params=params)
            jobs = data.get("workflows", [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            if len(all_jobs) >= max_jobs or len(jobs) < params["len"]:
                break
            offset += len(jobs)

        logger.info("Retrieved %d Oozie workflows", len(all_jobs))
        return all_jobs

    def list_coordinators(self, max_jobs: int = 5000) -> List[Dict]:
        """List coordinator jobs with pagination."""
        all_jobs = []
        offset = 1

        while True:
            params = {
                "jobtype": "coord",
                "len": min(max_jobs - len(all_jobs), 1000),
                "offset": offset,
            }
            data = self._get("oozie/v2/jobs", params=params)
            jobs = data.get("coordinatorjobs", [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            if len(all_jobs) >= max_jobs or len(jobs) < params["len"]:
                break
            offset += len(jobs)

        logger.info("Retrieved %d Oozie coordinators", len(all_jobs))
        return all_jobs

    def get_workflow_info(self, job_id: str) -> Dict:
        """Get detailed info for a specific workflow job."""
        return self._get(f"oozie/v2/job/{job_id}", params={"show": "info"})

    def get_workflow_definition(self, job_id: str) -> str:
        """Get the workflow.xml definition for a workflow job."""
        return self._get_text(f"oozie/v2/job/{job_id}", params={"show": "definition"})

    def get_coordinator_info(self, job_id: str) -> Dict:
        """Get detailed info for a specific coordinator job."""
        return self._get(f"oozie/v2/job/{job_id}", params={"show": "info"})

    def get_coordinator_definition(self, job_id: str) -> str:
        """Get the coordinator.xml definition."""
        return self._get_text(f"oozie/v2/job/{job_id}", params={"show": "definition"})

    def get_workflow_actions(self, job_id: str) -> List[Dict]:
        """Get actions for a workflow job."""
        info = self.get_workflow_info(job_id)
        return info.get("actions", [])

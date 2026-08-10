"""WebHDFS client for verifying code artifact paths on HDFS."""

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class WebHDFSClient:
    """Client for WebHDFS REST API to check file existence."""

    def __init__(self, base_url: str, user: str = "hdfs", timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.session = requests.Session()

    def file_exists(self, hdfs_path: str) -> bool:
        """Check if a file exists on HDFS using GETFILESTATUS."""
        # Normalize path: strip hdfs:// prefix if present
        path = hdfs_path
        if path.startswith("hdfs://"):
            # Remove hdfs://namenode:port prefix
            path = "/" + path.split("/", 3)[-1] if path.count("/") >= 3 else path

        url = f"{self.base_url}/webhdfs/v1{path}"
        params = {"op": "GETFILESTATUS", "user.name": self.user}

        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                return False
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("WebHDFS check failed for %s: %s", hdfs_path, e)

        return False

    def get_file_status(self, hdfs_path: str) -> Optional[dict]:
        """Get file status details from HDFS."""
        path = hdfs_path
        if path.startswith("hdfs://"):
            path = "/" + path.split("/", 3)[-1] if path.count("/") >= 3 else path

        url = f"{self.base_url}/webhdfs/v1{path}"
        params = {"op": "GETFILESTATUS", "user.name": self.user}

        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json().get("FileStatus")
        except requests.RequestException as e:
            logger.warning("WebHDFS status failed for %s: %s", hdfs_path, e)

        return None

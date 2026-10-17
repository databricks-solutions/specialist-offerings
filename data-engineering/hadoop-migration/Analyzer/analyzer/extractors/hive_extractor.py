"""Extract code artifacts from Oozie <hive>/<hive2> action definitions."""

import xml.etree.ElementTree as ET
from typing import List

from analyzer.models import CodeArtifact


def extract_hive_action(action_elem: ET.Element, app_path: str = "") -> dict:
    """Extract artifacts from a <hive> or <hive2> action element.

    Returns dict with keys: script, query, artifacts, dependencies
    """
    result = {
        "script": None,
        "query": None,
        "artifacts": [],
        "dependencies": [],
    }

    for child in action_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        text = (child.text or "").strip()

        if tag == "script" and text:
            result["script"] = text
            script_path = _resolve_path(text, app_path)
            result["artifacts"].append(CodeArtifact(
                path=script_path,
                location_type="hdfs",
                artifact_type="hql",
            ))

        elif tag == "query" and text:
            result["query"] = text
            result["artifacts"].append(CodeArtifact(
                path=text,
                location_type="embedded",
                artifact_type="sql",
            ))

        elif tag == "file" and text:
            file_path = _resolve_path(text, app_path)
            ext = text.rsplit(".", 1)[-1] if "." in text else "unknown"
            result["dependencies"].append(CodeArtifact(
                path=file_path,
                location_type="hdfs",
                artifact_type=ext,
            ))

    return result


def _resolve_path(path: str, app_path: str) -> str:
    """Resolve a relative path against the Oozie app path."""
    if path.startswith("/") or path.startswith("hdfs://"):
        return path
    if app_path:
        return f"{app_path.rstrip('/')}/{path}"
    return path

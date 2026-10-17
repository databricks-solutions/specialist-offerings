"""Extract code artifacts from Oozie <shell> action definitions."""

import xml.etree.ElementTree as ET
from typing import List

from analyzer.models import CodeArtifact


def extract_shell_action(action_elem: ET.Element, app_path: str = "") -> dict:
    """Extract artifacts from a <shell> action element.

    Returns dict with keys: exec_script, artifacts, dependencies
    """
    result = {
        "exec_script": None,
        "artifacts": [],
        "dependencies": [],
    }

    for child in action_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        text = (child.text or "").strip()

        if tag == "exec" and text:
            result["exec_script"] = text
            script_path = _resolve_path(text, app_path)
            result["artifacts"].append(CodeArtifact(
                path=script_path,
                location_type="hdfs",
                artifact_type="sh",
            ))

        elif tag == "file" and text:
            file_path = _resolve_path(text, app_path)
            ext = text.rsplit(".", 1)[-1] if "." in text else "unknown"
            result["dependencies"].append(CodeArtifact(
                path=file_path,
                location_type="hdfs",
                artifact_type=ext,
            ))

        elif tag == "argument":
            pass  # Arguments don't represent code artifacts

    return result


def _resolve_path(path: str, app_path: str) -> str:
    """Resolve a relative path against the Oozie app path."""
    if path.startswith("/") or path.startswith("hdfs://"):
        return path
    if app_path:
        return f"{app_path.rstrip('/')}/{path}"
    return path

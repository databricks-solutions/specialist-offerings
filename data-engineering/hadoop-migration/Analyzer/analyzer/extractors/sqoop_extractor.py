"""Extract code artifacts from Oozie <sqoop> action definitions."""

import re
import xml.etree.ElementTree as ET
from typing import List

from analyzer.models import CodeArtifact


def extract_sqoop_action(action_elem: ET.Element, app_path: str = "") -> dict:
    """Extract artifacts from a <sqoop> action element.

    Returns dict with keys: command, jdbc_url, table, target_dir, artifacts, dependencies
    """
    result = {
        "command": None,
        "jdbc_url": None,
        "table": None,
        "target_dir": None,
        "artifacts": [],
        "dependencies": [],
    }

    # Sqoop can use <command> (single string) or <arg> elements
    args = []
    for child in action_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        text = (child.text or "").strip()

        if tag == "command" and text:
            result["command"] = text
            args = text.split()

        elif tag == "arg" and text:
            args.append(text)

        elif tag == "file" and text:
            file_path = _resolve_path(text, app_path)
            ext = text.rsplit(".", 1)[-1] if "." in text else "unknown"
            result["dependencies"].append(CodeArtifact(
                path=file_path,
                location_type="hdfs",
                artifact_type=ext,
            ))

    # Parse args for JDBC URL, table, target-dir
    _parse_sqoop_args(args, result)

    return result


def _parse_sqoop_args(args: list, result: dict):
    """Parse sqoop command-line arguments."""
    i = 0
    while i < len(args):
        arg = args[i]
        next_arg = args[i + 1] if i + 1 < len(args) else None

        if arg == "--connect" and next_arg:
            result["jdbc_url"] = next_arg
            i += 2
        elif arg == "--table" and next_arg:
            result["table"] = next_arg
            i += 2
        elif arg == "--target-dir" and next_arg:
            result["target_dir"] = next_arg
            i += 2
        elif arg == "--export-dir" and next_arg:
            result["target_dir"] = next_arg
            i += 2
        elif arg == "--hive-table" and next_arg:
            result["table"] = next_arg
            i += 2
        else:
            i += 1


def _resolve_path(path: str, app_path: str) -> str:
    """Resolve a relative path against the Oozie app path."""
    if path.startswith("/") or path.startswith("hdfs://"):
        return path
    if app_path:
        return f"{app_path.rstrip('/')}/{path}"
    return path

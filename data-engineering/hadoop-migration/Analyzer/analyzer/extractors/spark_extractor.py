"""Extract code artifacts from Oozie <spark> action definitions."""

import xml.etree.ElementTree as ET
from typing import List

from analyzer.models import CodeArtifact


# Oozie namespace
NS = {"oozie": "uri:oozie:workflow:0.5",
      "spark": "uri:oozie:spark-action:0.2"}


def extract_spark_action(action_elem: ET.Element, app_path: str = "") -> dict:
    """Extract artifacts from a <spark> action element.

    Returns dict with keys: jar, main_class, spark_opts, artifacts, dependencies
    """
    result = {
        "jar": None,
        "main_class": None,
        "spark_opts": None,
        "artifacts": [],
        "dependencies": [],
    }

    # Try various namespace combinations
    spark_elem = None
    for ns_prefix in ["spark", "oozie", ""]:
        if ns_prefix:
            spark_elem = action_elem.find(f"{{{NS.get(ns_prefix, '')}}}")
        else:
            spark_elem = action_elem.find("spark")
        if spark_elem is not None:
            break

    # Often the spark action is directly under the action element
    if spark_elem is None:
        spark_elem = action_elem

    # Find elements by local name (namespace-agnostic)
    for child in spark_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        text = (child.text or "").strip()

        if tag == "jar" and text:
            result["jar"] = text
            jar_path = _resolve_path(text, app_path)
            artifact_type = "jar" if text.endswith(".jar") else "py"
            result["artifacts"].append(CodeArtifact(
                path=jar_path,
                location_type="hdfs",
                artifact_type=artifact_type,
            ))

        elif tag == "class" and text:
            result["main_class"] = text

        elif tag == "spark-opts" and text:
            result["spark_opts"] = text
            # Parse --jars and --files from spark-opts
            _parse_spark_opts(text, app_path, result)

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


def _parse_spark_opts(opts: str, app_path: str, result: dict):
    """Parse --jars, --files, --py-files from spark-opts string."""
    parts = opts.split()
    i = 0
    while i < len(parts):
        if parts[i] in ("--jars", "--files", "--py-files") and i + 1 < len(parts):
            paths = parts[i + 1].split(",")
            for p in paths:
                p = p.strip()
                if p:
                    resolved = _resolve_path(p, app_path)
                    ext = p.rsplit(".", 1)[-1] if "." in p else "unknown"
                    result["dependencies"].append(CodeArtifact(
                        path=resolved,
                        location_type="hdfs",
                        artifact_type=ext,
                    ))
            i += 2
        else:
            i += 1

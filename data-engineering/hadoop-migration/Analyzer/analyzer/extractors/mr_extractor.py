"""Extract code artifacts from Oozie <map-reduce> action definitions."""

import xml.etree.ElementTree as ET
from typing import List

from analyzer.models import CodeArtifact


# Common Hadoop config properties that reference JARs or classes
JAR_PROPERTIES = [
    "mapred.jar",
    "mapreduce.job.jar",
    "oozie.libpath",
]

CLASS_PROPERTIES = [
    "mapred.mapper.class",
    "mapred.reducer.class",
    "mapreduce.job.map.class",
    "mapreduce.job.reduce.class",
    "mapreduce.map.class",
    "mapreduce.reduce.class",
]


def extract_mr_action(action_elem: ET.Element, app_path: str = "") -> dict:
    """Extract artifacts from a <map-reduce> action element.

    Returns dict with keys: jar, mapper_class, reducer_class, artifacts, dependencies
    """
    result = {
        "jar": None,
        "mapper_class": None,
        "reducer_class": None,
        "artifacts": [],
        "dependencies": [],
    }

    # Parse <configuration> block for properties
    for child in action_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

        if tag == "configuration":
            _parse_configuration(child, app_path, result)

        elif tag == "file":
            text = (child.text or "").strip()
            if text:
                file_path = _resolve_path(text, app_path)
                ext = text.rsplit(".", 1)[-1] if "." in text else "unknown"
                result["dependencies"].append(CodeArtifact(
                    path=file_path,
                    location_type="hdfs",
                    artifact_type=ext,
                ))

        elif tag == "archive":
            text = (child.text or "").strip()
            if text:
                file_path = _resolve_path(text, app_path)
                result["dependencies"].append(CodeArtifact(
                    path=file_path,
                    location_type="hdfs",
                    artifact_type="archive",
                ))

    return result


def _parse_configuration(config_elem: ET.Element, app_path: str, result: dict):
    """Parse <configuration> block for JAR and class properties."""
    for prop in config_elem:
        prop_tag = prop.tag.split("}")[-1] if "}" in prop.tag else prop.tag
        if prop_tag != "property":
            continue

        name_elem = None
        value_elem = None
        for child in prop:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "name":
                name_elem = child
            elif child_tag == "value":
                value_elem = child

        if name_elem is None or value_elem is None:
            continue

        name = (name_elem.text or "").strip()
        value = (value_elem.text or "").strip()

        if not name or not value:
            continue

        if name in JAR_PROPERTIES:
            result["jar"] = value
            jar_path = _resolve_path(value, app_path)
            result["artifacts"].append(CodeArtifact(
                path=jar_path,
                location_type="hdfs",
                artifact_type="jar",
            ))

        elif name in CLASS_PROPERTIES:
            if "mapper" in name or ".map." in name or name.endswith(".map.class"):
                result["mapper_class"] = value
            else:
                result["reducer_class"] = value


def _resolve_path(path: str, app_path: str) -> str:
    """Resolve a relative path against the Oozie app path."""
    if path.startswith("/") or path.startswith("hdfs://"):
        return path
    if app_path:
        return f"{app_path.rstrip('/')}/{path}"
    return path

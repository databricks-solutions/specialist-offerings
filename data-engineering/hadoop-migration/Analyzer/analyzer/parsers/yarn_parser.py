"""Parse YARN ResourceManager application dump JSON files."""

import glob
import json
import logging
import os
import re
from typing import List, Optional, Tuple

from analyzer.models import CodeArtifact, WorkloadInventoryItem, WorkloadType

logger = logging.getLogger(__name__)

# Oozie launcher pattern: oozie:launcher:T=<type>:W=<wf-name>:A=<action-name>:ID=<id>
OOZIE_LAUNCHER_PATTERN = re.compile(
    r"^oozie:launcher:T=(\w+):W=(.+?):A=(.+?):ID=(.+)$"
)

# Hive query pattern — app name starts with SQL
HIVE_QUERY_PATTERN = re.compile(r"^(SELECT|INSERT|CREATE|DROP|ALTER|LOAD|SET|USE)\b", re.IGNORECASE)

# Sqoop pattern
SQOOP_PATTERN = re.compile(r"sqoop", re.IGNORECASE)


def _classify_app_type(app: dict) -> WorkloadType:
    """Determine workload type from YARN app metadata."""
    yarn_type = app.get("applicationType", "").upper()
    name = app.get("name", "")

    if yarn_type == "SPARK":
        return WorkloadType.SPARK
    if yarn_type == "MAPREDUCE":
        # Check if it's Oozie-launched
        oozie_match = OOZIE_LAUNCHER_PATTERN.match(name)
        if oozie_match:
            action_type = oozie_match.group(1).lower()
            type_map = {
                "spark": WorkloadType.SPARK,
                "hive": WorkloadType.HIVE,
                "hive2": WorkloadType.HIVE,
                "sqoop": WorkloadType.SQOOP,
                "shell": WorkloadType.SHELL,
                "map-reduce": WorkloadType.MAPREDUCE,
            }
            return type_map.get(action_type, WorkloadType.MAPREDUCE)

        # Check name patterns
        if HIVE_QUERY_PATTERN.match(name):
            return WorkloadType.HIVE
        if SQOOP_PATTERN.search(name):
            return WorkloadType.SQOOP
        if name.endswith(".jar"):
            return WorkloadType.MAPREDUCE

        return WorkloadType.MAPREDUCE

    return WorkloadType.UNKNOWN


def _extract_oozie_info(name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract Oozie workflow name, action name, and workflow ID from app name."""
    match = OOZIE_LAUNCHER_PATTERN.match(name)
    if match:
        return match.group(2), match.group(3), match.group(4)
    return None, None, None


def _infer_entry_point(app: dict) -> Optional[str]:
    """Infer the entry point from the app name."""
    name = app.get("name", "")
    yarn_type = app.get("applicationType", "").upper()

    if name.endswith(".jar"):
        return name
    if yarn_type == "SPARK" and "." in name and not name.startswith("SELECT"):
        return name  # e.g., "org.apache.spark.examples.SparkPi"
    return None


def _infer_code_artifacts(app: dict) -> List[CodeArtifact]:
    """Infer code artifacts from app metadata."""
    artifacts = []
    name = app.get("name", "")

    if name.endswith(".jar"):
        artifacts.append(CodeArtifact(
            path=name,
            location_type="local",
            artifact_type="jar",
        ))

    return artifacts


def parse_yarn_dump(file_path: str) -> List[WorkloadInventoryItem]:
    """Parse a single YarnApplicationDump JSON file."""
    logger.info("Parsing YARN dump: %s", file_path)

    with open(file_path, "r") as f:
        data = json.load(f)

    apps = data.get("apps", {}).get("app", [])
    items = []

    for app in apps:
        app_id = app.get("id", "")
        name = app.get("name", "")
        workload_type = _classify_app_type(app)
        wf_name, action_name, wf_id = _extract_oozie_info(name)

        tags = []
        if wf_name:
            tags.append("oozie-launched")
        if workload_type == WorkloadType.HIVE and HIVE_QUERY_PATTERN.match(name):
            tags.append("hive-initiated")

        item = WorkloadInventoryItem(
            workload_id=app_id,
            workload_name=name,
            workload_type=workload_type,
            user=app.get("user", ""),
            queue=app.get("queue", ""),
            entry_point=_infer_entry_point(app),
            code_artifacts=_infer_code_artifacts(app),
            oozie_workflow_name=wf_name,
            oozie_workflow_id=wf_id,
            yarn_app_id=app_id,
            source="yarn",
            tags=tags,
            final_status=app.get("finalStatus"),
            started_time=app.get("startedTime"),
            finished_time=app.get("finishedTime"),
            elapsed_time=app.get("elapsedTime"),
            memory_seconds=app.get("memorySeconds"),
            vcore_seconds=app.get("vcoreSeconds"),
            diagnostics=app.get("diagnostics") or None,
        )
        items.append(item)

    logger.info("Parsed %d YARN applications", len(items))
    return items


def find_and_parse_yarn_dumps(base_dir: str) -> List[WorkloadInventoryItem]:
    """Find all YarnApplicationDump JSON files under base_dir and parse them."""
    pattern = os.path.join(base_dir, "YARN", "**", "YarnApplicationDump*.json")
    files = glob.glob(pattern, recursive=True)

    if not files:
        logger.warning("No YARN dump files found under %s", base_dir)
        return []

    all_items = []
    for f in sorted(files):
        all_items.extend(parse_yarn_dump(f))
    return all_items

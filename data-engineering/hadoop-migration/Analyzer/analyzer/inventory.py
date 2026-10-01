"""InventoryBuilder: orchestrate parsers, Oozie client, and correlate data."""

import logging
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

from analyzer.config import AnalyzerConfig
from analyzer.connectors.oozie_client import OozieClient
from analyzer.connectors.webhdfs_client import WebHDFSClient
from analyzer.extractors.hive_extractor import extract_hive_action
from analyzer.extractors.mr_extractor import extract_mr_action
from analyzer.extractors.shell_extractor import extract_shell_action
from analyzer.extractors.spark_extractor import extract_spark_action
from analyzer.extractors.sqoop_extractor import extract_sqoop_action
from analyzer.extractors.subworkflow_extractor import extract_subworkflow_action
from analyzer.models import CodeArtifact, WorkloadInventoryItem, WorkloadType
from analyzer.parsers.impala_parser import find_and_parse_impala_queries
from analyzer.parsers.spark_parser import find_and_parse_spark_apps
from analyzer.parsers.yarn_parser import find_and_parse_yarn_dumps

logger = logging.getLogger(__name__)

# Oozie launcher name pattern
OOZIE_LAUNCHER_PATTERN = re.compile(
    r"^oozie:launcher:T=(\w+):W=(.+?):A=(.+?):ID=(.+)$"
)

# Map Oozie action XML tag to extractor function
ACTION_EXTRACTORS = {
    "spark": extract_spark_action,
    "hive": extract_hive_action,
    "hive2": extract_hive_action,
    "sqoop": extract_sqoop_action,
    "shell": extract_shell_action,
    "map-reduce": extract_mr_action,
    "sub-workflow": extract_subworkflow_action,
}

# Map Oozie action type to WorkloadType
ACTION_TYPE_MAP = {
    "spark": WorkloadType.SPARK,
    "hive": WorkloadType.HIVE,
    "hive2": WorkloadType.HIVE,
    "sqoop": WorkloadType.SQOOP,
    "shell": WorkloadType.SHELL,
    "map-reduce": WorkloadType.MAPREDUCE,
}


class InventoryBuilder:
    """Build a unified workload inventory from profiler output and Oozie."""

    def __init__(self, config: AnalyzerConfig):
        self.config = config
        self.oozie_client: Optional[OozieClient] = None
        self.webhdfs_client: Optional[WebHDFSClient] = None

    def _init_oozie(self):
        """Initialize Oozie client if configured."""
        if self.config.oozie.url:
            self.oozie_client = OozieClient(
                base_url=self.config.oozie.url,
                auth=self.config.oozie.auth,
                kerberos_principal=self.config.oozie.kerberos_principal,
                timeout=self.config.oozie.timeout,
            )

    def _init_webhdfs(self):
        """Initialize WebHDFS client if enabled."""
        if self.config.webhdfs.enabled:
            self.webhdfs_client = WebHDFSClient(
                base_url=self.config.webhdfs.url,
                user=self.config.webhdfs.user,
            )

    def build_from_profiler(self) -> List[WorkloadInventoryItem]:
        """Build inventory from profiler output only (no Oozie)."""
        base_dir = self.config.profiler_output.base_dir
        if not base_dir:
            logger.error("profiler_output.base_dir not configured")
            return []

        # Parse all profiler sources
        yarn_items = find_and_parse_yarn_dumps(base_dir)
        spark_items = find_and_parse_spark_apps(base_dir)
        impala_items = find_and_parse_impala_queries(base_dir)

        # Merge YARN + Spark HS (Spark HS may have additional detail)
        merged = self._merge_yarn_spark(yarn_items, spark_items)

        # Add Impala (these are separate — no YARN overlap)
        merged.extend(impala_items)

        logger.info("Built inventory with %d items from profiler output", len(merged))
        return merged

    def build_from_oozie(self) -> List[WorkloadInventoryItem]:
        """Build inventory from Oozie only (no profiler output)."""
        self._init_oozie()
        if not self.oozie_client:
            logger.error("Oozie not configured")
            return []

        items = []
        workflows = self.oozie_client.list_workflows(self.config.oozie.max_jobs)

        for wf in workflows:
            wf_id = wf.get("id", "")
            wf_name = wf.get("appName", "")
            app_path = wf.get("appPath", "")

            try:
                definition_xml = self.oozie_client.get_workflow_definition(wf_id)
                wf_items = self._parse_workflow_xml(definition_xml, wf_id, wf_name, app_path)
                items.extend(wf_items)
            except Exception as e:
                logger.warning("Failed to get definition for workflow %s: %s", wf_id, e)

        logger.info("Built inventory with %d items from Oozie", len(items))
        return items

    def build_full(self) -> List[WorkloadInventoryItem]:
        """Build full inventory from profiler + Oozie, with correlation."""
        profiler_items = self.build_from_profiler()
        oozie_items = self.build_from_oozie()

        # Correlate: match YARN apps to Oozie actions
        merged = self._correlate(profiler_items, oozie_items)

        logger.info("Built full inventory with %d items", len(merged))
        return merged

    def verify_paths(self, items: List[WorkloadInventoryItem]) -> List[WorkloadInventoryItem]:
        """Verify HDFS paths for all code artifacts using WebHDFS."""
        self._init_webhdfs()
        if not self.webhdfs_client:
            logger.warning("WebHDFS not enabled; skipping path verification")
            return items

        for item in items:
            for artifact in item.code_artifacts + item.dependencies:
                if artifact.location_type == "hdfs":
                    artifact.verified_exists = self.webhdfs_client.file_exists(artifact.path)

        return items

    def _merge_yarn_spark(self, yarn_items: List[WorkloadInventoryItem],
                          spark_items: List[WorkloadInventoryItem]) -> List[WorkloadInventoryItem]:
        """Merge YARN and Spark HS items by app ID, preferring the richer record."""
        yarn_by_id = {item.workload_id: item for item in yarn_items}
        spark_by_id = {item.workload_id: item for item in spark_items}

        merged = []
        seen_ids = set()

        for app_id, yarn_item in yarn_by_id.items():
            seen_ids.add(app_id)
            if app_id in spark_by_id:
                # Merge: YARN has queue/timing, Spark HS may have more detail
                spark_item = spark_by_id[app_id]
                yarn_item.source = "yarn+spark_hs"
                if spark_item.entry_point and not yarn_item.entry_point:
                    yarn_item.entry_point = spark_item.entry_point
                if spark_item.tags:
                    yarn_item.tags = list(set(yarn_item.tags + spark_item.tags))
                if spark_item.code_artifacts:
                    existing_paths = {a.path for a in yarn_item.code_artifacts}
                    for a in spark_item.code_artifacts:
                        if a.path not in existing_paths:
                            yarn_item.code_artifacts.append(a)
            merged.append(yarn_item)

        # Add Spark HS items not in YARN (shouldn't happen often)
        for app_id, spark_item in spark_by_id.items():
            if app_id not in seen_ids:
                merged.append(spark_item)

        return merged

    def _parse_workflow_xml(self, xml_str: str, wf_id: str, wf_name: str,
                            app_path: str) -> List[WorkloadInventoryItem]:
        """Parse a workflow.xml and extract inventory items for each action."""
        items = []
        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError as e:
            logger.warning("Failed to parse workflow XML for %s: %s", wf_id, e)
            return items

        # Find all <action> elements (namespace-agnostic)
        for action in root.iter():
            tag = action.tag.split("}")[-1] if "}" in action.tag else action.tag
            if tag != "action":
                continue

            action_name = action.get("name", "")

            # Look for known action types within this <action>
            for child in action:
                child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

                if child_tag in ACTION_EXTRACTORS:
                    extractor = ACTION_EXTRACTORS[child_tag]
                    extracted = extractor(child, app_path)

                    workload_type = ACTION_TYPE_MAP.get(child_tag, WorkloadType.UNKNOWN)
                    action_id = f"{wf_id}@{action_name}"

                    entry_point = None
                    if child_tag == "spark":
                        entry_point = extracted.get("main_class") or extracted.get("jar")
                    elif child_tag in ("hive", "hive2"):
                        entry_point = extracted.get("script")
                    elif child_tag == "sqoop":
                        entry_point = f"sqoop-{extracted.get('table', 'unknown')}"
                    elif child_tag == "shell":
                        entry_point = extracted.get("exec_script")
                    elif child_tag == "map-reduce":
                        entry_point = extracted.get("mapper_class") or extracted.get("jar")

                    item = WorkloadInventoryItem(
                        workload_id=action_id,
                        workload_name=f"{wf_name}:{action_name}",
                        workload_type=workload_type,
                        user="",
                        queue="",
                        entry_point=entry_point,
                        code_artifacts=extracted.get("artifacts", []),
                        dependencies=extracted.get("dependencies", []),
                        oozie_workflow_id=wf_id,
                        oozie_workflow_name=wf_name,
                        oozie_app_path=app_path,
                        source="oozie",
                        tags=["oozie-defined"],
                    )
                    items.append(item)

                    # Handle sub-workflows recursively
                    if child_tag == "sub-workflow" and extracted.get("app_path"):
                        item.tags.append("sub-workflow")

        return items

    def _correlate(self, profiler_items: List[WorkloadInventoryItem],
                   oozie_items: List[WorkloadInventoryItem]) -> List[WorkloadInventoryItem]:
        """Correlate profiler items with Oozie items.

        YARN apps launched by Oozie have names like:
        oozie:launcher:T=<type>:W=<wf-name>:A=<action-name>:ID=<wf-id>
        """
        # Index Oozie items by (workflow_name, action_name)
        oozie_index: Dict[str, WorkloadInventoryItem] = {}
        for item in oozie_items:
            if item.oozie_workflow_name and ":" in item.workload_name:
                parts = item.workload_name.split(":", 1)
                if len(parts) == 2:
                    key = (parts[0], parts[1])
                    oozie_index[f"{key[0]}:{key[1]}"] = item

        merged = []
        for item in profiler_items:
            match = OOZIE_LAUNCHER_PATTERN.match(item.workload_name)
            if match:
                wf_name = match.group(2)
                action_name = match.group(3)
                key = f"{wf_name}:{action_name}"

                if key in oozie_index:
                    oozie_item = oozie_index.pop(key)
                    # Enrich profiler item with Oozie details
                    item.code_artifacts.extend(oozie_item.code_artifacts)
                    item.dependencies.extend(oozie_item.dependencies)
                    item.oozie_workflow_id = oozie_item.oozie_workflow_id
                    item.oozie_workflow_name = oozie_item.oozie_workflow_name
                    item.oozie_app_path = oozie_item.oozie_app_path
                    if oozie_item.entry_point:
                        item.entry_point = oozie_item.entry_point
                    item.source = "oozie+yarn"
                    item.tags = list(set(item.tags + oozie_item.tags))

            merged.append(item)

        # Add remaining Oozie-only items
        for item in oozie_index.values():
            merged.append(item)

        return merged

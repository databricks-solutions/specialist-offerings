"""Extract sub-workflow references from Oozie <sub-workflow> action definitions."""

import xml.etree.ElementTree as ET
from typing import Optional


def extract_subworkflow_action(action_elem: ET.Element, app_path: str = "") -> dict:
    """Extract sub-workflow app-path from a <sub-workflow> action.

    Returns dict with keys: app_path, propagate_configuration
    """
    result = {
        "app_path": None,
        "propagate_configuration": False,
    }

    for child in action_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        text = (child.text or "").strip()

        if tag == "app-path" and text:
            result["app_path"] = text

        elif tag == "propagate-configuration":
            result["propagate_configuration"] = True

    return result

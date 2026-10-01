"""Configuration loader for the Analyzer."""

import os
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class ProfilerOutputConfig:
    base_dir: str = ""


@dataclass
class OozieConfig:
    url: str = "http://localhost:11000"
    auth: str = "simple"
    kerberos_principal: str = ""
    max_jobs: int = 5000
    timeout: int = 30


@dataclass
class WebHDFSConfig:
    enabled: bool = False
    url: str = "http://localhost:9870"
    user: str = "hdfs"


@dataclass
class OutputConfig:
    format: str = "json"
    dir: str = "./analyzer-output"


@dataclass
class ComplexityConfig:
    enabled: bool = False
    rules_dir: str = ""
    local_code_dir: str = ""


@dataclass
class AnalyzerConfig:
    profiler_output: ProfilerOutputConfig = field(default_factory=ProfilerOutputConfig)
    oozie: OozieConfig = field(default_factory=OozieConfig)
    webhdfs: WebHDFSConfig = field(default_factory=WebHDFSConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    complexity: ComplexityConfig = field(default_factory=ComplexityConfig)


def load_config(config_path: str) -> AnalyzerConfig:
    """Load configuration from a YAML file."""
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    config = AnalyzerConfig()

    if "profiler_output" in raw:
        po = raw["profiler_output"]
        config.profiler_output = ProfilerOutputConfig(
            base_dir=os.path.expanduser(po.get("base_dir", "")),
        )

    if "oozie" in raw:
        oz = raw["oozie"]
        config.oozie = OozieConfig(
            url=oz.get("url", "http://localhost:11000"),
            auth=oz.get("auth", "simple"),
            kerberos_principal=oz.get("kerberos_principal", ""),
            max_jobs=oz.get("max_jobs", 5000),
            timeout=oz.get("timeout", 30),
        )

    if "webhdfs" in raw:
        wh = raw["webhdfs"]
        config.webhdfs = WebHDFSConfig(
            enabled=wh.get("enabled", False),
            url=wh.get("url", "http://localhost:9870"),
            user=wh.get("user", "hdfs"),
        )

    if "output" in raw:
        out = raw["output"]
        config.output = OutputConfig(
            format=out.get("format", "json"),
            dir=os.path.expanduser(out.get("dir", "./analyzer-output")),
        )

    if "complexity" in raw:
        cx = raw["complexity"]
        rules_dir = cx.get("rules_dir", "")
        if rules_dir and not os.path.isabs(rules_dir):
            rules_dir = os.path.join(os.path.dirname(config_path), rules_dir)
        config.complexity = ComplexityConfig(
            enabled=cx.get("enabled", False),
            rules_dir=os.path.expanduser(rules_dir) if rules_dir else "",
            local_code_dir=os.path.expanduser(cx.get("local_code_dir", "")),
        )

    return config

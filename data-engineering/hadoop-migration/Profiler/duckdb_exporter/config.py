"""Configuration loader for the DuckDB exporter."""

import os
from dataclasses import dataclass, field

import yaml


@dataclass
class ProfilerOutputConfig:
    base_dir: str = ""


@dataclass
class OutputConfig:
    db_path: str = "./hadoop_profiler.duckdb"
    overwrite: bool = True


@dataclass
class CostRatesConfig:
    dbu_rate: float = 0.15
    vm_rate: float = 0.10


@dataclass
class SourcesConfig:
    yarn: bool = True
    spark: bool = True
    impala: bool = True
    cm: bool = True


@dataclass
class ExporterConfig:
    profiler_output: ProfilerOutputConfig = field(default_factory=ProfilerOutputConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    cost_rates: CostRatesConfig = field(default_factory=CostRatesConfig)
    sources: SourcesConfig = field(default_factory=SourcesConfig)


def load_config(config_path: str) -> ExporterConfig:
    """Load configuration from a YAML file."""
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    config = ExporterConfig()

    if "profiler_output" in raw:
        po = raw["profiler_output"]
        config.profiler_output = ProfilerOutputConfig(
            base_dir=os.path.expanduser(po.get("base_dir", "")),
        )

    if "output" in raw:
        out = raw["output"]
        config.output = OutputConfig(
            db_path=os.path.expanduser(out.get("db_path", "./hadoop_profiler.duckdb")),
            overwrite=out.get("overwrite", True),
        )

    if "cost_rates" in raw:
        cr = raw["cost_rates"]
        config.cost_rates = CostRatesConfig(
            dbu_rate=float(cr.get("dbu_rate", 0.15)),
            vm_rate=float(cr.get("vm_rate", 0.10)),
        )

    if "sources" in raw:
        src = raw["sources"]
        config.sources = SourcesConfig(
            yarn=src.get("yarn", True),
            spark=src.get("spark", True),
            impala=src.get("impala", True),
            cm=src.get("cm", True),
        )

    return config

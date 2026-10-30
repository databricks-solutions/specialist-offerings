from __future__ import annotations

import configparser
import os
from pathlib import Path

from ..util.errors import ConfigError


def from_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise ConfigError(f"secret env var {name} is not set (reference env:{name})")
    return v


def from_dbx_profile(profile: str) -> str:
    """Local development only: `client_secret` of a profile in ~/.databrickscfg."""
    cfg = configparser.ConfigParser()
    cfg.read(os.environ.get("DATABRICKS_CONFIG_FILE", Path.home() / ".databrickscfg"))
    if profile not in cfg or "client_secret" not in cfg[profile]:
        raise ConfigError(f"profile [{profile}] in ~/.databrickscfg has no client_secret")
    return cfg[profile]["client_secret"]

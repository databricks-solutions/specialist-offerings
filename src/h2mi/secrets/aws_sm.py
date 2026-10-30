from __future__ import annotations

import json

from ..util.errors import ConfigError

# Key names seen for a Databricks SP secret stored as JSON (the Glue IRC connection uses the USER_MANAGED_* name).
_JSON_KEYS = ("client_secret", "USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET", "SP_CLIENT_SECRET", "secret")


def from_aws_sm(spec: str, *, region: str | None = None, profile: str | None = None) -> str:
    import boto3  # optional dependency: pip install 'h2mi[aws]'

    name, _, key = spec.partition("#")
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    raw = session.client("secretsmanager", region_name=region).get_secret_value(SecretId=name)["SecretString"]
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    for k in ((key,) if key else _JSON_KEYS):
        if k in doc:
            return doc[k]
    raise ConfigError(f"secret {name} has none of the keys {key or _JSON_KEYS}")

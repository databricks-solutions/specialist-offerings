"""Resolve a secret *reference* to its value at runtime.

Reference forms (design §6, §9.1):
    vault:<path>#<key>            the customer's secret store (provider to be wired in vault.py)
    aws-sm:<name>[#<json-key>]    AWS Secrets Manager (lab / cloud-landing)
    env:<NAME>                    environment variable injected by the scheduler or a vault agent
    dbx-profile:<profile>         client_secret of a ~/.databrickscfg profile (local development only)
The `scheme://rest` spelling is accepted too. Never print the returned value.
"""
from __future__ import annotations

from ..util.errors import ConfigError
from . import aws_sm, env, vault

SCHEMES = ("vault", "aws-sm", "env", "dbx-profile")


def parse_ref(ref: str) -> tuple[str, str]:
    scheme, sep, rest = ref.partition(":")
    if not sep or scheme not in SCHEMES:
        raise ConfigError(f"secret reference must be one of {[s + ':<...>' for s in SCHEMES]} (got {ref!r})")
    return scheme, rest[2:] if rest.startswith("//") else rest


def resolve(ref: str, *, region: str | None = None, profile: str | None = None) -> str:
    scheme, rest = parse_ref(ref)
    if scheme == "env":
        return env.from_env(rest)
    if scheme == "aws-sm":
        return aws_sm.from_aws_sm(rest, region=region, profile=profile)
    if scheme == "vault":
        return vault.from_vault(rest)
    return env.from_dbx_profile(rest)


def sp_credential(client_id: str, secret_ref: str, **kw) -> str:
    """`<client_id>:<secret>` for Iceberg's `credential` property (OAuth client-credentials, auto-refreshing)."""
    return f"{client_id}:{resolve(secret_ref, **kw)}"

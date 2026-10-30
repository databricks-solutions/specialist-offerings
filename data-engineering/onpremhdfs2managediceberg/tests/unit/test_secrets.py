import json
import sys
import types

import pytest

from h2mi import secrets
from h2mi.util.errors import ConfigError


def test_env_reference(monkeypatch):
    monkeypatch.setenv("SP_SECRET", "s3cr3t")
    assert secrets.resolve("env:SP_SECRET") == "s3cr3t"
    assert secrets.resolve("env://SP_SECRET") == "s3cr3t"
    assert secrets.sp_credential("abc", "env:SP_SECRET") == "abc:s3cr3t"


def test_missing_env_and_bad_scheme(monkeypatch):
    monkeypatch.delenv("NOPE", raising=False)
    with pytest.raises(ConfigError, match="NOPE"):
        secrets.resolve("env:NOPE")
    with pytest.raises(ConfigError):
        secrets.resolve("plaintext-secret-value")


def test_aws_sm_json_secret(monkeypatch):
    class FakeClient:
        def get_secret_value(self, SecretId):
            assert SecretId == "my/sp"
            return {"SecretString": json.dumps({"USER_MANAGED_CLIENT_APPLICATION_CLIENT_SECRET": "glue-style"})}

    class FakeSession:
        def __init__(self, profile_name=None):
            pass

        def client(self, name, region_name=None):
            return FakeClient()

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(Session=FakeSession))
    assert secrets.resolve("aws-sm:my/sp", region="us-east-1") == "glue-style"


def test_dbx_profile(monkeypatch, tmp_path):
    cfgfile = tmp_path / "databrickscfg"
    cfgfile.write_text("[lab]\nhost = x\nclient_id = abc\nclient_secret = from-profile\n")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", str(cfgfile))
    assert secrets.resolve("dbx-profile:lab") == "from-profile"


def test_vault_not_wired():
    with pytest.raises(NotImplementedError):
        secrets.resolve("vault:secret/h2mi#client_secret")

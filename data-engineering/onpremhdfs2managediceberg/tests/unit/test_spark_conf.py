import pytest

from h2mi.spark import session as s


def test_uc_conf_has_lab_validated_keys_and_no_secret():
    conf = s.uc_catalog_conf("my-ws.example.com", "cat1", "us-east-1", run_id="r1")
    assert conf["spark.sql.catalog.uc.uri"] == "https://my-ws.example.com/api/2.1/unity-catalog/iceberg-rest"
    assert conf["spark.sql.catalog.uc.oauth2-server-uri"].endswith("/oidc/v1/token")
    assert conf["spark.sql.catalog.uc.token-exchange-enabled"] == "false"
    assert conf["spark.sql.catalog.uc.header.X-Iceberg-Access-Delegation"] == "vended-credentials"
    assert conf["spark.sql.catalog.uc.header.X-H2MI-Run-Id"] == "r1"
    assert not any(k.endswith(".credential") for k in conf)


def test_attach_uc_sets_credential_last():
    class FakeSpark:
        class conf:
            store = {}

            @classmethod
            def set(cls, k, v):
                cls.store[k] = v

    s.attach_uc(FakeSpark, "h", "c", "r", credential="id:secret")
    assert FakeSpark.conf.store["spark.sql.catalog.uc.credential"] == "id:secret"


def test_mandatory_conf():
    c = s.mandatory_conf("trim")
    assert c["spark.sql.session.timeZone"] == "UTC" and c["spark.sql.ansi.enabled"] == "true"
    assert c["spark.sql.sources.partitionOverwriteMode"] == "dynamic"
    assert c["spark.sql.readSideCharPadding"] == "false" and "credential" in c["spark.redaction.regex"]


def test_iceberg_version_gates():
    assert "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0" in s.iceberg_packages("spark35", "1.11.0")
    for bad in ("1.8.1", "1.7.2", "1.5.2"):
        with pytest.raises(ValueError, match="VER-01"):
            s.iceberg_packages("spark41", bad)


def test_hms_conf():
    c = s.hms_catalog_conf("thrift://hms:9083", lock_enabled=False)
    assert c["spark.sql.catalog.hms.type"] == "hive" and c["spark.sql.catalog.hms.engine.hive.lock-enabled"] == "false"

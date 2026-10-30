"""Spark session and catalog configuration (design §5.1, §5.2). The mandatory conf is set here, not by users.

Lab-validated settings baked in:
  * `credential=<id>:<secret>` (client-credentials, auto-refresh); never a static token for long jobs (SEC-01).
  * `token-exchange-enabled=false`: Databricks OIDC rejects Iceberg's default token-exchange refresh, so without it the
    catalog token dies at ~60 min and long jobs fail at commit (lab 2026-10-22, Iceberg 1.11.0 and 1.12.0).
  * `X-Iceberg-Access-Delegation: vended-credentials` + S3FileIO.
  * Iceberg >= 1.11.0; never 1.8.x (VER-01, VER-02).
The secret is injected at RUNTIME with attach_uc(), after the session exists: it never lands in spark-defaults, the
command line or the Spark UI Environment tab (SEC-02).
"""
from __future__ import annotations

from ..config.models import EnvConfig

PROFILES = {"spark41": "4.1_2.13", "spark40": "4.0_2.13", "spark35": "3.5_2.12"}     # VER-03
ICEBERG_MIN = (1, 11, 0)


def host_url(host: str) -> str:
    h = host.rstrip("/")
    return h if h.startswith("http") else f"https://{h}"


def iceberg_packages(profile: str = "spark41", iceberg_version: str = "1.12.0", with_hive: bool = True) -> list[str]:
    v = tuple(int(x) for x in iceberg_version.split(".")[:3])
    if v < ICEBERG_MIN or v[:2] == (1, 8):
        raise ValueError(f"Iceberg {iceberg_version} is not allowed: need >= 1.11.0 and never 1.8.x (VER-01, VER-02)")
    pkgs = [f"org.apache.iceberg:iceberg-spark-runtime-{PROFILES[profile]}:{iceberg_version}",
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}"]
    if with_hive:
        pkgs.append(f"org.apache.iceberg:iceberg-hive-metastore:{iceberg_version}")
    return pkgs


def mandatory_conf(char_policy: str = "preserve_padded") -> dict[str, str]:
    """§5.2 rows that are not catalog definitions."""
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.session.timeZone": "UTC",                                   # DT-02
        "spark.sql.ansi.enabled": "true",                                      # VER-08: bad casts throw, never NULL
        "spark.sql.sources.partitionOverwriteMode": "dynamic",                 # INC-05
        "spark.sql.readSideCharPadding": "true" if char_policy == "preserve_padded" else "false",   # TYP-05
        "spark.redaction.regex": "(?i)secret|password|token|access[.]key|credential",
    }


def uc_catalog_conf(host: str, uc_catalog: str, region: str, name: str = "uc",
                    run_id: str | None = None) -> dict[str, str]:
    """UC IRC catalog conf WITHOUT the credential (that is added by attach_uc at runtime)."""
    p, h = f"spark.sql.catalog.{name}", host_url(host)
    conf = {
        p: "org.apache.iceberg.spark.SparkCatalog",
        f"{p}.type": "rest",
        f"{p}.uri": f"{h}/api/2.1/unity-catalog/iceberg-rest",
        f"{p}.warehouse": uc_catalog,
        f"{p}.rest.auth.type": "oauth2",
        f"{p}.oauth2-server-uri": f"{h}/oidc/v1/token",
        f"{p}.scope": "all-apis",
        f"{p}.token-exchange-enabled": "false",
        f"{p}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"{p}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{p}.client.region": region,
    }
    if run_id:
        conf[f"{p}.header.X-H2MI-Run-Id"] = run_id                          # AUD-04 (audit capture is L-4)
    return conf


def hms_catalog_conf(hms_uri: str, name: str = "hms", warehouse: str | None = None,
                     lock_enabled: bool = True) -> dict[str, str]:
    """Iceberg HiveCatalog on the on-prem HMS (sources + h2mi_ctl). lock_enabled=False only for embedded Derby."""
    p = f"spark.sql.catalog.{name}"
    conf = {p: "org.apache.iceberg.spark.SparkCatalog", f"{p}.type": "hive", f"{p}.uri": hms_uri}
    if warehouse:
        conf[f"{p}.warehouse"] = warehouse
    if not lock_enabled:
        conf[f"{p}.engine.hive.lock-enabled"] = "false"
    return conf


def attach_uc(spark, host: str, uc_catalog: str, region: str, credential: str, name: str = "uc",
              run_id: str | None = None) -> None:
    """Register the UC catalog on a live session. Catalogs initialize lazily, so call this before the first `uc.`
    reference. `credential` is `<client_id>:<secret>` from h2mi.secrets.sp_credential; it is only ever passed here."""
    for k, v in uc_catalog_conf(host, uc_catalog, region, name, run_id).items():
        spark.conf.set(k, v)
    spark.conf.set(f"spark.sql.catalog.{name}.credential", credential)


def session(env: EnvConfig, app_name: str, *, char_policy: str = "preserve_padded", hive_support: bool = True,
            extra_conf: dict[str, str] | None = None):
    """Build the on-prem session: session catalog with Hive support + `hms` Iceberg catalog + mandatory conf.
    Jars come from spark-submit (--jars / --packages), not from here (VER-05). Attach `uc` afterwards."""
    from pyspark.sql import SparkSession

    b = SparkSession.builder.appName(app_name)
    conf = {**mandatory_conf(char_policy), **hms_catalog_conf(env.source.hms_uri), **(extra_conf or {})}
    for k, v in conf.items():
        b = b.config(k, v)
    if hive_support:
        b = b.enableHiveSupport()
    return b.getOrCreate()


def iceberg_runtime_version(spark) -> str:
    try:
        return spark._jvm.org.apache.iceberg.IcebergBuild.version()
    except Exception as e:  # noqa: BLE001
        return f"<unknown: {type(e).__name__}>"

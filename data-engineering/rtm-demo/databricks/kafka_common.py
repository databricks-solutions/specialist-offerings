"""Shared Kafka connection helpers for the Customer RTM harness.

Centralizes the Kafka option block so producers and pipelines stay auth-agnostic:
switch SASL/SCRAM <-> MSK IAM in ONE place. Import this from every producer/pipeline.

Usage:
    from kafka_common import kafka_read_options, kafka_write_options, CFG
    reader = spark.readStream.format("kafka").options(**kafka_read_options("txn-source"))
"""
import os


class CFG:
    BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "<KAFKA_BROKER_EIP>:9094")
    SECRET_SCOPE = os.environ.get("KAFKA_SECRET_SCOPE", "customer_rtm_kafka")
    SASL_USER_KEY = os.environ.get("KAFKA_SASL_USER_KEY", "username")
    SASL_PASS_KEY = os.environ.get("KAFKA_SASL_PASS_KEY", "password")
    # auth mode: "scram" (self-managed EC2, default) | "iam" (Amazon MSK)
    MODE = os.environ.get("KAFKA_AUTH_MODE", "scram")
    # security protocol: SASL_PLAINTEXT (test) | SASL_SSL (prod-like / MSK)
    SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    UC_CATALOG = os.environ.get("UC_CATALOG", "<CATALOG>")
    UC_SCHEMA = os.environ.get("UC_SCHEMA", "customer_rtm_streaming")
    CHECKPOINT_VOL = os.environ.get(
        "CHECKPOINT_VOL",
        "/Volumes/<CATALOG>/customer_rtm_streaming/checkpoints",
    )
    # Environment (resolved 2026-07-06):
    #   Databricks profile: aws_sandbox
    #     workspace: https://<WORKSPACE_HOST>.cloud.databricks.com
    #   AWS profile: sbx  | account <AWS_ACCOUNT_ID> | region us-west-2


def _sasl_options():
    """Return the auth-specific kafka.* options for the configured MODE."""
    if CFG.MODE == "iam":
        # Amazon MSK with IAM — requires aws-msk-iam-auth jar on the cluster.
        return {
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": (
                "software.amazon.msk.auth.iam.IAMLoginModule required;"
            ),
            "kafka.sasl.client.callback.handler.class": (
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
            ),
        }

    # default: SASL/SCRAM-SHA-512 (self-managed EC2). Secrets read at call time so this
    # module imports cleanly outside a Databricks runtime (e.g. local lint).
    try:
        user = dbutils.secrets.get(CFG.SECRET_SCOPE, CFG.SASL_USER_KEY)  # noqa: F821
        pw = dbutils.secrets.get(CFG.SECRET_SCOPE, CFG.SASL_PASS_KEY)  # noqa: F821
    except NameError:  # not running inside Databricks
        user = os.environ.get("KAFKA_SASL_USER", "dbxclient")
        pw = os.environ.get("KAFKA_SASL_PASS", "")
    # NOTE: Spark's Kafka connector SHADES its Kafka classes under `kafkashaded.`.
    # The JAAS config must reference the shaded ScramLoginModule, or executors throw
    # "No LoginModule found for org.apache.kafka.common.security.scram.ScramLoginModule".
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{user}" password="{pw}";'
    )
    return {
        "kafka.security.protocol": CFG.SECURITY_PROTOCOL,
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": jaas,
    }


def kafka_read_options(topic, starting_offsets="latest", min_partitions=6):
    opts = {
        "kafka.bootstrap.servers": CFG.BOOTSTRAP,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        "failOnDataLoss": "false",
    }
    # RTM rejects minPartitions ("minpartitions is not compatible with real time mode"); it
    # manages partition->task mapping itself. Pass min_partitions=None from RTM pipelines.
    if min_partitions is not None:
        opts["minPartitions"] = str(min_partitions)
    opts.update(_sasl_options())
    return opts


def kafka_write_options(topic):
    opts = {
        "kafka.bootstrap.servers": CFG.BOOTSTRAP,
        "topic": topic,
        "kafka.acks": "all",  # durability / exactly-once producer
    }
    opts.update(_sasl_options())
    return opts


def checkpoint(stream_name):
    return f"{CFG.CHECKPOINT_VOL}/{stream_name}"

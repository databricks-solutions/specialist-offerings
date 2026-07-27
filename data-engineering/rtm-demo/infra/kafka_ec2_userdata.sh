#!/usr/bin/env bash
# EC2 user-data: single-broker Apache Kafka (KRaft mode) with SASL/SCRAM-SHA-512.
# Target AMI: Amazon Linux 2023 (x86_64). Instance: t3.large+ recommended.
#
# What this does:
#   - installs Java 17 + Kafka
#   - configures a KRaft combined broker/controller with two listeners:
#       CONTROLLER (internal) and SASL_SSL on :9094 (client)  [see NOTE on TLS below]
#   - creates a SCRAM admin user, starts Kafka as a systemd service
#   - creates the 4 topics used by the harness
#
# NOTE on TLS: for a genuinely throwaway test you may downgrade SASL_SSL -> SASL_PLAINTEXT
#   (set LISTENER_SECURITY=SASL_PLAINTEXT and drop the ssl.* configs) and lock the security
#   group to the workspace egress CIDRs. SASL_SSL requires a keystore (self-signed OK); the
#   commented ssl block below shows where it goes. Databricks side must then trust the cert.
#
# Fill these in (or template via Terraform/cloud-init):
KAFKA_VERSION="3.9.2"   # keep current; old versions 404 on downloads.apache.org (moved to archive)
SCALA_VERSION="2.13"
SCRAM_USER="${SCRAM_USER:-dbxclient}"
SCRAM_PASSWORD="${SCRAM_PASSWORD:-CHANGE_ME_STRONG_PASSWORD}"
ADVERTISED_HOST="${ADVERTISED_HOST:-$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)}"
LISTENER_SECURITY="${LISTENER_SECURITY:-SASL_PLAINTEXT}"   # SASL_PLAINTEXT (test) | SASL_SSL (prod-like)
CLIENT_PORT=9094

set -euxo pipefail

# ---- packages -------------------------------------------------------------
dnf -y install java-17-amazon-corretto-headless tar gzip
useradd -r -m -d /opt/kafka-home kafka || true

cd /opt
# primary mirror keeps only current releases; fall back to archive for older ones
curl -fsSL "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o kafka.tgz \
  || curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o kafka.tgz
tar -xzf kafka.tgz
mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka
chown -R kafka:kafka /opt/kafka
KAFKA=/opt/kafka

# ---- KRaft server.properties ----------------------------------------------
CFG=$KAFKA/config/kraft/server.properties
cat > "$CFG" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=CONTROLLER://:9093,CLIENT://:${CLIENT_PORT}
inter.broker.listener.name=CLIENT
advertised.listeners=CLIENT://${ADVERTISED_HOST}:${CLIENT_PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:${LISTENER_SECURITY}

# SASL/SCRAM
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.client.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${SCRAM_USER}" password="${SCRAM_PASSWORD}";

# storage / durability
log.dirs=/var/lib/kafka-logs
num.partitions=6
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false

# --- SASL_SSL only (uncomment + provide keystore) ---
# ssl.keystore.location=/opt/kafka/ssl/kafka.keystore.jks
# ssl.keystore.password=CHANGE_ME
# ssl.key.password=CHANGE_ME
EOF

mkdir -p /var/lib/kafka-logs && chown -R kafka:kafka /var/lib/kafka-logs

# ---- format storage (KRaft) -----------------------------------------------
KAFKA_CLUSTER_ID="$($KAFKA/bin/kafka-storage.sh random-uuid)"
sudo -u kafka $KAFKA/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c "$CFG" --ignore-formatted

# store the SCRAM credential in the metadata quorum
sudo -u kafka $KAFKA/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c "$CFG" --ignore-formatted \
  --add-scram "SCRAM-SHA-512=[name=${SCRAM_USER},password=${SCRAM_PASSWORD}]" || true

# ---- systemd service -------------------------------------------------------
cat > /etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka (KRaft)
After=network.target

[Service]
Type=simple
User=kafka
Environment=KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
ExecStart=$KAFKA/bin/kafka-server-start.sh $CFG
ExecStop=$KAFKA/bin/kafka-server-stop.sh
Restart=on-failure
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now kafka
sleep 20

# ---- client config for admin ops (topic creation) ------------------------
CLIENT_PROPS=/opt/kafka/config/client.properties
cat > "$CLIENT_PROPS" <<EOF
security.protocol=${LISTENER_SECURITY}
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${SCRAM_USER}" password="${SCRAM_PASSWORD}";
EOF

BOOT="${ADVERTISED_HOST}:${CLIENT_PORT}"
create_topic () {
  $KAFKA/bin/kafka-topics.sh --bootstrap-server "$BOOT" --command-config "$CLIENT_PROPS" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

# 4 topics: 2 sources + 2 sinks
create_topic txn-source 6      # UC-1 source (bank transactions)
create_topic events-source 6   # UC-2 source (generic events)
create_topic fraud-scored 6    # UC-1 sink (scored transactions)
create_topic events-dlq 3      # UC-2 dead-letter (schema failures)

echo "Kafka ready. BOOTSTRAP=${BOOT}  SCRAM_USER=${SCRAM_USER}  SECURITY=${LISTENER_SECURITY}"

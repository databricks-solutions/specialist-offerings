# Ready-to-Use Init Script Templates

## 1. Install System Packages (apt-get)

```bash
#!/bin/bash
# init_install_packages.sh
# Purpose: Install OS-level system packages on Databricks cluster nodes
# Upload to: /Volumes/<catalog>/<schema>/init-scripts/install_packages.sh

set -euo pipefail

LOG_FILE="/tmp/init_install_packages.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date): Starting system package installation..."

apt-get update -y -qq
apt-get install -y -qq \
  gcc \
  g++ \
  libffi-dev \
  libssl-dev \
  libxml2-dev \
  libxslt1-dev \
  zlib1g-dev

echo "$(date): System package installation complete."
```

**Upload instructions:**
```bash
# Using Databricks CLI
databricks fs cp install_packages.sh /Volumes/catalog/schema/init-scripts/install_packages.sh

# Or using dbutils in a notebook
dbutils.fs.put("/Volumes/catalog/schema/init-scripts/install_packages.sh", open("install_packages.sh").read(), True)
```

**Cluster configuration (JSON):**
```json
{
  "cluster_name": "my-cluster",
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/init-scripts/install_packages.sh"
      }
    }
  ]
}
```

---

## 2. Install ODBC Drivers

```bash
#!/bin/bash
# init_odbc_drivers.sh
# Purpose: Install ODBC drivers for external database connectivity
# Upload to: /Volumes/<catalog>/<schema>/init-scripts/odbc_drivers.sh

set -euo pipefail

LOG_FILE="/tmp/init_odbc_drivers.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date): Installing ODBC drivers..."

# Install unixODBC
apt-get update -y -qq
apt-get install -y -qq unixodbc unixodbc-dev

# Install SQL Server ODBC Driver 18
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list
apt-get update -y -qq
ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18

# Verify installation
odbcinst -q -d

echo "$(date): ODBC driver installation complete."
```

**Cluster configuration (JSON):**
```json
{
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/init-scripts/odbc_drivers.sh"
      }
    }
  ]
}
```

---

## 3. Import Custom CA Certificates

```bash
#!/bin/bash
# init_ca_certs.sh
# Purpose: Install custom CA certificates for internal PKI / private endpoints
# Upload to: /Volumes/<catalog>/<schema>/init-scripts/ca_certs.sh
# Also upload your .crt files to: /Volumes/<catalog>/<schema>/certs/

set -euo pipefail

LOG_FILE="/tmp/init_ca_certs.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date): Installing custom CA certificates..."

CERT_VOLUME="/Volumes/catalog/schema/certs"
CERT_DIR="/usr/local/share/ca-certificates/custom"
JAVA_TRUSTSTORE="$JAVA_HOME/lib/security/cacerts"

# Create cert directory
mkdir -p "$CERT_DIR"

# Copy certificates from UC Volume
cp "${CERT_VOLUME}"/*.crt "$CERT_DIR/" 2>/dev/null || echo "No .crt files found in volume"

# Update OS certificate store
update-ca-certificates

# Import into Java truststore (for JDBC connections)
for cert in "$CERT_DIR"/*.crt; do
  if [ -f "$cert" ]; then
    ALIAS=$(basename "$cert" .crt)
    keytool -import -trustcacerts -alias "$ALIAS" \
      -file "$cert" -keystore "$JAVA_TRUSTSTORE" \
      -storepass changeit -noprompt 2>/dev/null || echo "Cert $ALIAS may already exist"
    echo "Imported certificate: $ALIAS"
  fi
done

echo "$(date): CA certificate installation complete."
```

**Cluster configuration (JSON):**
```json
{
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/init-scripts/ca_certs.sh"
      }
    }
  ]
}
```

---

## 4. Mount NFS/EFS

```bash
#!/bin/bash
# init_mount_nfs.sh
# Purpose: Mount NFS/EFS filesystem on Databricks cluster nodes
# Upload to: /Volumes/<catalog>/<schema>/init-scripts/mount_nfs.sh

set -euo pipefail

LOG_FILE="/tmp/init_mount_nfs.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date): Mounting NFS filesystem..."

# Install NFS client
apt-get update -y -qq
apt-get install -y -qq nfs-common

# Configure mount point
NFS_SERVER="${NFS_SERVER_ADDRESS:-fs-12345678.efs.us-east-1.amazonaws.com}"
MOUNT_POINT="/mnt/shared-data"

mkdir -p "$MOUNT_POINT"

# Mount with recommended options for EFS
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 \
  "${NFS_SERVER}:/" "$MOUNT_POINT"

# Verify mount
if mountpoint -q "$MOUNT_POINT"; then
  echo "$(date): NFS mounted successfully at $MOUNT_POINT"
else
  echo "$(date): ERROR - NFS mount failed!"
  exit 1
fi
```

**Cluster configuration (JSON):**
```json
{
  "spark_env_vars": {
    "NFS_SERVER_ADDRESS": "fs-12345678.efs.us-east-1.amazonaws.com"
  },
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/init-scripts/mount_nfs.sh"
      }
    }
  ]
}
```

---

## 5. Configure Custom DNS

```bash
#!/bin/bash
# init_custom_dns.sh
# Purpose: Configure custom DNS resolution for private endpoints
# Upload to: /Volumes/<catalog>/<schema>/init-scripts/custom_dns.sh

set -euo pipefail

LOG_FILE="/tmp/init_custom_dns.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date): Configuring custom DNS..."

# Backup existing resolv.conf
cp /etc/resolv.conf /etc/resolv.conf.bak

# Add custom nameservers (prepend to existing)
CUSTOM_DNS="${CUSTOM_DNS_SERVER:-10.0.0.2}"
SEARCH_DOMAIN="${CUSTOM_SEARCH_DOMAIN:-internal.example.com}"

# Use resolvconf if available, otherwise modify directly
if command -v resolvconf &>/dev/null; then
  echo "nameserver $CUSTOM_DNS" | resolvconf -a eth0.custom
else
  # Prepend custom DNS to resolv.conf
  {
    echo "nameserver $CUSTOM_DNS"
    echo "search $SEARCH_DOMAIN"
    cat /etc/resolv.conf.bak
  } > /etc/resolv.conf
fi

# Verify DNS resolution
if nslookup "test.${SEARCH_DOMAIN}" "$CUSTOM_DNS" &>/dev/null; then
  echo "$(date): Custom DNS configured and verified."
else
  echo "$(date): WARNING - DNS verification failed, but config is applied."
fi
```

**Cluster configuration (JSON):**
```json
{
  "spark_env_vars": {
    "CUSTOM_DNS_SERVER": "10.0.0.2",
    "CUSTOM_SEARCH_DOMAIN": "internal.example.com"
  },
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/init-scripts/custom_dns.sh"
      }
    }
  ]
}
```

---

## 6. Install Monitoring Agents

```bash
#!/bin/bash
# init_monitoring_agent.sh
# Purpose: Install and configure monitoring agent (e.g., Datadog, New Relic, Splunk)
# Upload to: /Volumes/<catalog>/<schema>/init-scripts/monitoring_agent.sh

set -euo pipefail

LOG_FILE="/tmp/init_monitoring_agent.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date): Installing monitoring agent..."

# Example: Datadog Agent
# Set API key via spark_env_vars (never hardcode)
DD_API_KEY="${DD_API_KEY:?ERROR: DD_API_KEY environment variable not set}"
DD_SITE="${DD_SITE:-datadoghq.com}"

# Install Datadog Agent
DD_AGENT_MAJOR_VERSION=7 DD_API_KEY="$DD_API_KEY" DD_SITE="$DD_SITE" \
  bash -c "$(curl -fsSL https://s]aw-install-method.datadoghq.com/scripts/install_script_agent7.sh)"

# Configure Spark integration
cat > /etc/datadog-agent/conf.d/spark.d/conf.yaml << EOF
init_config:

instances:
  - spark_url: http://localhost:4040
    cluster_name: databricks-cluster
    spark_cluster_mode: spark_standalone_mode
EOF

# Add cluster tags
CLUSTER_NAME="${DB_CLUSTER_NAME:-unknown}"
cat >> /etc/datadog-agent/datadog.yaml << EOF
tags:
  - env:databricks
  - cluster_name:${CLUSTER_NAME}
EOF

# Start agent
systemctl start datadog-agent

echo "$(date): Monitoring agent installation complete."
```

**Cluster configuration (JSON):**
```json
{
  "spark_env_vars": {
    "DD_API_KEY": "{{secrets/monitoring/datadog-api-key}}",
    "DD_SITE": "datadoghq.com"
  },
  "init_scripts": [
    {
      "volumes": {
        "destination": "/Volumes/catalog/schema/init-scripts/monitoring_agent.sh"
      }
    }
  ]
}
```

---

## General Best Practices for Init Scripts

1. **Always use `set -euo pipefail`** to fail fast on errors
2. **Log everything** to `/tmp/` for debugging (`/tmp/init_*.log`)
3. **Use environment variables** for configuration (set via spark_env_vars)
4. **Never hardcode secrets** -- use Databricks secret scopes via `{{secrets/scope/key}}`
5. **Keep scripts idempotent** -- they may run on cluster restart
6. **Test on a single-node cluster first** before deploying to production
7. **Check logs** via Cluster > Event Log > Init Scripts if a script fails
8. **Store scripts in UC Volumes** (preferred) over DBFS for governance

#!/bin/bash
# Template: Databricks Init Script
# Converted from EMR Bootstrap Action
#
# EMR Source: <BOOTSTRAP_ACTION_NAME>
# EMR Script: <S3_BOOTSTRAP_SCRIPT_PATH>
# Migration Date: <YYYY-MM-DD>
#
# USAGE:
#   1. Customize this script for your needs
#   2. Upload to Unity Catalog Volume:
#      databricks fs cp init_script.sh dbfs:/Volumes/<catalog>/<schema>/init-scripts/
#   3. Configure on cluster:
#      init_scripts:
#        - volumes:
#            destination: /Volumes/<catalog>/<schema>/init-scripts/init_script.sh
#
# IMPORTANT DIFFERENCES FROM EMR BOOTSTRAP:
#   - Init scripts run as root on Databricks (same as EMR bootstrap)
#   - Init scripts run AFTER the Spark environment is set up
#   - Use /databricks/spark for Spark home (not /usr/lib/spark)
#   - Use /databricks/python for Python (not /usr/bin/python3)
#   - Failures in init scripts will prevent cluster startup
#   - Keep scripts idempotent (safe to re-run)

set -euo pipefail

# ============================================================
# Section 1: System Package Installation
# (Converted from: apt-get/yum install in bootstrap)
# ============================================================

# Example: Install system packages
# apt-get update -qq
# apt-get install -y -qq \
#   libpq-dev \
#   libsasl2-dev \
#   libssl-dev

# ============================================================
# Section 2: Python Package Installation
# (Converted from: pip install in bootstrap)
#
# PREFERRED: Use %pip in notebooks or cluster libraries instead.
# Only use init script for packages that need system-level deps.
# ============================================================

# Example: Install Python packages with system dependencies
# /databricks/python/bin/pip install --quiet \
#   psycopg2-binary \
#   sasl \
#   thrift-sasl

# ============================================================
# Section 3: Environment Variables
# (Converted from: export in bootstrap or EMR configurations)
#
# PREFERRED: Use cluster spark_env_vars config instead.
# Only use init script for vars needed before Spark starts.
# ============================================================

# Example: Set environment variables
# cat >> /databricks/spark/conf/spark-env.sh <<'EOF'
# export CUSTOM_ENV_VAR="value"
# export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
# EOF

# ============================================================
# Section 4: Certificate / Trust Store Installation
# (Converted from: keytool/certificate bootstrap actions)
# ============================================================

# Example: Import custom CA certificate
# CERT_PATH="/tmp/custom-ca.crt"
# KEYSTORE="/usr/lib/jvm/java-11-openjdk-amd64/lib/security/cacerts"
# if [ -f "$CERT_PATH" ]; then
#   keytool -import -trustcacerts -alias custom-ca \
#     -file "$CERT_PATH" -keystore "$KEYSTORE" \
#     -storepass changeit -noprompt
# fi

# ============================================================
# Section 5: Custom JARs / Drivers
# (Converted from: JAR copy in bootstrap)
#
# PREFERRED: Use cluster libraries or UC Volumes instead.
# Only use init script for JARs that must be on the classpath
# before Spark initializes.
# ============================================================

# Example: Copy JDBC driver to Spark jars directory
# cp /dbfs/Volumes/<catalog>/<schema>/drivers/custom-driver.jar \
#   /databricks/jars/

# ============================================================
# Section 6: Configuration File Modifications
# (Converted from: config file edits in bootstrap)
# ============================================================

# Example: Add custom log4j configuration
# cat >> /databricks/spark/conf/log4j2.properties <<'EOF'
# logger.custom.name=com.mycompany
# logger.custom.level=DEBUG
# EOF

echo "Init script completed successfully"

---
name: emr-bootstrap-to-init-scripts
description: "Convert EMR bootstrap actions to Databricks init scripts. Use when: (1) 'convert bootstrap actions', (2) 'EMR bootstrap to init script', (3) 'custom software installation on Databricks clusters', (4) 'init script from EMR setup'."
---

# EMR Bootstrap Actions to Databricks Init Scripts

## Overview

EMR bootstrap actions are scripts that run on every node before Hadoop/Spark applications are installed. Databricks init scripts serve a similar purpose but run during cluster startup after the Spark environment is already set up. Many EMR bootstrap use cases have better alternatives on Databricks (cluster libraries, spark_env_vars, etc.), so init scripts should be a last resort.

## Key Differences

| Aspect | EMR Bootstrap | Databricks Init Script |
|---|---|---|
| Execution timing | Before apps install | After Spark env setup |
| Run as | hadoop user | root |
| Failure behavior | Cluster creation fails | Cluster creation fails |
| Storage | S3 | UC Volumes, DBFS, or workspace files |
| Max scripts | 16 | No limit |
| Scope | Cluster-wide | Cluster-scoped or global |

## Common Patterns

### 1. Package Installation (apt-get/yum)

**EMR**: Bootstrap action with `sudo yum install -y <package>`
**Databricks**: Init script OR cluster libraries. **Prefer cluster libraries** when a library exists (e.g., PyPI, Maven). Use init scripts only for OS-level system packages.

### 2. Python Packages (pip install)

**EMR**: Bootstrap action with `sudo pip install <package>`
**Databricks**: `%pip install` in notebooks or cluster libraries. **PREFERRED over init scripts.** Cluster libraries provide dependency management and conflict resolution.

### 3. Environment Variables

**EMR**: Bootstrap action with `export VAR=value` or writing to `/etc/environment`
**Databricks**: Cluster spark_env_vars configuration. **PREFERRED over init scripts.** Set via cluster JSON config or UI under Advanced Options > Spark > Environment Variables.

### 4. Custom JARs

**EMR**: Bootstrap action copying JARs from S3 to `/usr/lib/spark/jars/`
**Databricks**: Cluster libraries or UC Volumes. **PREFERRED over init scripts.** Upload JARs to UC Volumes and attach as cluster libraries.

### 5. Certificates

**EMR**: Bootstrap action installing custom CA certs
**Databricks**: **Init script is still needed.** Install certs into the JVM truststore and OS cert store.

### 6. Config File Modifications

**EMR**: Bootstrap action modifying Hadoop/Spark config files
**Databricks**: **Init script is still needed** for OS-level config. For Spark config, use cluster Spark conf settings instead.

## Decision Tree: Do I Need an Init Script?

```
Is it a Python package?
  YES --> Use %pip install or cluster library. DONE.
  NO  --> Continue.

Is it a JAR/Maven dependency?
  YES --> Use cluster library from UC Volumes or Maven coordinates. DONE.
  NO  --> Continue.

Is it an environment variable?
  YES --> Use spark_env_vars in cluster config. DONE.
  NO  --> Continue.

Is it a Spark/Hadoop configuration?
  YES --> Use Spark conf in cluster config. DONE.
  NO  --> Continue.

Is it an OS-level package, certificate, mount, driver, or config file?
  YES --> Use an init script.
  NO  --> Ask: can this be handled by a Databricks-native feature? Check docs first.
```

## Template Reference

Use the init script template at:
`/Users/kishore.mannava/cursorprojects/umlaut-poc-emr-claude/templates/init_script_template.sh`

## Related Skills

- **emr-config-migration**: For migrating EMR cluster configurations (Spark, YARN, HDFS settings)
- **emr-infra-migration**: For migrating EMR infrastructure (Terraform/CloudFormation to Databricks Asset Bundles)

# How to Run the Hadoop Profiler

The Hadoop Profiler (`profiler.sh`) extracts cluster metadata and workload data from Hadoop clusters via REST APIs. This runbook documents every step to deploy and execute it on a target cluster.

---

## Prerequisites

- **Bash** environment on a node with network access to cluster management UIs (YARN RM, CM/Ambari, Spark HS)
- **curl** and **openssl** installed
- **Credentials** for Cloudera Manager (CDH) or Ambari (HDP) — a read-only user is sufficient
- Network access to: YARN RM (default 8088), CM (default 7180) or Ambari (default 8080), Spark HS (default 18088/18081)

---

## Step 1: Get the Profiler onto the Target Host

The profiler lives at `src/hadoop/Profiler/` in the repo. Copy it to the target machine:

```bash
# Option A: SCP directly to a cluster edge node
scp -r src/hadoop/Profiler/ user@edge-node:/tmp/Profiler/

# Option B: For Docker-based clusters (like CDH QuickStart on EC2)
scp -i <key.pem> -r src/hadoop/Profiler/ ec2-user@<EC2_IP>:/tmp/Profiler/
ssh -i <key.pem> ec2-user@<EC2_IP> "docker cp /tmp/Profiler <container>:/tmp/Profiler"
```

Ensure the user running the script owns the files:

```bash
chown -R <user>:<group> /tmp/Profiler
chmod +x /tmp/Profiler/profiler.sh
```

---

## Step 2: Encrypt Passwords

All passwords in `profiler.conf` must be AES-256-CBC encrypted. Use this command:

```bash
echo '"YourPassword"' | openssl enc -base64 -e -aes-256-cbc -nosalt -pass pass:mySecretKey
```

**Important caveats:**

- The password **must** be enclosed in double quotes inside single quotes: `echo '"password"'`
- The `mySecretKey` is your encryption key — you'll pass it as an argument when running the profiler
- Ignore any "Deprecated Key" warnings from openssl
- Keep your secret key safe; never store it in the conf file

---

## Step 3: Configure `profiler.conf`

Edit `profiler.conf` with cluster-specific values. The sections to fill depend on your distribution.

### Common Settings (All Distributions)

| Property | Description | Example |
|----------|-------------|---------|
| `PROFILER_OUTPUT_PATH` | Where to write output (blank = script dir) | `/tmp/profiler-output` |
| `RM_SERVER_URL` | YARN Resource Manager hostname(s), comma-separated for HA | `rm1.example.com,rm2.example.com` |
| `RM_SERVER_PORT` | YARN RM port | `8088` |
| `RM_SECURE` | Y if HTTPS | `N` |
| `RM_KERBERIZED` | Y if Kerberos-secured | `N` |
| `RM_APP_FILTER` | Optional YARN app filter | `?user=hive` or blank |
| `SPARK_EXTRACT` | Y to extract Spark HS data | `Y` |
| `SPARK_HS_URL` | Spark History Server hostname | `spark-hs.example.com` |
| `SPARK_HS_PORT` | Spark HS port | `18088` (CDH) or `18081` (HDP) |
| `DISTRIBUTION` | `CDH`, `HDP`, or `OTH` | `CDH` |

### Kerberos Settings (if `RM_KERBERIZED=Y`)

| Property | Description |
|----------|-------------|
| `GOT_KEYTAB` | Y if providing a keytab file |
| `KEYTAB_PATH` | Path to keytab directory |
| `KEYTAB` | Keytab filename |
| `PRINCIPAL` | Kerberos principal |

If not using a keytab, run `kinit` manually before executing the script.

### CDH/CDP Distribution Settings

| Property | Description | Example |
|----------|-------------|---------|
| `IS_CDP` | Y for CDP Private Cloud | `N` |
| `CM_SERVER_URL` | Cloudera Manager hostname | `cm.example.com` |
| `CM_SERVER_PORT` | CM port | `7180` (http) or `7183` (https) |
| `CM_ADMIN_USER` | CM user (read-only is fine) | `admin` |
| `CM_ADMIN_PASSWORD` | Encrypted CM password | `GEwvzjMwpcwEozfMU9Sk5Q==` |
| `CM_CLUSTER` | Cluster display name (spaces OK, special chars need URL encoding) | `"My Cluster"` |
| `CM_SECURED` | Y if CM uses HTTPS | `N` |
| `CM_EXTRACT_IMPALA_QUERIES` | Y to extract Impala queries | `Y` |
| `CM_IMPALA_SERVICE` | Impala service name in CM | `impala` |
| `CM_IMPALA_NUMBER_OF_DAYS` | Days of Impala history to extract | `7` |
| `CM_IMPALA_INTERVAL_MINUTES` | Time window granularity (1,2,3,6,10,20,30,60) | `60` |
| `CM_IMPALA_PAGES` | Pages per interval (1000 queries/page) | `2` |

### HDP Distribution Settings

| Property | Description | Example |
|----------|-------------|---------|
| `IS_HDI` | Y for Azure HDInsight | `N` |
| `AMBARI_ADMIN_USERID` | Ambari user | `admin` |
| `AMBARI_ADMIN_PASSWORD` | Encrypted Ambari password | `S7CSr2wq...` |
| `AMBARI_SERVER` | Ambari hostname | `ambari.example.com` |
| `AMBARI_PORT` | Ambari port | `8080` |
| `CLUSTER_NAME` | Cluster name | `mycluster` |
| `AMBARI_SECURED` | Y if HTTPS | `N` |
| `IS_RANGER_SETUP` | Y to extract Ranger policies | `N` |
| `RANGER_URL` / `RANGER_PORT` / `RANGER_USER` / `RANGER_PWD` | Ranger connection details | |

---

## Step 4: Run the Profiler

```bash
cd /tmp/Profiler
./profiler.sh <yourSecretKey>
```

The secret key must match the one used to encrypt passwords in Step 2.

### What Happens During Execution

1. **Checks run status** — looks for `ExtractTracker/initialrun.txt`
   - First run = **Initial Extract** (full snapshot: apps + nodes + metrics + scheduler + CM/Ambari config)
   - Subsequent runs = **Incremental Extract** (only apps, timeseries, Impala queries)
   - Auto-recovers to initial if prior config files are missing
2. **Decrypts passwords** using the provided secret key
3. **Finds active YARN RM** (iterates HA URLs, checks for ACTIVE state)
4. **Extracts data** via curl to REST APIs based on distribution type
5. **Writes JSON files** to the output directory

### Expected Runtime

- Initial run: 2-5 minutes (more if Impala extraction is enabled with many days)
- Incremental run: 1-2 minutes

---

## Step 5: Verify Output

Output lands in `<PROFILER_OUTPUT_PATH>/Output/` (or `<script_dir>/Output/` if path not set):

```
Output/
├── YARN/<date>/
│   ├── YarnApplicationDump_<ts>.json     # All YARN apps (the key file)
│   ├── YarnNodesDump_<ts>.json           # Cluster nodes (initial only)
│   ├── YarnMetricsDump_<ts>.json         # Cluster resource metrics (initial only)
│   └── YarnSchedulerDump_<ts>.json       # Queue config (initial only)
├── SPARK/<date>/
│   └── Spark_Applications_<ts>.json      # Spark HS apps
├── CM/<date>/                            # CDH only
│   ├── cmServices_<ts>.json              # Service inventory + health
│   ├── cmHosts_<ts>.json                 # Host hardware specs
│   ├── cmConfig_<ts>.json                # Host-level config
│   ├── cmExport_<ts>.json               # Full cluster config export
│   ├── cmHostRoles_<ts>.json             # Role states + uptime
│   ├── cmHDFSUsage_<ts>.json             # HDFS capacity timeseries
│   ├── cmYarnUtilization_<ts>.json       # YARN memory/vcore timeseries
│   ├── cmYarnMemoryAndCPU_<ts>.json      # YARN pool-level utilization
│   ├── cmImpalaUtilization_<ts>.json     # Impala query rate timeseries
│   ├── cmClusterCPUUtilization_<ts>.json # Cluster CPU % timeseries
│   └── cmClusterMemoryUtilization_<ts>.json # Cluster memory % timeseries
├── IMPALA/<date>/                        # CDH only, if enabled
│   └── impala_<date>_<hour>_<range>.json # Individual query batches
├── AMBARI/<date>/                        # HDP only
│   ├── AmbariBlueprint_<ts>.json         # Cluster blueprint
│   ├── AmbariHost_<ts>.json              # Host details
│   ├── AmbariServices_<ts>.json          # Service list
│   ├── AmbariComponents_<ts>.json        # Component-to-host map
│   ├── AmbariStack_<ts>.json             # Stack version
│   ├── AmbariHDFS_<ts>.json              # HDFS NameNode metrics
│   ├── AmbariDN_<ts>.json                # DataNode metrics
│   ├── AmbariRM_<ts>.json                # ResourceManager metrics
│   └── AmbariNM_<ts>.json               # NodeManager metrics
└── RANGER/<date>/                        # If Ranger enabled
    ├── Ranger_Repos_<ts>.json
    └── Ranger_Policies_<ts>.json
```

### Quick Validation

```bash
# Check files were created and are non-empty
find Output/ -name "*.json" -size +0 | wc -l

# Verify YARN dump has apps
python -c "import json; d=json.load(open('Output/YARN/*/YarnApplicationDump_*.json')); print(len(d['apps']['app']), 'apps')"

# Check for errors in CM timeseries files (common on minimal clusters)
grep -l "Connection refused" Output/CM/*/*.json
```

---

## Step 6: Schedule Recurring Runs (Optional)

### Option A: Cron (Recommended)

```bash
# Run every 4 hours for 14 days
0 */4 * * * /tmp/Profiler/profiler.sh mySecretKey >> /tmp/Profiler/profiler.log 2>&1
```

### Option B: Built-in Scheduler

Set in `profiler.conf`:

```
FREQUENCY_OF_EXECUTION=4    # hours between runs (max 24)
NO_OF_DAYS=14               # total days to run
```

Then run once — it will loop internally.

---

## Common Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| `bad decrypt` | Wrong secret key or password not encrypted properly | Re-encrypt: `echo '"password"' \| openssl enc -base64 -e -aes-256-cbc -nosalt -pass pass:KEY` |
| `Bad Cloudera Manager credentials` | Wrong CM user/password | Verify user can log into CM UI; re-encrypt correct password |
| `Active Resource manager URL not found` | RM unreachable or wrong URL/port | Check `curl http://RM_URL:PORT/ws/v1/cluster/info` manually |
| `Impala service 'X' not found` | Wrong `CM_IMPALA_SERVICE` name | Check CM UI for exact service name; or set `CM_EXTRACT_IMPALA_QUERIES=N` |
| `Connection refused` in timeseries files | CM Activity Monitor / Host Monitor not running | Start these services in CM; on QuickStart/minimal clusters, this is expected |
| `Permission denied` creating Output dir | Script user doesn't own the profiler directory | `chown -R user:group /tmp/Profiler` |
| `IMPALA QUERY SCAN LIMIT HIT` | Too many Impala queries per interval | Reduce `CM_IMPALA_INTERVAL_MINUTES` and increase `CM_IMPALA_PAGES` |
| Empty YARN dump | RM history TTL expired or wrong RM URL | Check `yarn.resourcemanager.max-completed-applications` in YARN config |

---

## Data Masking (Optional)

To scrub sensitive data from output, create a `sed.txt` file with substitution rules and uncomment in `profiler.conf`:

```bash
export SEARCH_REPLACE="| sed -f sed.txt"
```

Example `sed.txt`:

```
s/sensitive-hostname/MASKED/g
s/user@email\.com/MASKED_EMAIL/g
```

---

## Example: CDH QuickStart Configuration

```bash
# Config values used:
RM_SERVER_URL=localhost
RM_SERVER_PORT=8088
SPARK_EXTRACT=Y
SPARK_HS_URL=localhost
SPARK_HS_PORT=18088
DISTRIBUTION=CDH
CM_SERVER_URL=localhost
CM_SERVER_PORT=7180
CM_ADMIN_USER=admin
CM_ADMIN_PASSWORD=GEwvzjMwpcwEozfMU9Sk5Q==   # encrypted "admin"
CM_CLUSTER="Cloudera Quickstart"
CM_EXTRACT_IMPALA_QUERIES=N

# Execution:
./profiler.sh myTestSecretKey

# Result: 16 JSON files (YARN: 4, SPARK: 1, CM: 11)
# YARN captured 132 apps from 11 Oozie medallion-pipeline runs
# CM timeseries returned "Connection refused" (Activity Monitor not running on QuickStart)
```

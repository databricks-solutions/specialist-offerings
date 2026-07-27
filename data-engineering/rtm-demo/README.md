# Customer RTM — Two Kafka Streaming Use Cases on Databricks

A working test harness validating two Databricks streaming architectures against a
self-managed Kafka broker on AWS:

| # | Use case | Pattern | Trigger | Sink |
|---|----------|---------|---------|------|
| **UC-1** | Bank fraud detection | Kafka → transform → Kafka | **Real-Time Mode** (`realTime`) | Kafka `fraud-scored` |
| **UC-2** | Event standardization | Kafka → standardize → table | micro-batch (`processingTime`) | Managed **Iceberg** UC table |

> **Why two compute/trigger models?** Real-Time Mode does **not** support a table sink or
> `forEachBatch` — it is Kafka-in / Kafka-out, update mode only. So UC-2 (which writes to a
> UC table) *cannot* use RTM and runs as standard micro-batch with `foreachBatch`.

---

## Architecture

```
                              AWS  (account <AWS_ACCOUNT_ID>, us-west-2)
      ┌─────────────────────────────────────────────────────────────────────────┐
      │                                                                           │
      │   EC2 t3.large  <KAFKA_INSTANCE_ID>   ·   Kafka 3.9.2 (KRaft, no ZK)      │
      │   EIP <KAFKA_BROKER_EIP>   ·   SG <KAFKA_SG_ID>                           │
      │                                                                           │
      │   ┌───────────────────────── listeners ─────────────────────────────┐    │
      │   :  CONTROLLER  :9093  PLAINTEXT       (internal Raft quorum)       :    │
      │   :  INTERNAL    :9092  PLAINTEXT       adv= <KAFKA_PRIVATE_IP> (on-box)  :    │
      │   :  CLIENT      :9094  SASL_SCRAM-512  adv= <KAFKA_BROKER_EIP>  (external) :    │
      │   └──────────────────────────────────────────────────────────────────┘   │
      │                                                                           │
      │   topics:  txn-source(6)   events-source(6)   fraud-scored(6)   events-dlq(3)
      │            retention: 30 min / 200 MB per partition                       │
      └───────────────────────────────────┬───────────────────────────────────────┘
                                           :
                       SASL/SCRAM over :9094  (SG allows cluster egress
                                           :   <WORKSPACE_NAT_EIP> + your admin IP)
                                           :
      ┌────────────────────────────────────┴──────────────────────────────────────┐
      │            Databricks  (workspace <WORKSPACE_HOST>)            │
      │            RTM classic cluster  <RTM_CLUSTER_ID>                        │
      │            DBR 16.4 LTS · Photon · Dedicated · 4 workers · no autoscale     │
      │            spark.databricks.streaming.realTimeMode.enabled = true           │
      │                                                                             │
      │   ┌─ producers (dbldatagen) ─┐                                              │
      │   :  gen_fraud_txns          :····▶ txn-source                              │
      │   :  gen_generic_events      :····▶ events-source                           │
      │   └──────────────────────────┘                                             │
      │                                                                             │
      │   UC-1  Real-Time Mode                                                       │
      │   ┌──────────────────────────────────────────────────────────────────┐     │
      │   : txn-source ─▶ from_json ─▶ stateless fraud scoring ─▶ to_json ────:───▶ fraud-scored
      │   :   (large amount · foreign geo · risky merchant → fraud_score)     :     │
      │   :   outputMode=update   trigger(realTime="5 minutes")               :     │
      │   └──────────────────────────────────────────────────────────────────┘     │
      │                                                                             │
      │   UC-2  Micro-batch → managed Iceberg                                        │
      │   ┌──────────────────────────────────────────────────────────────────┐     │
      │   : events-source ─▶ from_json ─▶ standardize (UPPER/trim/coalesce)   :     │
      │   :   ─▶ foreachBatch: write.format("iceberg").mode("append")         :     │
      │   :        ├──▶ <CATALOG>.customer_rtm_streaming.events_std  (Iceberg)
      │   :        └──▶ bad records ──▶ events-dlq                            :     │
      │   :   trigger(processingTime="30 seconds")                            :     │
      │   └──────────────────────────────────────────────────────────────────┘     │
      │                                                                             │
      │   checkpoints:  /Volumes/<CATALOG>/                  │
      │                          customer_rtm_streaming/checkpoints/<stream>            │
      └─────────────────────────────────────────────────────────────────────────────┘

  legend:   ─── data flow / wire        :::  network boundary / listener box
```

---

## Environment (resolved)

| Thing | Value |
|-------|-------|
| Databricks profile | `aws_sandbox` → `<WORKSPACE_HOST>.cloud.databricks.com` |
| AWS profile | `sbx` → account `<AWS_ACCOUNT_ID>`, region `us-west-2` |
| Kafka broker | EC2 `<KAFKA_INSTANCE_ID>`, EIP `<KAFKA_BROKER_EIP>`, SG `<KAFKA_SG_ID>` |
| RTM cluster | `<RTM_CLUSTER_ID>` (DBR 16.4 LTS, Photon, Dedicated, 4 workers) |
| UC namespace | `<CATALOG>.customer_rtm_streaming` |
| Secrets scope | `customer_rtm_kafka` (keys `username`, `password`) |
| Workspace code | `/Users/<USER_EMAIL>/customer_rtm/` (notebooks) |
| Local state | `~/.customer_rtm/` (eip, sg, iid, rtm_cluster_id, keypair, scram_password) |

---

## Steps to run

All commands assume `export AWS_PROFILE=sbx AWS_REGION=us-west-2` and the Databricks CLI
profile `aws_sandbox`.

### 1. Broker (already provisioned — only if rebuilding)
```bash
export SCRAM_PASSWORD='<strong-password>'
bash infra/provision.sh            # creates EIP, keypair, SG, EC2; installs Kafka; makes topics
# store creds as Databricks secrets (see databricks/01_secrets_setup.md)
```
If the broker already exists, just confirm health and that **your current IP** is allowed
on the SG (your public IP changes over time):
```bash
ssh -i ~/.customer_rtm/customer-rtm-kafka.pem ec2-user@<KAFKA_BROKER_EIP> 'sudo systemctl is-active kafka'
# if SSH/Kafka time out, add your new IP:
MYIP=$(curl -s https://checkip.amazonaws.com)
aws ec2 authorize-security-group-ingress --group-id $(cat ~/.customer_rtm/sg) --protocol tcp --port 22   --cidr $MYIP/32
aws ec2 authorize-security-group-ingress --group-id $(cat ~/.customer_rtm/sg) --protocol tcp --port 9094 --cidr $MYIP/32
```

### 2. Unity Catalog objects (once)
```bash
# run databricks/00_setup_uc.sql on a DBR 16.4 LTS+ warehouse
#   → schema customer_rtm_streaming, checkpoints Volume, managed Iceberg table events_std
```

### 3. Start / confirm the RTM cluster
```bash
databricks clusters start <RTM_CLUSTER_ID> -p aws_sandbox
# confirm the cluster egress IP is on the SG (port 9094). Get it from a notebook:
#   urllib.request.urlopen("https://checkip.amazonaws.com").read()
```

### 4. Deploy the pipelines as persistent jobs

The pipelines run as **continuous Databricks Jobs** (always-on, auto-restart on failure,
one stable Job page each) — not ad-hoc `jobs submit` runs. The producer runs as a
**scheduled** job (every 5 min batch) on its own small cluster so it never competes with
RTM for slots.

| Job | job_id | Type | Compute | Notebook |
|-----|--------|------|---------|----------|
| `customer-uc1-rtm-fraud` | <UC1_JOB_ID> | continuous | big classic `<RTM_CLUSTER_ID>` | `uc1_rtm_fraud` |
| `customer-uc2-iceberg` | <UC2_JOB_ID> | continuous | big classic `<RTM_CLUSTER_ID>` | `uc2_iceberg_standardize` |
| `customer-trickle-producer` | <TRICKLE_JOB_ID> | scheduled `0 0/5 * * * ?` | small classic `<PRODUCER_CLUSTER_ID>` | `gen_trickle` (10 rows/run) |

Job specs are versioned in `databricks/jobs/*.json`. Create/recreate with:
```bash
databricks jobs create --json @databricks/jobs/uc1_rtm_job.json     -p aws_sandbox
databricks jobs create --json @databricks/jobs/uc2_iceberg_job.json -p aws_sandbox
# continuous jobs auto-start; start UC-1 (RTM) first so it grabs its 6 slots before UC-2.
```
To pause/resume without deleting:
```bash
databricks jobs update --json '{"job_id":<id>,"new_settings":{"continuous":{"pause_status":"PAUSED"}}}' -p aws_sandbox
```

### 5. Validate
```bash
# UC-1: scored transactions on fraud-scored (from the broker)
ssh -i ~/.customer_rtm/customer-rtm-kafka.pem ec2-user@<KAFKA_BROKER_EIP> \
  'sudo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <KAFKA_PRIVATE_IP>:9092 \
     --topic fraud-scored --from-beginning --max-messages 5 --timeout-ms 10000'

# UC-2: Iceberg table (run databricks/validate/check_iceberg.sql on the warehouse)
#   expect Provider: iceberg, growing row count, event_type UPPER/trimmed, snapshot history
```

---

## Repo layout

```
customer_rtm/
  README.md
  infra/
    provision.sh            one-shot: EIP + keypair + SG + EC2 + topics
    kafka_ec2_userdata.sh   KRaft install, SASL/SCRAM, dual listeners, topic creation
    security_group.md       SG rules + how to find the workspace egress IP
    topics.md               topic map + console ops
  databricks/
    00_setup_uc.sql         catalog/schema/volume + managed Iceberg table (+ preview probe)
    01_secrets_setup.md      secret-scope commands (SCRAM + MSK/IAM variant)
    kafka_common.py          shared Kafka options (SCRAM⇄IAM swap; RTM minPartitions handling)
    clusters/rtm_cluster.json  RTM cluster spec (DBR 16.4, Photon, Dedicated, 4 workers)
    producers/               gen_fraud_txns.py, gen_generic_events.py (dbldatagen)
    pipelines/               uc1_rtm_fraud.py, uc2_iceberg_standardize.py
    validate/                check_rtm_output.py, check_iceberg.sql
```

---

## Gotchas learned building this (all handled in code)

1. **Kafka broker needs dual listeners** — INTERNAL (private IP, PLAINTEXT, on-box admin) +
   CLIENT (public EIP, SASL/SCRAM, for Databricks). A single listener breaks either local
   admin or external clients (an EC2 box can't reach its own EIP).
2. **SCRAM JAAS must use the shaded class** `kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule`
   — Spark shades its Kafka classes; the plain class fails on executors.
3. **RTM enablement is two parts** — `spark.databricks.streaming.realTimeMode.enabled=true`
   on the cluster (needs restart) **and** `.trigger(realTime="…")` in code.
4. **RTM limitations** — no streaming joins (any kind) yet, and `minPartitions` is rejected.
   UC-1 uses stateless per-transaction scoring only.
5. **RTM needs free slots** — it schedules all stages at once (needs ~6 slots). Don't
   over-subscribe the cluster with other streaming jobs; size accordingly (4 workers here).
6. **Managed Iceberg has no direct streaming sink** — use `foreachBatch` with
   `batch_df.write.format("iceberg").mode("append").saveAsTable(...)`. Batch writes work fine.
7. **8 GB root disk fills fast** — unbounded producers filled the broker disk and crashed
   Kafka. Topics now have 30-min / 200 MB retention caps.
8. **Your public IP changes** — re-add it to the SG (ports 22 + 9094) when SSH/Kafka time out.
9. **`kafka-get-offsets.sh`** is the Kafka 3.9 offsets tool (not `kafka.tools.GetOffsetShell`).

---

## Teardown

```bash
# Databricks — delete the 3 jobs (stops continuous runs), then both clusters + secrets
databricks jobs delete <UC1_JOB_ID> -p aws_sandbox   # customer-uc1-rtm-fraud (continuous)
databricks jobs delete <UC2_JOB_ID> -p aws_sandbox   # customer-uc2-iceberg (continuous)
databricks jobs delete <TRICKLE_JOB_ID> -p aws_sandbox   # customer-trickle-producer (scheduled)
databricks clusters permanent-delete --cluster-id <RTM_CLUSTER_ID> -p aws_sandbox  # big RTM
databricks clusters permanent-delete --cluster-id <PRODUCER_CLUSTER_ID> -p aws_sandbox  # small trickle
databricks secrets delete-scope customer_rtm_kafka -p aws_sandbox

# AWS
aws ec2 terminate-instances --instance-ids <KAFKA_INSTANCE_ID> --region us-west-2
aws ec2 release-address --allocation-id $(cat ~/.customer_rtm/eip_alloc) --region us-west-2
aws ec2 delete-security-group --group-id $(cat ~/.customer_rtm/sg) --region us-west-2   # after instance is gone

# Unity Catalog (optional)
#   DROP TABLE <CATALOG>.customer_rtm_streaming.events_std;
#   DROP SCHEMA <CATALOG>.customer_rtm_streaming CASCADE;
```

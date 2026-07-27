# Kafka SASL credentials → Databricks secrets

The pipelines and producers read the Kafka SASL/SCRAM username & password from a Databricks
secret scope so no credential is ever hardcoded. Create the scope once (Phase 1, after the
EC2 broker is up).

```bash
SCOPE=customer_rtm_kafka

databricks secrets create-scope $SCOPE -p aws_sandbox           # idempotent-ish; ignore "exists"

# store the SCRAM user + password created in kafka_ec2_userdata.sh
databricks secrets put-secret $SCOPE username --string-value "dbxclient"        -p aws_sandbox
databricks secrets put-secret $SCOPE password --string-value "<SCRAM_PASSWORD>" -p aws_sandbox

# verify
databricks secrets list-secrets $SCOPE -p aws_sandbox
```

In notebooks/jobs the code does:
```python
user = dbutils.secrets.get("customer_rtm_kafka", "username")
pw   = dbutils.secrets.get("customer_rtm_kafka", "password")
```

## If you switch to MSK + IAM instead of SASL/SCRAM
- No username/password secret needed — auth is via the cluster instance profile / IAM role.
- Install the `aws-msk-iam-auth` jar on the cluster and set:
  ```
  kafka.security.protocol = SASL_SSL
  kafka.sasl.mechanism    = AWS_MSK_IAM
  kafka.sasl.jaas.config  = software.amazon.msk.auth.iam.IAMLoginModule required;
  kafka.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
  ```
- The `kafka_common.py` helper has a `MODE=iam` branch stub for this.

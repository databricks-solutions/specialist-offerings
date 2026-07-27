#!/usr/bin/env bash
# One-shot provisioning for the Customer RTM harness. REVIEW before running.
# Creates: security group + EC2 Kafka broker (default VPC, public), then prints next steps.
# Everything is tagged project=customer_rtm ephemeral=true for clean teardown.
#
# Resolved environment (2026-07-06):
#   AWS profile sbx | account <AWS_ACCOUNT_ID> | region us-west-2
#   Databricks profile aws_sandbox | <WORKSPACE_HOST>
#
# Prereqs: SCRAM_PASSWORD set to a strong value; admin IP confirmed.
set -euxo pipefail

export AWS_PROFILE=sbx
export AWS_REGION=us-west-2
REGION=us-west-2
VPC=<VPC_ID>                 # default VPC
SUBNET=<SUBNET_ID>           # us-west-2a, public (MapPublicIpOnLaunch=true)
ADMIN_IP="${ADMIN_IP:-<YOUR_ADMIN_IP>}/32" # your IP for SSH (resolved 2026-07-06)
SCRAM_USER="${SCRAM_USER:-dbxclient}"
: "${SCRAM_PASSWORD:?set SCRAM_PASSWORD to a strong value before running}"
KEY_NAME="${KEY_NAME:-customer-rtm-kafka}"    # EC2 keypair name (create or reuse)

# Amazon Linux 2023 AMI (SSM public parameter — always current)
AMI=$(aws ssm get-parameters --names \
  /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query 'Parameters[0].Value' --output text)

# --- 1. security group -----------------------------------------------------
SG=$(aws ec2 create-security-group \
  --group-name customer-rtm-kafka-sg \
  --description "Customer RTM Kafka broker" \
  --vpc-id "$VPC" \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=project,Value=customer_rtm},{Key=ephemeral,Value=true}]' \
  --query 'GroupId' --output text)

# SSH from admin only
aws ec2 authorize-security-group-ingress --group-id "$SG" \
  --protocol tcp --port 22 --cidr "$ADMIN_IP"
# Kafka client 9094 — START restricted to admin IP; ADD the workspace NAT egress /32 AFTER
# the RTM cluster is up (see step 4). Do NOT open 9094 to 0.0.0.0/0.
aws ec2 authorize-security-group-ingress --group-id "$SG" \
  --protocol tcp --port 9094 --cidr "$ADMIN_IP"

# --- 2. launch broker ------------------------------------------------------
# user-data passes SCRAM creds + advertised host via cloud-init env; the script reads them.
USERDATA=$(SCRAM_USER="$SCRAM_USER" SCRAM_PASSWORD="$SCRAM_PASSWORD" \
  envsubst < "$(dirname "$0")/kafka_ec2_userdata.sh" | base64)

IID=$(aws ec2 run-instances \
  --image-id "$AMI" --instance-type t3.large \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG" --subnet-id "$SUBNET" \
  --associate-public-ip-address \
  --user-data "$USERDATA" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=customer-rtm-kafka},{Key=project,Value=customer_rtm},{Key=ephemeral,Value=true}]' \
  --query 'Instances[0].InstanceId' --output text)

aws ec2 wait instance-running --instance-ids "$IID"
PUBIP=$(aws ec2 describe-instances --instance-ids "$IID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

set +x
cat <<EOF

================= PROVISIONED =================
Security group : $SG
Instance       : $IID
Public IP      : $PUBIP
Bootstrap      : $PUBIP:9094   (broker advertises its PRIVATE ip by default — see NOTE)

NEXT STEPS
1. Wait ~3 min for user-data to install Kafka + create topics. Check:
     ssh ec2-user@$PUBIP 'sudo systemctl status kafka --no-pager'
2. NOTE: kafka_ec2_userdata.sh advertises the instance's local-ipv4. For a PUBLIC broker,
   set ADVERTISED_HOST=$PUBIP in the user-data (re-run envsubst) OR edit
   advertised.listeners on the box to CLIENT://$PUBIP:9094 and restart kafka.
3. Store secrets:
     export KAFKA_BOOTSTRAP=$PUBIP:9094
     databricks secrets create-scope customer_rtm_kafka -p aws_sandbox
     databricks secrets put-secret customer_rtm_kafka username --string-value "$SCRAM_USER" -p aws_sandbox
     databricks secrets put-secret customer_rtm_kafka password --string-value "<SCRAM_PASSWORD>" -p aws_sandbox
4. Create RTM cluster, get its egress IP from a notebook (checkip.amazonaws.com), then:
     aws ec2 authorize-security-group-ingress --group-id $SG --protocol tcp --port 9094 --cidr <NAT_EIP>/32
5. Run UC setup, producers, pipelines (see README Phases 2-7).

TEARDOWN
  aws ec2 terminate-instances --instance-ids $IID
  aws ec2 delete-security-group --group-id $SG   # after instance is gone
===============================================
EOF

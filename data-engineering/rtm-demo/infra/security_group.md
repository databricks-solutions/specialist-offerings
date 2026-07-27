# Kafka EC2 security group

Single-broker Kafka must be reachable from the Databricks workspace compute (both the RTM
classic cluster and whatever runs UC-2 / the producers). Lock inbound to the workspace's
outbound source, not `0.0.0.0/0`.

## Inbound rules

| Port | Protocol | Source | Why |
|------|----------|--------|-----|
| 9094 | TCP | Databricks workspace egress CIDR(s) / NAT gateway EIP | Kafka client (SASL) traffic |
| 22   | TCP | your admin IP /32 | SSH for setup/debug (remove after) |

> The controller listener (9093) stays broker-local — do **not** expose it.

## Determining the workspace egress source

`<WORKSPACE_HOST>` is a Databricks-managed classic workspace (AWS account
<AWS_ACCOUNT_ID>, us-west-2). Its classic clusters run in a **Databricks-managed VPC** that we
cannot co-locate the broker into, so the broker lives in the default VPC with a public IP and
egresses to us. Classic clusters egress via the workspace VPC's NAT gateway. Options:

1. **Same-VPC / VPC peering** — place the EC2 in (or peer to) the workspace data-plane VPC and
   allow the private subnet CIDRs on 9094. Most secure; no public exposure. *(Preferred; needs
   the workspace VPC/subnet IDs from whoever administers the account.)*
2. **Public broker + NAT EIP allowlist** — give the EC2 a public IP and allow only the
   workspace NAT gateway Elastic IP(s) on 9094. Simplest for a short-lived test.

To find the NAT EIP(s), from a workspace notebook:
```python
import urllib.request
print(urllib.request.urlopen("https://checkip.amazonaws.com", timeout=5).read().decode())
```
Run it on the **classic** cluster (serverless egresses differently) to capture the actual
egress IP, then allowlist that /32.

## Outbound
Default allow-all outbound is fine (broker initiates nothing external for this test).

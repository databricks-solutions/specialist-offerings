# EMR VPC Networking Patterns to Databricks

## Overview

EMR clusters run in your VPC on EC2 instances you configure. Databricks also deploys compute into your VPC (customer-managed VPC) or into a Databricks-managed VPC. This guide maps common EMR networking patterns to their Databricks equivalents.

## Architecture Comparison

### EMR Network Architecture

```
Your VPC
├── Public Subnet (optional)
│   └── EMR Master (if public access needed)
├── Private Subnet
│   ├── EMR Master Node
│   ├── EMR Core Nodes
│   └── EMR Task Nodes
├── Security Groups
│   ├── EMR Master SG (SSH, Spark UI, YARN)
│   └── EMR Slave SG (inter-node communication)
├── NAT Gateway (for private subnet internet access)
├── S3 VPC Endpoint (gateway)
└── VPC Peering (to other services)
```

### Databricks Network Architecture

```
Your VPC (Customer-Managed)
├── Public Subnet (NAT for outbound)
│   └── NAT Gateway
├── Private Subnet 1 (Databricks nodes)
│   ├── Driver Node
│   └── Worker Nodes
├── Private Subnet 2 (Databricks nodes — second AZ)
│   ├── Driver Node
│   └── Worker Nodes
├── Security Groups
│   └── Databricks SG (all inter-node, outbound to control plane)
├── S3 VPC Endpoint (gateway)
├── STS VPC Endpoint (interface — for cross-account auth)
└── PrivateLink (optional — to Databricks control plane)
```

## Pattern-by-Pattern Migration

### Pattern 1: Public Subnet EMR -> Databricks-Managed VPC

**EMR**: Cluster in a public subnet with public IPs, internet-accessible Spark UI.

**Databricks**: Use Databricks-managed VPC (default). Databricks creates and manages the VPC, subnets, NAT, and security groups. No networking setup required.

```hcl
# Terraform: Databricks workspace with managed VPC (simplest)
resource "databricks_mws_workspaces" "this" {
  account_id     = var.databricks_account_id
  workspace_name = "emr-migration-workspace"
  aws_region     = "us-east-1"

  # No network_id = Databricks-managed VPC
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
}
```

**When to use**: Simple setups, no VPC peering requirements, no strict network isolation policies.

### Pattern 2: Private Subnet EMR -> Customer-Managed VPC

**EMR**: Cluster in a private subnet with NAT gateway for outbound internet, S3 endpoint for data access.

**Databricks**: Customer-managed VPC with the same pattern.

**Requirements for Databricks customer-managed VPC:**
- Two private subnets in different AZs (for the data plane)
- NAT gateway for outbound access to Databricks control plane
- S3 gateway endpoint
- STS interface endpoint (recommended)
- Dedicated subnets (not shared with other workloads)

```hcl
# VPC and Subnets
resource "aws_vpc" "databricks" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "databricks-vpc" }
}

resource "aws_subnet" "private_1" {
  vpc_id            = aws_vpc.databricks.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-1a"
  tags              = { Name = "databricks-private-1" }
}

resource "aws_subnet" "private_2" {
  vpc_id            = aws_vpc.databricks.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "us-east-1b"
  tags              = { Name = "databricks-private-2" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.databricks.id
  cidr_block              = "10.0.0.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true
  tags                    = { Name = "databricks-public" }
}

# NAT Gateway
resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.databricks.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }
}

resource "aws_route_table_association" "private_1" {
  subnet_id      = aws_subnet.private_1.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_2" {
  subnet_id      = aws_subnet.private_2.id
  route_table_id = aws_route_table.private.id
}

# S3 Gateway Endpoint
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.databricks.id
  service_name      = "com.amazonaws.us-east-1.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}

# STS Interface Endpoint
resource "aws_vpc_endpoint" "sts" {
  vpc_id              = aws_vpc.databricks.id
  service_name        = "com.amazonaws.us-east-1.sts"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.databricks_endpoint.id]
  private_dns_enabled = true
}

# Security Group for Databricks
resource "aws_security_group" "databricks" {
  vpc_id = aws_vpc.databricks.id
  name   = "databricks-data-plane"

  # Allow all internal traffic (Databricks nodes communicate freely)
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  # Allow all outbound (NAT gateway handles routing)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "databricks-data-plane-sg" }
}

# Register VPC with Databricks
resource "databricks_mws_networks" "this" {
  account_id         = var.databricks_account_id
  network_name       = "emr-migration-network"
  vpc_id             = aws_vpc.databricks.id
  subnet_ids         = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids = [aws_security_group.databricks.id]
}

resource "databricks_mws_workspaces" "this" {
  account_id     = var.databricks_account_id
  workspace_name = "emr-migration-workspace"
  aws_region     = "us-east-1"

  network_id               = databricks_mws_networks.this.network_id
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
}
```

### Pattern 3: VPC Peering -> VPC Peering or PrivateLink

**EMR**: VPC peering to RDS, Redshift, or other services.

**Databricks**: Same VPC peering works with customer-managed VPC.

```hcl
# Peer Databricks VPC with existing services VPC
resource "aws_vpc_peering_connection" "databricks_to_services" {
  vpc_id      = aws_vpc.databricks.id
  peer_vpc_id = var.services_vpc_id
  auto_accept = true
}

# Add route in Databricks private route table
resource "aws_route" "to_services" {
  route_table_id            = aws_route_table.private.id
  destination_cidr_block    = var.services_vpc_cidr  # e.g., "10.1.0.0/16"
  vpc_peering_connection_id = aws_vpc_peering_connection.databricks_to_services.id
}

# Add route in services VPC route table
resource "aws_route" "to_databricks" {
  route_table_id            = var.services_route_table_id
  destination_cidr_block    = "10.0.0.0/16"  # Databricks VPC CIDR
  vpc_peering_connection_id = aws_vpc_peering_connection.databricks_to_services.id
}
```

### Pattern 4: PrivateLink (No Public Internet)

For highly secure environments where Databricks control plane access must not traverse the public internet.

**Databricks PrivateLink** provides:
- **Front-end PrivateLink**: User access to Databricks web UI and API without public internet
- **Back-end PrivateLink**: Data plane to control plane communication without public internet

```hcl
# Back-end PrivateLink (data plane -> control plane)
resource "aws_vpc_endpoint" "databricks_backend" {
  vpc_id              = aws_vpc.databricks.id
  service_name        = "com.amazonaws.vpce.us-east-1.vpce-svc-<backend-id>"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  security_group_ids  = [aws_security_group.databricks_endpoint.id]
  private_dns_enabled = false  # Use Databricks-provided DNS
}

# Front-end PrivateLink (users -> control plane)
resource "aws_vpc_endpoint" "databricks_frontend" {
  vpc_id              = aws_vpc.databricks.id
  service_name        = "com.amazonaws.vpce.us-east-1.vpce-svc-<frontend-id>"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private_1.id]
  security_group_ids  = [aws_security_group.databricks_endpoint.id]
  private_dns_enabled = false
}

# Register PrivateLink endpoints with Databricks
resource "databricks_mws_private_access_settings" "this" {
  account_id                   = var.databricks_account_id
  private_access_settings_name = "emr-migration-pas"
  region                       = "us-east-1"
  public_access_enabled        = false  # Disable public access entirely
}

resource "databricks_mws_vpc_endpoint" "backend" {
  account_id          = var.databricks_account_id
  vpc_endpoint_name   = "backend-vpce"
  aws_vpc_endpoint_id = aws_vpc_endpoint.databricks_backend.id
  region              = "us-east-1"
}
```

### Pattern 5: Security Group Mapping

| EMR Security Group Rule | Databricks Equivalent |
|---|---|
| Master SG: SSH (22) from bastion | Not needed (use Databricks web terminal) |
| Master SG: Spark UI (18080) | Access via Databricks UI (proxied) |
| Master SG: YARN RM (8088) | Not applicable (no YARN) |
| Slave SG: All traffic from Master SG | Databricks SG: self-referencing all traffic |
| Service access SG: Hive (10000) | Not needed (use SQL Warehouses) |
| Outbound: All to internet | Outbound: All (NAT or PrivateLink) |

Databricks uses a single security group with a self-referencing rule (all ports, all protocols) for inter-node communication.

### Pattern 6: DNS Resolution

**EMR**: DNS resolution for Hive Metastore, RDS, custom services.

**Databricks**: Same DNS works if using customer-managed VPC in the same region. For cross-VPC DNS:

```hcl
# Route 53 private hosted zone association
resource "aws_route53_zone_association" "databricks" {
  zone_id = var.private_hosted_zone_id
  vpc_id  = aws_vpc.databricks.id
}
```

## CIDR Planning

When migrating from EMR to Databricks, ensure no CIDR overlap:

| Component | Example CIDR | Notes |
|---|---|---|
| Existing services VPC | 10.1.0.0/16 | RDS, Redshift, app servers |
| EMR VPC (being retired) | 10.2.0.0/16 | Can be reused after migration |
| Databricks VPC | 10.3.0.0/16 | New VPC for Databricks |
| Databricks private subnet 1 | 10.3.1.0/24 | AZ-a, minimum /24 |
| Databricks private subnet 2 | 10.3.2.0/24 | AZ-b, minimum /24 |
| Databricks public subnet | 10.3.0.0/24 | For NAT gateway |

**Sizing**: Each Databricks cluster node consumes one IP. A /24 subnet supports ~250 concurrent nodes. Use /20 or larger for heavy workloads.

## Migration Checklist

- [ ] Choose Databricks-managed or customer-managed VPC
- [ ] If customer-managed: create VPC with two private subnets in different AZs
- [ ] Create NAT gateway (or configure PrivateLink for no-internet)
- [ ] Create S3 gateway VPC endpoint
- [ ] Create STS interface VPC endpoint
- [ ] Set up security group with self-referencing rule
- [ ] Configure VPC peering to existing services (if needed)
- [ ] Configure DNS resolution for cross-VPC services
- [ ] Register network configuration with Databricks
- [ ] Create workspace with the network configuration
- [ ] Validate connectivity from a cluster to S3, RDS, and other services

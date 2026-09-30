# Databricks Specialist Solutions Architect Content Repository

This repository serves as the official storage and distribution platform for Databricks Specialist Solutions Architect (SSA) content. It contains a comprehensive collection of technical resources, engagement materials, and structured content designed to support SSA activities and customer engagements.

## Repository Purpose

This repository provides a centralized location for SSAs to access, share, and collaborate on official content including:

- Technical resources and documentation
- Engagement materials and templates
- Structured offerings and methodologies
- Best practices and reference implementations
- Training materials and knowledge sharing content

Some content in this repository includes structured "Offerings" - proactive, prescriptive engagement catalogs that focus on outcomes and optimize the ASQ process for driving momentum and results. These offerings are designed to:

- Accelerate the adoption of new technologies (e.g., Unity Catalog, GenAI)
- Expedite technical evaluation and competition (e.g., Databricks SQL, GenAI)
- Streamline positioning and hand-off to Professional Services and Partners

Each Offering is described by its `README.md`, whose YAML frontmatter is the machine-readable source that internal tooling reads to promote discoverability. 

## Repository Structure

This repository is organized into five main domains, each containing content focused on specific technology areas:

### 📊 Data Warehousing (`/data-warehousing`)
**Purpose:** Accelerate adoption of modern data warehousing technologies, expedite technical evaluation, and streamline positioning for data warehousing solutions.

**Common Use Cases:**
- Unity Catalog adoption and governance
- Databricks SQL optimization
- Data lakehouse architecture
- Performance tuning and optimization
- Migration strategies from traditional data warehouses

### 🔧 Data Engineering (`/data-engineering`)
**Purpose:** Accelerate adoption of modern data engineering patterns, expedite technical evaluation of data platforms, and streamline positioning for data engineering solutions.

**Common Use Cases:**
- ETL/ELT pipeline optimization
- Data quality and governance
- Real-time data processing
- Data pipeline monitoring and observability
- Modern data stack architecture
- Apache Spark optimization

### 🛡️ Data Governance (`/data-governance`)
**Purpose:** Accelerate adoption of data governance best practices, expedite technical evaluation of governance solutions, and streamline positioning for Unity Catalog and data governance implementations.

**Common Use Cases:**
- Unity Catalog design and deployment
- Data governance strategy and policy frameworks
- Multi-region, multi-cloud, and multi-BU governance
- Regulatory compliance (PII, PCI, PHI, data residency)
- Integration with third-party governance tools (Collibra, Immuta, Purview)
- Metastore and catalog hierarchy design

### 🤖 GenAI (`/gen-ai`)
**Purpose:** Accelerate adoption of generative AI technologies, expedite technical evaluation of AI platforms, and streamline positioning for AI/ML solutions.

**Common Use Cases:**
- Large Language Model (LLM) integration
- Vector databases and embeddings
- RAG (Retrieval-Augmented Generation) patterns
- AI/ML model deployment and serving
- MLOps and model lifecycle management
- AI governance and responsible AI practices

### 🔒 Cybersecurity (`/cybersecurity`)
**Purpose:** Accelerate adoption of security best practices, expedite technical evaluation of security solutions, and streamline positioning for cybersecurity implementations.

**Common Use Cases:**
- Data security and encryption
- Access control and identity management
- Compliance and governance frameworks
- Security monitoring and threat detection
- Data privacy and protection
- Security architecture and design patterns

### 📦 Archive (`/z-archive`)
**Purpose:** Preserve retired Specialist offerings for reference. The `z-` prefix sorts this folder **last** in the repository root so active domain folders appear first. Folders here mirror domain structure (`z-archive/data-warehousing`, etc.) but are **not** part of active Field Catalog ingestion—downstream tooling should exclude `z-archive/` when syncing offering `README.md` frontmatter.

See [z-archive/README.md](z-archive/README.md) for policy and layout.

## Content Standards

Content in this repository follows consistent standards to ensure quality and effectiveness:

- **Clear objectives and deliverables**
- **Well-documented and structured**
- **Minimal overhead requirements**
- **Prescriptive approach with structured methodology**

## Getting Started

1. Browse the domain folders to find relevant content (`data-warehousing`, `data-engineering`, `data-governance`, `gen-ai`, `cybersecurity`; retired offerings live under `/z-archive`; see [z-archive/README.md](z-archive/README.md))
2. Use the templates in `/templates` for creating new structured content
3. Ensure content aligns with SSA engagement principles
4. Focus on measurable outcomes and clear value propositions

## Templates

The `/templates` directory contains standardized templates and guidelines for creating consistent SSA content across all domains. Structured offerings should follow the template structure to maintain consistency and quality.

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|

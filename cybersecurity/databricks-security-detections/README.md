# Security Detection-as-Code with Databricks

This project demonstrates how to author and deploy detection rules using SQL and Databricks notebooks for Security Detection-as-Code principles. The detection rules use Databricks system tables for real-time security monitoring and automatically create alerts for security incidents. **Now enhanced with AI-powered security analysis for intelligent risk assessment and recommendations.**

## 🏗️ Project Structure

```
databricks-security-detections/
├── src/                      # Source files
│   ├── ai_analysis/         # AI-powered security analysis
│   │   ├── ai_security_analysis.sql      # Main AI analysis functions
│   │   ├── example_ai_queries.sql        # Example queries for AI analysis
│   ├── detections/          # Security detection rules
│   │   ├── failed_login_detection.sql
│   │   ├── large_data_export_detection.sql
│   │   └── privilege_escalation_detection.sql
│   ├── utils/               # SQL functions and utilities
│   │   ├── alert_function.sql      # Function to create http connection and alert function
│   │   ├── delete_alert.sql       # Alert deletion utilities
│   │   └── generate_alerts.py     # Alert generation script
│   ├── alerts.sql           # Generated alerts from rules.yml
│   └── security_detection_report.lvdash.json  # Dashboard configuration
├── resources/               # Databricks workflow configuration
│   ├── databricks_workflow.yml
│   └── security_detection_report_dashboard.yml
├── rules.yml                # Alert rules configuration
├── databricks.yml          # Bundle configuration
├── requirements.txt         # Python dependencies
└── README.md               # Project documentation
```

## 🚀 Key Features

- 📊 **SQL-based Detection Rules**: Leverage enterprise-grade SQL for sophisticated security detection logic
- 🤖 **AI-Powered Security Analysis**: Intelligent risk assessment and recommendations using Databricks AI models
- 🔍 **Databricks System Tables**: Harness comprehensive operational data from your Databricks environment for deep visibility
- 🚨 **Automated Alert Creation**: Streamline security operations with intelligent alert generation and management
- 📦 **Databricks Asset Bundle (DAB)**: Enable seamless deployment across development, testing and production environments
- 🔄 **Workflow Orchestration**: Optimize detection rule execution with advanced dependency management and scheduling
- 📈 **Databricks AI/BI Dashboard**: Generate visualizations and interactive reports using Databricks SQL dashboards

## 📋 Prerequisites

Before you begin, ensure you have:

1. **Databricks Workspace**: Access to a Databricks workspace with appropriate permissions
2. **DBSQL Warehouse Access**: Ability to create and manage Databricks SQL Warehouse
3. **Git Repository**: Version control setup for your detection rules
4. **Python Environment**: Python 3.11+ with required packages
5. **Unity Catalog**: Workspace must be enabled for Unity Catalog to access system tables

## 🚀 Quick Starts

### 1. Python Virtual Environment

```bash

python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Install the Databricks CLI
Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/install.html

### 3. Authenticate to your Databricks workspace
Choose one of the following authentication methods:

#### Option A: Personal Access Token (PAT)

1. **Generate Personal Access Token:**
   - Log into your Databricks workspace
   - Click on your username in the top-right corner
   - SELECT **User Settings** → **Developer** → **Access tokens**
   - Click **Generate new token**
   - Give it a name (e.g., "Local Development") and set expiration
   - Copy the generated token

2. **Configure CLI with PAT:**
   ```bash
   databricks configure --token --profile DEFAULT
   ```
   
   You'll be prompted for:
   - **Databricks Host**: `https://your-workspace.cloud.databricks.com`
   - **Token**: Paste your generated token

    This will update DEFAULT profile in `~/.databrickscfg` 

#### Option B: OAuth Authentication

Configure OAuth:

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com --profile DEFAULT
```

This will:
- Open your browser for authentication
- Create a profile in `~/.databrickscfg`
- Store OAuth credentials securely

#### Verify Databricks Configuration

Check your configuration:

```bash
# List all profiles
databricks auth profiles
```

Your `~/.databrickscfg` should look like:

```ini
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi123abc...

[DEV]
host = https://dev-workspace.cloud.databricks.com
token = dapi456def...

[PROD]
host = https://prod-workspace.cloud.databricks.com
token = databricks-cli
```

### 4. Configure databricks.yml Variables
Update the variables in `databricks.yml` to match your environment. The dev target defaults to catalog `users` and a schema based on on the developer's name.

- **catalog**: The catalog name where your tables will be created
- **schema**: The schema name within the catalog
- **warehouse_id**: ID of your SQL warehouse for production deployment. For development, the bundle will lookup the ID based on the specified name (Eg, Shared Serverless).
- **workspace.host**: Your Databricks workspace URL

Example configuration for prod target:
```yaml
targets:
  prod:
    mode: production
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com
    variables:
      warehouse_id: your_warehouse_id
      catalog: your_catalog
      schema: your_schema
```

### 5. Set Up Security Rules and Alerts (Before Deployment)

Before deploying to your Databricks workspace, you should customize the security detection system:

#### A. Review and Customize Security Rules

1. **Examine existing rules** in `src/authentication/` and `src/data_access/` directories
2. **Modify thresholds** and logic based on your security requirements
3. **Add new detection rules** for your specific use cases

#### B. Configure Alert Rules

1. **Edit `rules.yml`** to customize alert configurations (see in [Alerts Setup](#-alerts-setup)):
   - Adjust threshold values for your environment
   - Set appropriate time intervals (short: 24h, medium: 168h, long: 720h)
   - Configure notification settings and scheduling

2. **Generate alerts** from your rules:
   ```bash
   python3 src/utils/generate_alert_queries.py
   ```

3. **Review generated SQL** in `src/create_alerts.sql` to ensure it meets your requirements

#### C. Test Your Configuration

1. **Validate rules syntax** by running the generation script
2. **Check alert parameters** for completeness
3. **Verify time intervals** match your monitoring needs

#### D. Prepare for Deployment

1. **Commit your changes** to version control
2. **Document any customizations** for your team
3. **Ensure all required parameters** are set in `databricks.yml`

### 6. Update Dashboard Configuration
Before deploying the bundle, in the base folder, run the Python script to update the dashboard query parameters for the target environment. The target name and CLI profile are required arguments:

```bash
$ python src/replace_dashboard_vars.py --target dev --profile DEFAULT

# OR for PROD

$ python src/replace_dashboard_vars.py --target PROD --profile PROD
```

This script:
- Resolves DAB variables for the specified target
- Updates the existing dashboard file (`dbsql_metrics.lvdash.json`)
- Replaces the catalog and schema parameter values
- Preserves all other dashboard configuration settings

### 7. Deploy to Databricks Workspace

#### Deploy in Development Environment

```bash
$ databricks bundle deploy --target dev --profile DEFAULT
```
Note: Since "dev" is specified as the default target in databricks.yml, you can omit the `--target dev` parameter. Similarly, `--profile DEFAULT` can be omitted if you only have one profile configured for your workspace.

This deploys everything that's defined for this project, including:
- A job called `[dev yourname] databricks_authoring_security_detection`
- All associated resources

You can find the deployed job by opening your workspace and clicking on **Workflows**.

#### Deploy to Production Environment
```bash
$ databricks bundle deploy --target prod --profile PROD
```

This deploys everything that's defined for this project, including:
- A job called `databricks_authoring_security_detection`

### 8. Run a Job

**Run in Dev**

```bash
$ databricks bundle run --target dev --profile DEFAULT
```
**Run in Prod**

```bash
$ databricks bundle run --target prod --profile PROD
```

### 9. Optional - Destroy all assets 

```bash
# Force destroy without confirmation prompts
databricks bundle destroy --force

# Destroy a specific bundle if you have multiple
databricks bundle destroy --target <bundle-name>
```

## 🚨 Alerts Setup

### Alert Creation Workflow

The project includes an automated alert creation system that runs after all detection rules complete:

1. **Detection Execution**: All security detection rules run first
2. **Alert Generation**: The `create_alert.sql` script automatically creates alerts based on detection results
3. **Alert Management**: Alerts are configured with thresholds, notifications, and scheduling

### Alert Configuration

Alerts are automatically created with the following settings:

- **Failed Login Alert**: Triggers when failed login count > 0 in the last 24 hours (short interval)
- **Account Admin Assignment**: Triggers when admin assignments > 5 in the last 24 hours (short interval)
- **Data Export Alert**: Triggers when data exports > 1000 in the last 168 hours (medium interval)
- **UC Permission Escalation**: Triggers when permission escalations > 5 in the last 720 hours (long interval)

### Alert Parameters

Each alert can be configured with:

- **Threshold Values**: Configurable thresholds for triggering alerts
- **Comparison Operators**: Greater than, less than, equals, etc.
- **Notification Settings**: Email notifications to specified users
- **Schedule**: Cron-based scheduling for alert evaluation
- **Retrigger Settings**: Configurable retrigger intervals
- **Time Intervals**: Configurable monitoring windows (short: 24h, medium: 168h, long: 720h)

### Adding New Security Rules and Alerts

Before deploying to your Databricks workspace, you can customize and extend the security detection system:

#### 1. Adding a New Alert

Add this to your `rules.yml`:

```yaml
alerts:
  # ... existing alerts ...
  
  data_export_ai_alert:
    display_name: "data_export_ai_alert"
    description: "Alert for large data exports"
    time_interval: "medium"  # Options: short(24h), medium(168h), long(720h), default(168h)
    query_template: |
        SELECT sum(export_count) AS exports
         FROM %s.%s.sec_mv_ai_data_export_analysis
         WHERE get_json_object(ai_comprehensive_analysis, '$.risk_level') IN ('HIGH', 'CRITICAL')
    comparison_operator: "GREATER_THAN"
    threshold_value: 10
    cron_schedule: "0 0 10 1/7 * ?"
    empty_result_state: "UNKNOWN"
```

#### 2. Modifying Existing Alerts

Change threshold values, comparison operators, or scheduling:

```yaml
alerts:
  data_export_alert:
    # ... other settings ...
    threshold_value: 10  # Changed from 1000
    time_interval: "short"  # Changed from medium to short (24h instead of 168h)
    cron_schedule: "0 */2 * * * ?"  # Every 2 hours instead of weekly
```

#### 3. Global Settings

Modify settings that apply to all alerts:

```yaml
global:
  # ... other settings ...
  timezone_id: "America/New_York"  # Change timezone
  notify_on_ok: false              # Don't notify on OK status
  retrigger_seconds: 300           # Retrigger after 5 minutes
```

#### 4. Time Interval Configuration

Customize the time intervals used across all alerts:

```yaml
time_intervals:
  short: 24     # 1 day - for frequent, real-time alerts
  medium: 168   # 7 days - for weekly monitoring
  long: 720     # 30 days - for monthly compliance checks
  default: 168  # 7 days - fallback interval
```

### Generating Alert Queries

After any changes to `rules.yml` to regenerate the `alerts.sql` file, run below command:

```bash
python3 src/utils/generate_alert_queries.py
```

### Validation and Best Practices

The generated SQL includes:
- All required parameters
- Consistent formatting
- Clear comments for each alert

**Best Practices:**
- Keep alert descriptions clear and actionable
- Use appropriate threshold values for your environment
- Test alerts in development before production
- Version control your `rules.yml` alongside code
- Document any custom query logic in descriptions
- Choose appropriate time intervals based on monitoring needs:
  - **Short (24h)**: For real-time security events
  - **Medium (168h)**: For weekly monitoring
  - **Long (720h)**: For monthly compliance checks


## Data Requirement
### System Tables Access

The detection rules use Databricks system tables for real-time monitoring. Users must have READ permission on below system tables to run the detection rules.

#### **Audit Logs** (`system.access.audit`)
- **Purpose**: Authentication and privilege events
- **Retention**: 365 days (free)
- **Use Cases**: Login attempts, privilege changes, administrative actions

## 🤖 AI-Powered Security Analysis

The project includes comprehensive AI-powered security analysis capabilities that provide intelligent risk assessment, actionable recommendations, and automated security insights.

### AI Analysis Components

#### 1. **Failed Login Risk Assessment**
- **View**: `sec_mv_ai_failed_login_analysis`
- **Capabilities**: 
  - IP-based and user-based pattern analysis
  - Risk level classification (HIGH/MEDIUM/LOW)
  - Immediate action recommendations
  - Long-term security controls

#### 2. **Data Export Risk Assessment**
- **View**: `sec_mv_ai_data_export_analysis`
- **Capabilities**:
  - Export pattern analysis with actor frequency
  - Risk assessment based on source sensitivity
  - Time pattern analysis
  - User role consideration

#### 3. **Privilege Escalation Analysis**
- **Views**: Multiple specialized analysis views for different escalation types
  - `sec_v_ai_account_admin_analysis` - Account admin assignments
  - `sec_v_ai_workspace_acl_analysis` - Workspace ACL changes
  - `sec_v_ai_sensitive_group_analysis` - Sensitive group additions
  - `sec_v_ai_uc_permission_analysis` - Unity Catalog permissions
  - `sec_v_ai_account_setting_analysis` - Account setting changes
- **Capabilities**:
  - Actor frequency analysis
  - Risk level assessment (CRITICAL/HIGH/MEDIUM/LOW)
  - Investigation step recommendations
  - Long-term control suggestions

#### 4. **Consolidated Privilege Escalation View**
- **View**: `sec_mv_ai_privilege_escalation_consolidated`
- **Capabilities**:
  - Unified view across all escalation types
  - Cross-correlation analysis
  - Comprehensive actor activity tracking

#### 5. **Security Posture Summary**
- **View**: `sec_mv_ai_security_posture_summary`
- **Capabilities**:
  - Overall risk level assessment
  - Key risk area identification
  - Strategic recommendations
  - Compliance considerations

### AI Model Configuration

- **Model**: `databricks-meta-llama-3-3-70b-instruct`
- **Response Format**: Structured JSON with predefined schemas
- **Analysis Types**: Risk assessment, recommendations, investigation steps
- **Update Frequency**: analysis on detection events

### Using AI Analysis

#### Example Queries

The `src/ai_analysis/example_ai_queries.sql` file provides comprehensive examples for:

- **Risk Level Filtering**: Filter events by AI-assessed risk levels
- **Array Processing**: Work with AI-generated recommendation arrays
- **Cross-Correlation**: Analyze relationships between different security events
- **Performance Monitoring**: Track AI analysis performance and response times
- **Compliance Reporting**: Generate compliance-focused reports

#### Key AI Analysis Fields

Each AI analysis view returns structured JSON with:

```json
{
  "risk_level": "HIGH|MEDIUM|LOW|CRITICAL",
  "risk_factors": ["factor1", "factor2"],
  "immediate_actions": ["action1", "action2"],
  "investigation_steps": ["step1", "step2"],
  "long_term_controls": ["control1", "control2"]
}
```

## 📚 Additional Resources

- [Databricks System Tables Documentation](https://docs.databricks.com/aws/en/admin/system-tables/)
- [Unity Catalog Setup Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Audit log system table reference](https://docs.databricks.com/aws/en/admin/system-tables/audit-logs.html)
- [Databricks Alerts Documentation](https://docs.databricks.com/aws/en/sql/user/alerts/)
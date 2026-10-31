# Databricks notebook source
# =====================================================================
# ABAC PoC: Create Governed Tags via REST API
# =====================================================================
# Governed tags enforce allowed values and require ASSIGN permission.
# There is no SQL DDL for creating governed tags — they must be created
# via the Databricks REST API (POST /api/2.1/tag-policies).
# This cell defines all 5 governed tag keys and their allowed values
# used by the ABAC policy in subsequent cells.
#
# Prerequisites:
# - Account admin or metastore admin privileges (CREATE permission)
# - Databricks Runtime 16.1+ / Serverless SQL warehouse
# =====================================================================

import requests

# Get workspace URL and token from the current Databricks context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# -- Define all governed tags and their allowed values --
governed_tags = {
    "abac_sublob": {
        "description": "Sub-Line of Business classification for business divisions",
        "values": ["AF", "BB", "CB", "CCB"],
    },
    "abac_data_use_category": {
        "description": "Indicates if data has usage restrictions",
        "values": ["true", "false"],
    },
    "abac_protection_group": {
        "description": "Data protection classification for column-level ABAC policies",
        "values": ["INTL", "CNFD", "HIGHPI", "HICNFD", "PIDRID", "GOVTID", "CNFDPI", "RISKSC"],
    },
    "abac_sensitive": {
        "description": "Indicates if column contains sensitive data",
        "values": ["true", "false"],
    },
    "abac_pci": {
        "description": "Indicates if column contains PCI-regulated data",
        "values": ["true", "false"],
    },
}

# -- Create each governed tag via the Tag Policies API --
api_url = f"{host}/api/2.1/tag-policies"

for tag_key, config in governed_tags.items():
    payload = {
        "tag_key": tag_key,
        "description": config["description"],
        "values": [{"name": v} for v in config["values"]],
    }

    resp = requests.post(api_url, headers=headers, json=payload)

    if resp.status_code == 200:
        print(f"Created governed tag: {tag_key}")
    elif resp.status_code == 409 or "ALREADY_EXISTS" in resp.text:
        print(f"Governed tag already exists (OK): {tag_key}")
    else:
        print(f"Error creating {tag_key}: {resp.status_code} - {resp.text}")

print("\nGoverned tags ready. Proceed with tag assignment in subsequent cells.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- ABAC PoC: Create Tables with Tags in Main Catalog
# MAGIC -- =====================================================================
# MAGIC -- This script creates 3 tables in each dataspace schema with tags applied
# MAGIC -- 
# MAGIC -- Prerequisites:
# MAGIC -- - main catalog exists with af_dataspace and bb_dataspace schemas
# MAGIC -- - Governed tags created in the cell above (abac_sublob, abac_data_use_category, etc.)
# MAGIC -- - User has CREATE TABLE, APPLY TAG, and ASSIGN permissions on governed tags
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- Set catalog context
# MAGIC USE CATALOG aa_catalog;
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SECTION 1: Create AF Dataspace Tables
# MAGIC -- =====================================================================
# MAGIC
# MAGIC USE SCHEMA af_schema;
# MAGIC
# MAGIC -- Table 1: Accounts
# MAGIC CREATE TABLE IF NOT EXISTS accounts (
# MAGIC     account_id STRING COMMENT 'Unique account identifier',
# MAGIC     customer_id STRING COMMENT 'Reference to customer',
# MAGIC     account_type STRING COMMENT 'Type of account (checking, savings, etc.)',
# MAGIC     balance DOUBLE COMMENT 'Current account balance',
# MAGIC     status STRING COMMENT 'Account status (active, inactive, closed)',
# MAGIC     opened_date DATE COMMENT 'Date account was opened'
# MAGIC ) COMMENT 'Customer account information table';
# MAGIC
# MAGIC -- Table 2: Transactions
# MAGIC CREATE TABLE IF NOT EXISTS transactions (
# MAGIC     transaction_id STRING COMMENT 'Unique transaction identifier',
# MAGIC     account_id STRING COMMENT 'Reference to account',
# MAGIC     amount DOUBLE COMMENT 'Transaction amount',
# MAGIC     transaction_type STRING COMMENT 'Type of transaction (debit, credit)',
# MAGIC     description STRING COMMENT 'Transaction description',
# MAGIC     transaction_date TIMESTAMP COMMENT 'Transaction timestamp'
# MAGIC ) COMMENT 'Account transaction records table';
# MAGIC
# MAGIC -- Table 3: Customers
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC     customer_id STRING COMMENT 'Unique customer identifier',
# MAGIC     first_name STRING COMMENT 'Customer first name',
# MAGIC     last_name STRING COMMENT 'Customer last name',
# MAGIC     email STRING COMMENT 'Customer email address',
# MAGIC     phone STRING COMMENT 'Customer phone number',
# MAGIC     address STRING COMMENT 'Customer address',
# MAGIC     created_date DATE COMMENT 'Customer record creation date'
# MAGIC ) COMMENT 'Customer profile information table';
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SECTION 2: Create BB Dataspace Tables
# MAGIC -- =====================================================================
# MAGIC
# MAGIC USE SCHEMA bb_schema;
# MAGIC
# MAGIC -- Table 1: Commercial Accounts
# MAGIC CREATE TABLE IF NOT EXISTS commercial_accounts (
# MAGIC     account_id STRING COMMENT 'Unique commercial account identifier',
# MAGIC     business_name STRING COMMENT 'Business name',
# MAGIC     tax_id STRING COMMENT 'Business tax identification number',
# MAGIC     industry_code STRING COMMENT 'Industry classification code',
# MAGIC     account_balance DOUBLE COMMENT 'Current account balance',
# MAGIC     credit_limit DOUBLE COMMENT 'Credit limit for account',
# MAGIC     account_manager STRING COMMENT 'Assigned account manager',
# MAGIC     opened_date DATE COMMENT 'Account opening date'
# MAGIC ) COMMENT 'Commercial business accounts table';
# MAGIC
# MAGIC -- Table 2: Corporate Transactions
# MAGIC CREATE TABLE IF NOT EXISTS corporate_transactions (
# MAGIC     transaction_id STRING COMMENT 'Unique transaction identifier',
# MAGIC     account_id STRING COMMENT 'Reference to commercial account',
# MAGIC     amount DOUBLE COMMENT 'Transaction amount',
# MAGIC     currency STRING COMMENT 'Transaction currency',
# MAGIC     transaction_type STRING COMMENT 'Type of transaction',
# MAGIC     reference_number STRING COMMENT 'Transaction reference number',
# MAGIC     processing_date TIMESTAMP COMMENT 'Transaction processing timestamp'
# MAGIC ) COMMENT 'Corporate transaction records table';
# MAGIC
# MAGIC -- Table 3: Corporate Clients
# MAGIC CREATE TABLE IF NOT EXISTS corporate_clients (
# MAGIC     client_id STRING COMMENT 'Unique corporate client identifier',
# MAGIC     company_name STRING COMMENT 'Corporation name',
# MAGIC     headquarters_country STRING COMMENT 'Headquarters country',
# MAGIC     business_sector STRING COMMENT 'Business sector classification',
# MAGIC     annual_revenue DOUBLE COMMENT 'Annual revenue',
# MAGIC     employee_count INT COMMENT 'Number of employees',
# MAGIC     relationship_start_date DATE COMMENT 'Relationship start date',
# MAGIC     client_tier STRING COMMENT 'Client tier classification'
# MAGIC ) COMMENT 'Corporate client profiles table';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- SECTION 3: Apply Table-Level Tags - AF Dataspace
# MAGIC -- =====================================================================
# MAGIC USE SCHEMA af_schema;
# MAGIC
# MAGIC -- Apply table-level tags to AF tables
# MAGIC ALTER TABLE accounts SET TAGS (
# MAGIC     'abac_sublob' = 'AF',
# MAGIC     'abac_data_use_category' = 'false'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE transactions SET TAGS (
# MAGIC     'abac_sublob' = 'CB',
# MAGIC     'abac_data_use_category' = 'true'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE customers SET TAGS (
# MAGIC     'abac_sublob' = 'CCB',
# MAGIC     'abac_data_use_category' = 'false'
# MAGIC );
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SECTION 4: Apply Table-Level Tags - BB Dataspace
# MAGIC -- =====================================================================
# MAGIC
# MAGIC USE SCHEMA bb_schema;
# MAGIC
# MAGIC -- Apply table-level tags to BB tables
# MAGIC ALTER TABLE commercial_accounts SET TAGS (
# MAGIC     'abac_sublob' = 'BB',
# MAGIC     'abac_data_use_category' = 'true'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE corporate_transactions SET TAGS (
# MAGIC     'abac_sublob' = 'CB',
# MAGIC     'abac_data_use_category' = 'false'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE corporate_clients SET TAGS (
# MAGIC     'abac_sublob' = 'CCB',
# MAGIC     'abac_data_use_category' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA af_schema;
# MAGIC
# MAGIC -- Accounts table column tags
# MAGIC ALTER TABLE accounts ALTER COLUMN account_id SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE accounts ALTER COLUMN customer_id SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE accounts ALTER COLUMN account_type SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE accounts ALTER COLUMN balance SET TAGS ('abac_protection_group' = 'HIGHPI', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE accounts ALTER COLUMN status SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE accounts ALTER COLUMN opened_date SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC
# MAGIC -- Transactions table column tags
# MAGIC ALTER TABLE transactions ALTER COLUMN transaction_id SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE transactions ALTER COLUMN account_id SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE transactions ALTER COLUMN amount SET TAGS ('abac_protection_group' = 'HIGHPI', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE transactions ALTER COLUMN transaction_type SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE transactions ALTER COLUMN description SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE transactions ALTER COLUMN transaction_date SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC
# MAGIC -- Customers table column tags
# MAGIC ALTER TABLE customers ALTER COLUMN customer_id SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE customers ALTER COLUMN first_name SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE customers ALTER COLUMN last_name SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE customers ALTER COLUMN email SET TAGS ('abac_protection_group' = 'HICNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'true');
# MAGIC ALTER TABLE customers ALTER COLUMN phone SET TAGS ('abac_protection_group' = 'PIDRID', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE customers ALTER COLUMN address SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE customers ALTER COLUMN created_date SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SECTION 6: Apply Column-Level Tags - BB Dataspace
# MAGIC -- =====================================================================
# MAGIC
# MAGIC USE SCHEMA bb_schema;
# MAGIC
# MAGIC -- Commercial Accounts table column tags
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN account_id SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN business_name SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN tax_id SET TAGS ('abac_protection_group' = 'GOVTID', 'abac_sensitive' = 'true', 'abac_pci' = 'true');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN industry_code SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN account_balance SET TAGS ('abac_protection_group' = 'HIGHPI', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN credit_limit SET TAGS ('abac_protection_group' = 'HIGHPI', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN account_manager SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE commercial_accounts ALTER COLUMN opened_date SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC
# MAGIC -- Corporate Transactions table column tags
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN transaction_id SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN account_id SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN amount SET TAGS ('abac_protection_group' = 'HIGHPI', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN currency SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN transaction_type SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN reference_number SET TAGS ('abac_protection_group' = 'HICNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_transactions ALTER COLUMN processing_date SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC
# MAGIC -- Corporate Clients table column tags
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN client_id SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN company_name SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN headquarters_country SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN business_sector SET TAGS ('abac_protection_group' = 'CNFD', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN annual_revenue SET TAGS ('abac_protection_group' = 'CNFDPI', 'abac_sensitive' = 'true', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN employee_count SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN relationship_start_date SET TAGS ('abac_protection_group' = 'INTL', 'abac_sensitive' = 'false', 'abac_pci' = 'false');
# MAGIC ALTER TABLE corporate_clients ALTER COLUMN client_tier SET TAGS ('abac_protection_group' = 'RISKSC', 'abac_sensitive' = 'false', 'abac_pci' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aa_catalog.information_schema.column_tags
# MAGIC where schema_name = 'af_schema';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aa_catalog.information_schema.table_tags
# MAGIC where schema_name = 'af_schema';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================
# MAGIC -- ABAC PoC: Insert Sample Data into Tables
# MAGIC -- =====================================================================
# MAGIC -- This script inserts 5-10 sample records into each table
# MAGIC -- Includes realistic data with proper relationships between tables
# MAGIC -- 
# MAGIC -- Prerequisites:
# MAGIC -- - Tables already created in main.af_dataspace and main.bb_dataspace
# MAGIC -- - User has INSERT permissions on all tables
# MAGIC -- =====================================================================
# MAGIC
# MAGIC -- Set catalog context
# MAGIC USE CATALOG aa_catalog;
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SECTION 1: Insert Data into AF Dataspace Tables
# MAGIC -- =====================================================================
# MAGIC
# MAGIC USE SCHEMA af_schema;
# MAGIC
# MAGIC -- Insert customers first (referenced by accounts table)
# MAGIC INSERT INTO customers VALUES
# MAGIC     ('CUST001', 'John', 'Smith', 'john.smith@email.com', '+1-555-0101', '123 Main St, New York, NY 10001', '2020-01-15'),
# MAGIC     ('CUST002', 'Sarah', 'Johnson', 'sarah.j@email.com', '+1-555-0102', '456 Oak Ave, Boston, MA 02101', '2020-03-22'),
# MAGIC     ('CUST003', 'Michael', 'Brown', 'mbrown@email.com', '+1-555-0103', '789 Pine St, Chicago, IL 60601', '2019-11-08'),
# MAGIC     ('CUST004', 'Emily', 'Davis', 'emily.davis@email.com', '+1-555-0104', '321 Elm St, Miami, FL 33101', '2021-06-12'),
# MAGIC     ('CUST005', 'David', 'Wilson', 'dwilson@email.com', '+1-555-0105', '654 Maple Dr, Seattle, WA 98101', '2020-09-30'),
# MAGIC     ('CUST006', 'Lisa', 'Garcia', 'lisa.garcia@email.com', '+1-555-0106', '987 Cedar Ln, Austin, TX 78701', '2021-02-18'),
# MAGIC     ('CUST007', 'James', 'Martinez', 'j.martinez@email.com', '+1-555-0107', '147 Birch St, Denver, CO 80201', '2019-08-25'),
# MAGIC     ('CUST008', 'Maria', 'Rodriguez', 'maria.r@email.com', '+1-555-0108', '258 Ash Ave, Phoenix, AZ 85001', '2020-12-03');
# MAGIC
# MAGIC -- Insert accounts (references customers)
# MAGIC INSERT INTO accounts VALUES
# MAGIC     ('ACC001', 'CUST001', 'checking', 15420.75, 'active', '2020-01-20'),
# MAGIC     ('ACC002', 'CUST001', 'savings', 45780.50, 'active', '2020-02-10'),
# MAGIC     ('ACC003', 'CUST002', 'checking', 8932.25, 'active', '2020-03-25'),
# MAGIC     ('ACC004', 'CUST003', 'savings', 67421.80, 'active', '2019-11-15'),
# MAGIC     ('ACC005', 'CUST004', 'checking', 3567.90, 'active', '2021-06-20'),
# MAGIC     ('ACC006', 'CUST005', 'savings', 89123.45, 'active', '2020-10-05'),
# MAGIC     ('ACC007', 'CUST006', 'checking', 12876.30, 'active', '2021-02-25'),
# MAGIC     ('ACC008', 'CUST007', 'investment', 125000.00, 'active', '2019-09-01'),
# MAGIC     ('ACC009', 'CUST008', 'checking', 5643.12, 'active', '2020-12-10'),
# MAGIC     ('ACC010', 'CUST002', 'savings', 23456.78, 'inactive', '2021-01-15');
# MAGIC
# MAGIC -- Insert transactions (references accounts)
# MAGIC INSERT INTO transactions VALUES
# MAGIC     ('TXN001', 'ACC001', -250.00, 'debit', 'ATM Withdrawal', '2024-01-15 10:30:00'),
# MAGIC     ('TXN002', 'ACC001', 2500.00, 'credit', 'Salary Deposit', '2024-01-01 09:00:00'),
# MAGIC     ('TXN003', 'ACC002', 1000.00, 'credit', 'Transfer from Checking', '2024-01-10 14:25:00'),
# MAGIC     ('TXN004', 'ACC003', -89.50, 'debit', 'Grocery Store', '2024-01-12 16:45:00'),
# MAGIC     ('TXN005', 'ACC004', 500.00, 'credit', 'Interest Payment', '2024-01-05 12:00:00'),
# MAGIC     ('TXN006', 'ACC005', -1200.00, 'debit', 'Rent Payment', '2024-01-01 08:00:00'),
# MAGIC     ('TXN007', 'ACC006', -350.75, 'debit', 'Utility Bill', '2024-01-08 11:30:00'),
# MAGIC     ('TXN008', 'ACC007', 3000.00, 'credit', 'Freelance Payment', '2024-01-14 15:20:00'),
# MAGIC     ('TXN009', 'ACC008', 5000.00, 'credit', 'Investment Return', '2024-01-03 10:15:00'),
# MAGIC     ('TXN010', 'ACC009', -45.99, 'debit', 'Online Purchase', '2024-01-11 13:40:00');
# MAGIC
# MAGIC -- =====================================================================
# MAGIC -- SECTION 2: Insert Data into BB Dataspace Tables
# MAGIC -- =====================================================================
# MAGIC
# MAGIC USE SCHEMA bb_schema;
# MAGIC
# MAGIC -- Insert corporate clients first (referenced by other tables)
# MAGIC INSERT INTO corporate_clients VALUES
# MAGIC     ('CLIENT001', 'TechCorp Solutions Inc', 'United States', 'Technology', 25000000.00, 150, '2018-03-15', 'Platinum'),
# MAGIC     ('CLIENT002', 'Global Manufacturing Ltd', 'Canada', 'Manufacturing', 45000000.00, 320, '2017-09-20', 'Platinum'),
# MAGIC     ('CLIENT003', 'Energy Dynamics Corp', 'United States', 'Energy', 78000000.00, 200, '2019-01-10', 'Diamond'),
# MAGIC     ('CLIENT004', 'Healthcare Innovations', 'United Kingdom', 'Healthcare', 12000000.00, 85, '2020-06-25', 'Gold'),
# MAGIC     ('CLIENT005', 'Financial Services Group', 'Singapore', 'Financial Services', 95000000.00, 450, '2016-11-30', 'Diamond'),
# MAGIC     ('CLIENT006', 'Retail Chain Enterprises', 'Australia', 'Retail', 35000000.00, 1200, '2019-08-12', 'Platinum'),
# MAGIC     ('CLIENT007', 'Construction Alliance', 'Germany', 'Construction', 18000000.00, 95, '2021-02-28', 'Gold'),
# MAGIC     ('CLIENT008', 'Logistics International', 'Netherlands', 'Transportation', 28000000.00, 180, '2018-12-05', 'Silver');
# MAGIC
# MAGIC -- Insert commercial accounts (references corporate clients via business relationships)
# MAGIC INSERT INTO commercial_accounts VALUES
# MAGIC     ('COMM001', 'TechCorp Solutions Inc', '12-3456789', 'TECH54', 2500000.00, 5000000.00, 'Jennifer Adams', '2018-03-20'),
# MAGIC     ('COMM002', 'Global Manufacturing Ltd', '98-7654321', 'MANF31', 8750000.00, 15000000.00, 'Robert Chen', '2017-09-25'),
# MAGIC     ('COMM003', 'Energy Dynamics Corp', '45-6789012', 'ENRG22', 15600000.00, 25000000.00, 'Sarah Williams', '2019-01-15'),
# MAGIC     ('COMM004', 'Healthcare Innovations', '78-9012345', 'HLTH67', 3200000.00, 6000000.00, 'Michael Johnson', '2020-07-01'),
# MAGIC     ('COMM005', 'Financial Services Group', '23-4567890', 'FINA89', 22800000.00, 35000000.00, 'Lisa Zhang', '2016-12-05'),
# MAGIC     ('COMM006', 'Retail Chain Enterprises', '56-7890123', 'RETL45', 7500000.00, 12000000.00, 'David Martinez', '2019-08-18'),
# MAGIC     ('COMM007', 'Construction Alliance', '89-0123456', 'CONS12', 4100000.00, 8000000.00, 'Emma Thompson', '2021-03-05'),
# MAGIC     ('COMM008', 'Logistics International', '34-5678901', 'LOGS78', 6200000.00, 10000000.00, 'James Wilson', '2018-12-10');
# MAGIC
# MAGIC -- Insert corporate transactions (references commercial accounts)
# MAGIC INSERT INTO corporate_transactions VALUES
# MAGIC     ('CORP001', 'COMM001', 450000.00, 'USD', 'wire_transfer', 'REF2024001', '2024-01-15 09:30:00'),
# MAGIC     ('CORP002', 'COMM002', -125000.00, 'CAD', 'payment', 'REF2024002', '2024-01-12 14:15:00'),
# MAGIC     ('CORP003', 'COMM003', 2800000.00, 'USD', 'deposit', 'REF2024003', '2024-01-10 11:00:00'),
# MAGIC     ('CORP004', 'COMM004', -89000.00, 'GBP', 'payment', 'REF2024004', '2024-01-08 16:45:00'),
# MAGIC     ('CORP005', 'COMM005', 750000.00, 'SGD', 'wire_transfer', 'REF2024005', '2024-01-14 10:20:00'),
# MAGIC     ('CORP006', 'COMM006', -340000.00, 'AUD', 'payment', 'REF2024006', '2024-01-11 13:30:00'),
# MAGIC     ('CORP007', 'COMM007', 180000.00, 'EUR', 'deposit', 'REF2024007', '2024-01-09 15:10:00'),
# MAGIC     ('CORP008', 'COMM008', -95000.00, 'EUR', 'payment', 'REF2024008', '2024-01-13 12:45:00'),
# MAGIC     ('CORP009', 'COMM001', 75000.00, 'USD', 'interest_credit', 'REF2024009', '2024-01-05 08:00:00'),
# MAGIC     ('CORP010', 'COMM005', -1250000.00, 'USD', 'investment', 'REF2024010', '2024-01-07 10:15:00');
# MAGIC
# MAGIC -- -- =====================================================================
# MAGIC -- -- SECTION 3: Verification Queries
# MAGIC -- -- =====================================================================
# MAGIC
# MAGIC -- -- Verify AF dataspace data
# MAGIC -- USE SCHEMA af_dataspace;
# MAGIC
# MAGIC -- SELECT 'AF Customers Count' as table_name, COUNT(*) as record_count FROM customers
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'AF Accounts Count', COUNT(*) FROM accounts
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'AF Transactions Count', COUNT(*) FROM transactions;
# MAGIC
# MAGIC -- -- Sample data from each AF table
# MAGIC -- SELECT 'Sample Customers' as info;
# MAGIC -- SELECT customer_id, first_name, last_name, email FROM customers LIMIT 3;
# MAGIC
# MAGIC -- SELECT 'Sample Accounts' as info;
# MAGIC -- SELECT account_id, customer_id, account_type, balance FROM accounts LIMIT 3;
# MAGIC
# MAGIC -- SELECT 'Sample Transactions' as info;
# MAGIC -- SELECT transaction_id, account_id, amount, description FROM transactions LIMIT 3;
# MAGIC
# MAGIC -- -- Verify BB dataspace data
# MAGIC -- USE SCHEMA bb_dataspace;
# MAGIC
# MAGIC -- SELECT 'BB Corporate Clients Count' as table_name, COUNT(*) as record_count FROM corporate_clients
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'BB Commercial Accounts Count', COUNT(*) FROM commercial_accounts
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'BB Corporate Transactions Count', COUNT(*) FROM corporate_transactions;
# MAGIC
# MAGIC -- -- Sample data from each BB table
# MAGIC -- SELECT 'Sample Corporate Clients' as info;
# MAGIC -- SELECT client_id, company_name, business_sector, client_tier FROM corporate_clients LIMIT 3;
# MAGIC
# MAGIC -- SELECT 'Sample Commercial Accounts' as info;
# MAGIC -- SELECT account_id, business_name, account_balance, credit_limit FROM commercial_accounts LIMIT 3;
# MAGIC
# MAGIC -- SELECT 'Sample Corporate Transactions' as info;
# MAGIC -- SELECT transaction_id, account_id, amount, currency, transaction_type FROM corporate_transactions LIMIT 3;
# MAGIC
# MAGIC -- -- =====================================================================
# MAGIC -- -- SECTION 4: Relationship Verification
# MAGIC -- -- =====================================================================
# MAGIC
# MAGIC -- -- Verify relationships in AF dataspace
# MAGIC -- USE SCHEMA af_dataspace;
# MAGIC
# MAGIC -- SELECT 'AF Customer-Account Relationships' as info;
# MAGIC -- SELECT c.customer_id, c.first_name, c.last_name, COUNT(a.account_id) as account_count
# MAGIC -- FROM customers c 
# MAGIC -- LEFT JOIN accounts a ON c.customer_id = a.customer_id
# MAGIC -- GROUP BY c.customer_id, c.first_name, c.last_name
# MAGIC -- ORDER BY c.customer_id;
# MAGIC
# MAGIC -- SELECT 'AF Account-Transaction Relationships' as info;
# MAGIC -- SELECT a.account_id, a.account_type, a.balance, COUNT(t.transaction_id) as transaction_count
# MAGIC -- FROM accounts a 
# MAGIC -- LEFT JOIN transactions t ON a.account_id = t.account_id
# MAGIC -- GROUP BY a.account_id, a.account_type, a.balance
# MAGIC -- ORDER BY a.account_id;
# MAGIC
# MAGIC -- -- Verify relationships in BB dataspace
# MAGIC -- USE SCHEMA bb_dataspace;
# MAGIC
# MAGIC -- SELECT 'BB Client-Account Relationships' as info;
# MAGIC -- SELECT cc.client_id, cc.company_name, ca.account_id, ca.account_balance
# MAGIC -- FROM corporate_clients cc 
# MAGIC -- LEFT JOIN commercial_accounts ca ON cc.company_name = ca.business_name
# MAGIC -- ORDER BY cc.client_id;
# MAGIC
# MAGIC -- -- =====================================================================
# MAGIC -- -- SECTION 5: Summary Report
# MAGIC -- -- =====================================================================
# MAGIC
# MAGIC -- SELECT 'DATA INSERTION SUMMARY' as summary;
# MAGIC
# MAGIC -- -- Total records inserted
# MAGIC -- USE SCHEMA af_dataspace;
# MAGIC -- SELECT 
# MAGIC --     'AF Dataspace' as dataspace,
# MAGIC --     (SELECT COUNT(*) FROM customers) as customers,
# MAGIC --     (SELECT COUNT(*) FROM accounts) as accounts,
# MAGIC --     (SELECT COUNT(*) FROM transactions) as transactions;
# MAGIC
# MAGIC -- USE SCHEMA bb_dataspace;
# MAGIC -- SELECT 
# MAGIC --     'BB Dataspace' as dataspace,
# MAGIC --     (SELECT COUNT(*) FROM corporate_clients) as corporate_clients,
# MAGIC --     (SELECT COUNT(*) FROM commercial_accounts) as commercial_accounts,
# MAGIC --     (SELECT COUNT(*) FROM corporate_transactions) as corporate_transactions;
# MAGIC
# MAGIC -- -- Sample tagged data verification
# MAGIC -- USE SCHEMA af_dataspace;
# MAGIC -- SELECT 'Sample Tagged Data - AF Accounts' as verification;
# MAGIC -- SELECT account_id, account_type, balance FROM accounts WHERE account_id IN ('ACC001', 'ACC002', 'ACC003');
# MAGIC
# MAGIC -- USE SCHEMA bb_dataspace;
# MAGIC -- SELECT 'Sample Tagged Data - BB Commercial Accounts' as verification;
# MAGIC -- SELECT account_id, business_name, account_balance FROM commercial_accounts WHERE account_id IN ('COMM001', 'COMM002', 'COMM003');
# MAGIC
# MAGIC -- Note: All inserted data is now subject to the tag-based access controls
# MAGIC -- defined in the previous script. The tags will govern who can access
# MAGIC -- which data based on abac_sublob, abac_protection_group, etc. 

# COMMAND ----------


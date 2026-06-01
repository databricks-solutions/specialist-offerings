# Common EMR Bootstrap Patterns with Databricks Equivalents

## 1. Install Python Packages

**EMR Bootstrap:**
```bash
#!/bin/bash
sudo pip3 install pandas==2.0.0 scikit-learn==1.3.0 boto3
```

**Databricks Equivalent (PREFERRED - no init script needed):**
```python
# Option A: In notebook (best for interactive work)
%pip install pandas==2.0.0 scikit-learn==1.3.0 boto3

# Option B: requirements.txt on cluster (best for jobs)
# Upload requirements.txt to UC Volume, set as cluster library

# Option C: Cluster library via UI/API
# Cluster > Libraries > Install New > PyPI > package_name
```

---

## 2. Install System Packages (apt-get)

**EMR Bootstrap:**
```bash
#!/bin/bash
sudo yum install -y gcc libffi-devel openssl-devel
```

**Databricks Init Script:**
```bash
#!/bin/bash
apt-get update -y
apt-get install -y gcc libffi-dev libssl-dev
```

**Upload and configure:**
```python
# Upload to UC Volume
dbutils.fs.put("/Volumes/catalog/schema/init-scripts/install_packages.sh", open("install_packages.sh").read(), True)

# Or via Databricks CLI
# databricks fs cp install_packages.sh dbfs:/Volumes/catalog/schema/init-scripts/install_packages.sh
```

---

## 3. Copy JARs from S3

**EMR Bootstrap:**
```bash
#!/bin/bash
aws s3 cp s3://my-bucket/jars/custom-connector.jar /usr/lib/spark/jars/
```

**Databricks Equivalent (PREFERRED - no init script needed):**
```
1. Upload JAR to UC Volume:
   databricks fs cp custom-connector.jar /Volumes/catalog/schema/jars/custom-connector.jar

2. Attach as cluster library:
   Cluster > Libraries > Install New > Upload > JAR > /Volumes/catalog/schema/jars/custom-connector.jar
```

---

## 4. Set Environment Variables

**EMR Bootstrap:**
```bash
#!/bin/bash
echo "export APP_ENV=production" >> /etc/environment
echo "export API_ENDPOINT=https://api.example.com" >> /etc/environment
```

**Databricks Equivalent (PREFERRED - no init script needed):**
```json
{
  "spark_env_vars": {
    "APP_ENV": "production",
    "API_ENDPOINT": "https://api.example.com"
  }
}
```
Set via Cluster UI: Advanced Options > Spark > Environment Variables.

---

## 5. Configure Kerberos

**EMR Bootstrap:**
```bash
#!/bin/bash
sudo yum install -y krb5-workstation
sudo cp /tmp/krb5.conf /etc/krb5.conf
sudo kinit -kt /tmp/service.keytab service@REALM.COM
```

**Databricks Equivalent:**
Databricks manages authentication differently. For Kerberos-protected data sources:
- Use Databricks secrets to store credentials
- Use JDBC/ODBC connectors with Kerberos delegation tokens
- For HDFS access, configure `spark.hadoop.dfs.namenode.kerberos.principal` in Spark conf
- Consider using Unity Catalog external locations with storage credentials instead

---

## 6. Install Custom Monitoring Agents

**EMR Bootstrap:**
```bash
#!/bin/bash
curl -O https://monitoring.example.com/agent-install.sh
chmod +x agent-install.sh
sudo ./agent-install.sh --api-key $API_KEY
```

**Databricks Init Script:**
```bash
#!/bin/bash
curl -fsSL https://monitoring.example.com/agent-install.sh -o /tmp/agent-install.sh
chmod +x /tmp/agent-install.sh
/tmp/agent-install.sh --api-key "${MONITORING_API_KEY}"
```
Note: Set `MONITORING_API_KEY` in spark_env_vars or use Databricks secrets.

---

## 7. Mount EFS/NFS

**EMR Bootstrap:**
```bash
#!/bin/bash
sudo yum install -y nfs-utils
sudo mkdir -p /mnt/efs
sudo mount -t nfs4 fs-12345678.efs.us-east-1.amazonaws.com:/ /mnt/efs
echo "fs-12345678.efs.us-east-1.amazonaws.com:/ /mnt/efs nfs4 defaults 0 0" | sudo tee -a /etc/fstab
```

**Databricks Init Script:**
```bash
#!/bin/bash
apt-get update -y
apt-get install -y nfs-common
mkdir -p /mnt/efs
mount -t nfs4 fs-12345678.efs.us-east-1.amazonaws.com:/ /mnt/efs
```

---

## 8. Install ODBC/JDBC Drivers

**EMR Bootstrap:**
```bash
#!/bin/bash
sudo yum install -y unixODBC unixODBC-devel
sudo rpm -i https://packages.example.com/odbc-driver.rpm
sudo odbcinst -i -d -f /opt/driver/odbcinst.ini
```

**Databricks Init Script:**
```bash
#!/bin/bash
apt-get update -y
apt-get install -y unixodbc unixodbc-dev
curl -fsSL https://packages.example.com/odbc-driver.deb -o /tmp/odbc-driver.deb
dpkg -i /tmp/odbc-driver.deb
```
Note: For JDBC drivers, prefer uploading the JAR as a cluster library instead.

---

## 9. Configure Custom DNS

**EMR Bootstrap:**
```bash
#!/bin/bash
echo "nameserver 10.0.0.2" | sudo tee /etc/resolv.conf
echo "search internal.example.com" | sudo tee -a /etc/resolv.conf
```

**Databricks Init Script:**
```bash
#!/bin/bash
cat > /etc/resolv.conf << EOF
nameserver 10.0.0.2
search internal.example.com
EOF
```
Note: Consider using VPC DNS settings or Private Link instead for a more robust solution.

---

## 10. Download Model Files

**EMR Bootstrap:**
```bash
#!/bin/bash
aws s3 cp s3://my-bucket/models/model.pkl /opt/models/
aws s3 cp s3://my-bucket/models/tokenizer/ /opt/models/tokenizer/ --recursive
```

**Databricks Equivalent (PREFERRED - no init script needed):**
```python
# Option A: Use UC Volumes (best)
# Upload models to /Volumes/catalog/schema/models/
# Access directly in code: "/Volumes/catalog/schema/models/model.pkl"

# Option B: Use MLflow Model Registry
import mlflow
model = mlflow.pyfunc.load_model("models:/my_model/Production")

# Option C: Use Databricks secrets + init script if models are very large
```

**Init Script (if needed for large models pre-loaded on all nodes):**
```bash
#!/bin/bash
mkdir -p /opt/models
# Use Databricks CLI or curl to download from UC Volumes
cp /dbfs/Volumes/catalog/schema/models/* /opt/models/
```

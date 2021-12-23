# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Delta Sharing - Securely share Data with external team / partners
# MAGIC 
# MAGIC * Share existing, live data in data lakes / lakehouses (no need to copy it out)
# MAGIC * Support a wide range of clients by using existing, open data formats (pandas, spark, Tableau etc)
# MAGIC * Strong security, auditing and governance
# MAGIC * Efficiently scale to massive datasets
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/delta-sharing/resources/images/delta-sharing-flow.png" width="900px"/>
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fdelta_sharing%2Fdelta_lake&dt=DELTA">

# COMMAND ----------

# MAGIC %md ##Start by downloading your credentials to have access to your share
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-share-recipient-download.png"/>

# COMMAND ----------

# MAGIC %md ## Working with the Python API
# MAGIC Delta Sharing requires the python lib to read the external share with a given credential

# COMMAND ----------

# DBTITLE 1,Delta sharing python lib installation
# MAGIC %pip install delta-sharing

# COMMAND ----------

# DBTITLE 1,Creating a share conf file with your secret
# MAGIC %sh 
# MAGIC mkdir -p /dbfs/tmp/delta_share_demo
# MAGIC cat <<EOT > /dbfs/tmp/delta_share_demo/open-datasets.share
# MAGIC {
# MAGIC   "shareCredentialsVersion": 1,
# MAGIC   "endpoint": "https://sharing.delta.io/delta-sharing/",
# MAGIC   "bearerToken": "faaie590d541265bcab1f2de9813274bf233"
# MAGIC }
# MAGIC EOT

# COMMAND ----------

import os
import delta_sharing

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = "/dbfs/tmp/delta_share_demo/open-datasets.share"

# Create a SharingClient.
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
print("########### All Available Tables #############")
print(client.list_all_tables())

# COMMAND ----------

# Load a table as a Pandas DataFrame. This can be used to process tables that can fit in the memory.
print("########### Loading delta_sharing.default.boston-housing as a Pandas DataFrame #############")
data = delta_sharing.load_as_pandas(profile_file + "#delta_sharing.default.boston-housing")

# Do whatever you want to your share data!
print("########### Show Data #############")
data[data["age"] > 18].head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working at scale with spark
# MAGIC 
# MAGIC Need to process big dataset in parallel? Load them with Delta Sharing spark connector (cluster requires `io.delta:delta-sharing-spark_2.12:0.2.0`)

# COMMAND ----------

import os
import delta_sharing
from pyspark.sql import SparkSession

# Read data using format "deltaSharing"
spark.read.format("deltaSharing").load("/tmp/quentin/open-datasets.share" + "#delta_sharing.default.boston-housing") \
	 .where("age > 18") \
	 .display()

# COMMAND ----------

# MAGIC %md ## Exporing REST API Using Databricks OSS Delta Sharing Server
# MAGIC 
# MAGIC Databricks hosts a sharing server for test: https://sharing.delta.io/ 
# MAGIC 
# MAGIC *Note: it doesn't require authentification, real-world scenario require a Bearer token in your calls*

# COMMAND ----------

# DBTITLE 1,Installing jq to have nice json display as cells output
# MAGIC %sh sudo apt-get install jq

# COMMAND ----------

# DBTITLE 1,List Shares, a share is a top level container
# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares -s | jq '.'

# COMMAND ----------

# DBTITLE 1,List Schema within the delta_sharing share
# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas -s | jq '.'

# COMMAND ----------

# DBTITLE 1,List the tables within our share
# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables -s | jq '.'

# COMMAND ----------

# MAGIC %md ### Get metadata from our "boston-housing" table

# COMMAND ----------

# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables/boston-housing/metadata -s | jq '.'

# COMMAND ----------

# MAGIC %md ### Getting the data
# MAGIC Delta Share works by creating temporary self-signed links to download the underlying files. It leverages Delta Lake statistics to pushdown the query and only retrive a subset of file. 
# MAGIC 
# MAGIC The REST API allow you to get those links and dowload the data:

# COMMAND ----------

# DBTITLE 1,Getting access to boston-housing data
# MAGIC %sh curl -X POST https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables/boston-housing/query -s -H 'Content-Type: application/json' -d @- << EOF
# MAGIC {
# MAGIC    "predicateHints" : [
# MAGIC       "date >= '2021-01-01'",
# MAGIC       "date <= '2021-01-31'"
# MAGIC    ],
# MAGIC    "limitHint": 1000
# MAGIC }
# MAGIC EOF

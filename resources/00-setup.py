# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

mount_name = "field-demos"

try:
  dbutils.fs.ls("/mnt/%s" % mount_name)
except:
  if "azure" in dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get():
    print("ADLS2 isn't mounted, mount the demo data under %s" % mount_name)
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "common-sp", key = "common-sa-sp-client-id"),
              "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "common-sp", key = "common-sa-sp-client-secret"),
              "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

    dbutils.fs.mount(
      source = "abfss://field-demos@fielddemosdatasets.dfs.core.windows.net/field-demos",
      mount_point = "/mnt/"+mount_name,
      extra_configs = configs)
  else:
    aws_bucket_name = ""
    print("bucket isn't mounted, mount the demo bucket under %s" % mount_name)
    dbutils.fs.mount(f"s3a://databricks-datasets-private/field-demos" , f"/mnt/{mount_name}")

# COMMAND ----------

import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbName = "delta_"+re.sub(r'\W+', '_', current_user)
cloud_storage_path = "/Users/{}/field_demos_delta".format(current_user)
dbutils.widgets.text("path", cloud_storage_path, "path")
dbutils.widgets.text("dbName", dbName, "dbName")
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")
print("using cloud_storage_path {}".format(cloud_storage_path))
print("using database: {}".format(dbName))

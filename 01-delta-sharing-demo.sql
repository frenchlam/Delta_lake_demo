-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Sharing - Securely share Data with external team / partners
-- MAGIC 
-- MAGIC * Share existing, live data in data lakes / lakehouses (no need to copy it out)
-- MAGIC * Support a wide range of clients by using existing, open data formats (pandas, spark, Tableau etc)
-- MAGIC * Strong security, auditing and governance
-- MAGIC * Efficiently scale to massive datasets
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/delta-sharing/resources/images/delta-sharing-flow.png" width="900px"/>
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fdelta_sharing%2Fdelta_lake&dt=DELTA">

-- COMMAND ----------

-- MAGIC %md ## Creating share & granting access to external supplier with Databricks (requires Unity Catalog!)

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS delta_sharing;

-- COMMAND ----------

ALTER SHARE delta_sharing ADD TABLE main.retail.customers;
ALTER SHARE delta_sharing ADD TABLE main.retail.customer_satisfaction;

SHOW ALL IN SHARE delta_sharing;

-- COMMAND ----------

-- MAGIC %md ##Sharing the data to external user

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE RECIPIENT if not exists external_organization ;
-- MAGIC 
-- MAGIC GRANT SELECT ON SHARE delta_sharing TO RECIPIENT external_organization;

-- COMMAND ----------

-- DBTITLE 1,This will display the link to download the credential
-- MAGIC %sql
-- MAGIC SHOW GRANT ON SHARE delta_sharing;

-- COMMAND ----------

-- MAGIC %md ##You can then share the activation link to your external organization
-- MAGIC The activation link can be shared and the recipient credential downloaded: 
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-share-recipient-download.png"/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## That's all you need, your data is ready to be shared accross your Data Mesh & with other organizations!
-- MAGIC Once the credential file / link is shared, we can now download the existing data

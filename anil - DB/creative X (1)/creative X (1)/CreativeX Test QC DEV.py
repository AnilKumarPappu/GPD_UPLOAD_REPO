# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def dev_synapse_jdbc_connection(dbtable):
    url = "jdbc:sqlserver://globalxsegsrmdatafoundationdevsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
    user_name = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databricksusername"
    )
    password = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databrickspassword2"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user_name)
        .option("password", password)
        .option("authentication", "ActiveDirectoryPassword")
        .load()
    )
    return df

# COMMAND ----------

fact_creativeX = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_fact_creativetest')
dim_creative = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_creative')
dim_channel = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_channel')
dim_campaign = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_campaign')
dim_product = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_product')
dim_strategy = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_strategy')
dim_mediabuy = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_mediabuy')
dim_site = dev_synapse_jdbc_connection('mm_test.vw_mw_gpd_dim_site')

# COMMAND ----------

dim_creative = dim_creative.filter(col("VendorID") == "CREATIVEX")
dim_channel = dim_channel.filter(col("VendorID") == "CREATIVEX")
dim_campaign = dim_campaign.filter(col("VendorID") == "CREATIVEX")
dim_product = dim_product.filter(col("VendorID") == "CREATIVEX")
dim_strategy = dim_strategy.filter(col("VendorID") == "CREATIVEX")
dim_mediabuy = dim_mediabuy.filter(col("VendorID") == "CREATIVEX")
dim_site = dim_site.filter(col("VendorID") == "CREATIVEX")

# COMMAND ----------

fact_creativeX.limit(2000).display()

# COMMAND ----------

dim_creative.display()

# COMMAND ----------

dim_channel.display() 

# COMMAND ----------

dim_campaign.display() 

# COMMAND ----------

dim_product.display() 

# COMMAND ----------

dim_strategy.display() 

# COMMAND ----------

dim_mediabuy.display() 

# COMMAND ----------

dim_site.display() 

# COMMAND ----------

fact_creativeX.join(dim_creative,on= "creative_id").join(dim_channel.select("channel_id","platform_desc"), on = "channel_id").join(dim_product.select("product_id","brand"), on = "product_id").display()

# COMMAND ----------

dim_creative.display()

# COMMAND ----------

dim_channel.display()

# COMMAND ----------

dim_campaign.display()

# COMMAND ----------

dim_product.display()

# COMMAND ----------

dim_strategy.display()

# COMMAND ----------

dim_mediabuy.display()

# COMMAND ----------

dim_site.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

fact_creativeX.createOrReplaceTempView("creativeX")
dim_creative.createOrReplaceTempView("creative")
dim_channel.createOrReplaceTempView("channel")
dim_campaign.createOrReplaceTempView("campaign")
dim_product.createOrReplaceTempView("product")
dim_strategy.createOrReplaceTempView("strategy")
dim_mediabuy.createOrReplaceTempView("mediabuy")
dim_site.createOrReplaceTempView("site")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from creative

# COMMAND ----------

dim_creative.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_creative.columns]).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from creative
# MAGIC where isnan(creative_audience_type)

# COMMAND ----------

dim_creative.filter(col('creative_audience_type').isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking the granularity of fact creative test

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from creativeX

# COMMAND ----------

# MAGIC %sql
# MAGIC select creative_id, guideline_id, count(*) as row_count
# MAGIC from creativeX
# MAGIC group by creative_id, guideline_id
# MAGIC having count(*) > 1

# COMMAND ----------

fact_creativeX.select([count(when(col(c).isNull(), c)).alias(c) for c in fact_creativeX.columns]).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select date_id, count(distinct creative_id)
# MAGIC from creativeX
# MAGIC group by date_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Campaign tables columns

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from campaign

# COMMAND ----------

dim_campaign.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_campaign.columns]).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from product

# COMMAND ----------

dim_product.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_product.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### New primary key logic (VendorID + Previous PK) should stand true.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct channel_id, concat(vendor_id,'_',country_id,'_',channel_code) as new_id
# MAGIC from channel
# MAGIC where channel_id <> concat(vendor_id,'_',country_id,'_',channel_code)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct gpd_campaign_id, concat(vendor_id,'_',country_id,'_',gpd_campaign_code) as new_id
# MAGIC from campaign
# MAGIC where gpd_campaign_id <> concat(vendor_id,'_',country_id,'_',gpd_campaign_code)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct creative_id, concat(vendor_id,'_',country_id,'_',creative_code) as new_id
# MAGIC from creative
# MAGIC where creative_id <> concat(vendor_id,'_',country_id,'_',creative_code)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct media_buy_id, concat(vendor_id,'_',country_id,'_',media_buy_code) as new_id
# MAGIC from mediabuy
# MAGIC where media_buy_id <> concat(vendor_id,'_',country_id,'_',media_buy_code)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct strategy_id, concat(vendor_id,'_',country_id,'_',strategy_code) as new_id
# MAGIC from strategy
# MAGIC where strategy_id <> concat(vendor_id,'_',country_id,'_',strategy_code)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct site_id, concat(vendor_id,'_',country_id,'_',site_code) as new_id
# MAGIC from site
# MAGIC where site_id <> concat(vendor_id,'_',country_id,'_',site_code)

# COMMAND ----------


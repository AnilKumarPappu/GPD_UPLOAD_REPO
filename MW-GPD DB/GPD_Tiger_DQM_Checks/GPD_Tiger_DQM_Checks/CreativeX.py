# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def prod_synapse_jdbc_connection(dbtable):
    url = "jdbc:sqlserver://globalxsegsrmdatafoundationprodsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
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

fact_creativeX = prod_synapse_jdbc_connection('mm.vw_mw_gpd_fact_creativetest')
dim_creative = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_creative')
dim_channel = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_channel')
dim_campaign = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_campaign')
dim_product = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_product')
dim_strategy = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_strategy')
dim_mediabuy = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_mediabuy')
dim_site = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_site')

# COMMAND ----------

dim_creative = dim_creative.filter(col("VendorID") == "CREATIVEX")
dim_channel = dim_channel.filter(col("VendorID") == "CREATIVEX")
dim_campaign = dim_campaign.filter(col("VendorID") == "CREATIVEX")
dim_product = dim_product.filter(col("VendorID") == "CREATIVEX")
dim_strategy = dim_strategy.filter(col("VendorID") == "CREATIVEX")
dim_mediabuy = dim_mediabuy.filter(col("VendorID") == "CREATIVEX")
dim_site = dim_site.filter(col("VendorID") == "CREATIVEX")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 campaign key should not be assigned to multiple campaign names

# COMMAND ----------


dim_campaign.select('gpd_campaign_id','campaign_desc').groupBy("gpd_campaign_id").count().filter(col("count") > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### (No blank/null values in all Global filters) OR  (Fact_CreativeX table data should be present in all creative X dim tables)

# COMMAND ----------

fact_creativeX.join(dim_creative, on = 'creative_id', how = 'left_anti').display()
fact_creativeX.join(dim_campaign, on = 'gpd_campaign_id', how = 'left_anti').display()
fact_creativeX.join(dim_channel, on = 'channel_id', how = 'left_anti').display()
fact_creativeX.join(dim_product, on = 'product_id', how = 'left_anti').display()
# fact_creativeX.join(dim_mediabuy, on = 'meida_buy_id', how = 'left_anti').display()
# fact_creativeX.join(dim_site, on = 'site_id', how = 'left_anti').display()
# fact_creativeX.join(dim_strategy, on = 'strategy_id', how = 'left_anti').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking granularity of fact_creativeX

# COMMAND ----------

fact_creativeX.groupBy("creative_id","guideline_id").count().filter(col("count")>1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Guideline value should be bw 0 and 1

# COMMAND ----------

fact_creativeX.filter(~((col('guideline_value') <= 1) &(col("guideline_value") >= 0))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### creative_quality_score value should be bw 0 and 1

# COMMAND ----------

fact_creativeX.filter(~((col("creative_quality_score") <= 1) &(col("creative_quality_score") >= 0))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### creative_quality_tier should be one of those values

# COMMAND ----------

# dim_creative.select("creative_quality_tier").distinct().display()
dim_creative.filter(~col("creative_quality_tier").isin(['Excellent','On Track','Needs Work','NA'])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if the primary keys are valid in dim tables & their count
# MAGIC --> Since the tables are following star schema, it's important to check if the primary keys are holding correctly.

# COMMAND ----------

dim_product.groupBy("product_id").count().filter(col("count") > 1).join(
    dim_product, "product_id"
).display()
dim_campaign.groupBy("gpd_campaign_id").count().filter(col("count") > 1).join(
    dim_campaign, "gpd_campaign_id"
).display()
dim_creative.groupBy("creative_id").count().filter(col("count") > 1).join(
    dim_creative, "creative_id"
).display()
dim_channel.groupBy("channel_id").count().filter(col("count") > 1).join(
    dim_channel, "channel_id"
).display()
dim_mediabuy.groupBy("media_buy_id").count().filter(col("count") > 1).join(
    dim_mediabuy, "media_buy_id"
).display()
dim_site.groupBy("site_id").count().filter(col("count") > 1).join(
    dim_site, "site_id"
).display()
dim_strategy.groupBy("strategy_id").count().filter(col("count") > 1).join(
    dim_strategy, "strategy_id"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### There should not be any null values in all Tables

# COMMAND ----------

# checcking Null values
dim_creative.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_creative.columns]).display()
dim_channel.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_channel.columns]).display()
dim_campaign.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_campaign.columns]).display()
dim_product.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_product.columns]).display()


# COMMAND ----------

# checcking NA values
dim_creative.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_creative.columns]).display()
dim_channel.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_channel.columns]).display()
dim_campaign.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_campaign.columns]).display()
dim_product.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_product.columns]).display()

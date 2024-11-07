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

creative_summary = fact_creativeX.join(dim_creative.select('creative_id','creative_desc','creative_quality_tier'), on = "creative_id", how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id', how= 'left')
# creative_summary.display()

# COMMAND ----------

creative_summary.display()

# COMMAND ----------

preFlight = creative_summary.select('creative_id','creative_desc','guideline_name','status').filter(col('status') == 'Pre-Flight')
inFlight = creative_summary.select('creative_id','creative_desc','guideline_name','status').filter(col('status') == 'In-Flight')

# COMMAND ----------

# preFlight.display()
inFlight.display()

# COMMAND ----------

preFlight.count()
# inFlight.count()

# COMMAND ----------

preFlight.join(inFlight, on = ['creative_desc','guideline_name'], how = 'inner').display()
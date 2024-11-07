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

fact_creativeX.select('status').distinct().display()

# COMMAND ----------

fact_creativeX.display()

# COMMAND ----------

dim_creative.display()

# COMMAND ----------

dim_channel.display()

# COMMAND ----------

dim_product.display()

# COMMAND ----------

# MAGIC %md
# MAGIC column 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Creative test page 

# COMMAND ----------

creative = fact_creativeX.join(dim_creative.select('creative_id','creative_desc','creative_quality_tier','creative_aspect_ratio','creative_company','ad_tag_size'), on = "creative_id", how = 'right').join(dim_channel.select('channel_id','platform_desc',), on = 'channel_id', how = 'left').join(dim_product.select('product_id','brand','segment'), on = 'product_id', how= 'left')

# COMMAND ----------

creative.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Quality checks using Creative_Desc

# COMMAND ----------

creative.filter(col('creative_desc') =='Companion HFSS Edit 3 Maltesers UK 20.mp4').display()


# COMMAND ----------

creative.groupBy('creative_desc').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Creative Guidline perfomance across Platform

# COMMAND ----------

creative.filter(col('creative_desc')== "Holiday DCO M&M's US 6.mp4").groupBy('platform_desc', 'guideline_name','guideline_value').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### guideline performance

# COMMAND ----------

creative.groupBy("guideline_name").agg(sum("guideline_value")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### platfrom level

# COMMAND ----------

creative.groupBy('platform_desc','guideline_name').agg(sum("guideline_value")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### platform X brand

# COMMAND ----------

creative.groupBy('platform_desc','brand','guideline_name').agg(sum("guideline_value")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### brand null 

# COMMAND ----------

creative.filter(col('brand').isNull()).display()
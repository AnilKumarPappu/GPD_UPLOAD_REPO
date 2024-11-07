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

# dim_campaign = dim_campaign.filter(col("VendorID") == "CREATIVEX")
dim_campaign.display()


# COMMAND ----------

fact_creativeX.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Date slicing

# COMMAND ----------

fact_creativeX = fact_creativeX.filter(col('date_id') <= '2024-04-16' )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creative summary page
# MAGIC

# COMMAND ----------

creative_summary = fact_creativeX.join(dim_creative.select('creative_id','creative_desc','creative_quality_tier'), on = "creative_id", how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id', how= 'left').join(dim_campaign.select('gpd_campaign_id', 'campaign_desc'),on = 'gpd_campaign_id', how = 'left')
# creative_summary.display()

# COMMAND ----------

creative_summary.display()

# COMMAND ----------

# creative_summary.filter(col('creative_id')=='CREATIVEX_MY_60327327').display()
creative_summary.groupBy('creative_id').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Guideline performance
# MAGIC
# MAGIC

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy("guideline_name").count().display()

creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy("guideline_name").count().display()



# COMMAND ----------

# MAGIC %md
# MAGIC ######## platform level
# MAGIC

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy('platform_desc','guideline_name').count().display()

creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('platform_desc','guideline_name').count().display()




# COMMAND ----------

# MAGIC %md
# MAGIC ########### campaign_desc level

# COMMAND ----------

creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('campaign_desc','guideline_name').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ########## platform x brand level

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy('platform_desc','brand','guideline_name').count().display()
creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('platform_desc','brand','guideline_name').count().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ######### Fit Tier level

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy('creative_quality_tier','guideline_name').count().display()

creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('creative_quality_tier','guideline_name').count().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Brand Following/ missing most guidlines

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy('brand').count().display()


creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('brand').count().display()



# COMMAND ----------

# MAGIC %md
# MAGIC ########## platform level

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy('platform_desc','brand').count().display()

creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('platform_desc','brand').count().display()



# COMMAND ----------

# MAGIC %md
# MAGIC ######## Fit Tier level

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').filter(col('guideline_value') == 1).groupBy('creative_quality_tier','brand').count().display()

creative_summary.filter(col('status') == 'In-Flight').filter(col('guideline_value') == 1).groupBy('creative_quality_tier','brand').count().display()



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Average Quality score tier

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').select(mean('creative_quality_score')).display()

creative_summary.filter(col('status') == 'In-Flight').select(mean('creative_quality_score')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######## platform level

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').groupBy('platform_desc').agg(avg('creative_quality_score')).display()

creative_summary.filter(col('status') == 'In-Flight').groupBy('platform_desc').agg(avg('creative_quality_score')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### campaign_desc level

# COMMAND ----------

creative_summary.filter(col('status') == 'In-Flight').groupBy('campaign_desc').agg(avg('creative_quality_score')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######### Fit Tier level

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').groupBy('creative_quality_tier').agg(avg('creative_quality_score')).display()

creative_summary.filter(col('status') == 'In-Flight').groupBy('creative_quality_tier').agg(avg('creative_quality_score')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creative quality tier 

# COMMAND ----------

# MAGIC %md
# MAGIC ######## Total creatives

# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').select('creative_id','creative_quality_tier').dropDuplicates().groupBy('creative_quality_tier').count().display()

creative_summary.filter(col('status') == 'In-Flight').select('creative_id','creative_quality_tier').dropDuplicates().groupBy('creative_quality_tier').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######## Platform level

# COMMAND ----------

# At platform level
creative_summary.filter(col('status') == 'Pre-Flight').select('creative_id','platform_desc','creative_quality_tier').dropDuplicates().groupBy('platform_desc','creative_quality_tier').count().display()

# At platform level
creative_summary.filter(col('status') == 'In-Flight').select('creative_id','platform_desc','creative_quality_tier').dropDuplicates().groupBy('platform_desc','creative_quality_tier').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####### campaign_desc level

# COMMAND ----------

creative_summary.filter(col('status') == 'In-Flight').select('creative_id','campaign_desc','platform_desc','creative_quality_tier').dropDuplicates().groupBy('campaign_desc','creative_quality_tier').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ############## Total spend

# COMMAND ----------

creative_summary.filter(col('status') == 'In-Flight').select('creative_id','creative_quality_tier','spend').dropDuplicates().groupBy('creative_quality_tier').agg(sum('spend')).display()

# .withColumn('percent', (F.col("sum('spend')")/n)*100)

# COMMAND ----------

# MAGIC %md
# MAGIC ############### platform level

# COMMAND ----------

# At platform level
creative_summary.filter(col('status') == 'In-Flight').select('creative_id','platform_desc','creative_quality_tier','spend').dropDuplicates().groupBy('platform_desc','creative_quality_tier').agg(sum('spend')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######## campaign_desc level

# COMMAND ----------

creative_summary.filter(col('status') == 'In-Flight').select('creative_id', 'campaign_desc','platform_desc','creative_quality_tier','spend').dropDuplicates().groupBy('campaign_desc','creative_quality_tier').agg(sum('spend')).display()

# COMMAND ----------

# crative count at Platform level
creative_summary.filter(col('status') == 'Pre-Flight').select('creative_id','platform_desc').dropDuplicates().groupBy('platform_desc').count().display()

creative_summary.filter(col('status') == 'In-Flight').select('creative_id','platform_desc').dropDuplicates().groupBy('platform_desc').count().display()
# Total count of creatives 


# COMMAND ----------

creative_summary.filter(col('status') == 'Pre-Flight').select('creative_id').distinct().count()


# COMMAND ----------

creative_summary.filter(col('status') == 'In-Flight').select('creative_id').distinct().count()

# COMMAND ----------

creative_summary.select('creative_desc','creative_quality_tier').dropDuplicates().groupBy('creative_desc').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creative test details page

# COMMAND ----------

creative_summary_testpage = fact_creativeX.join(dim_creative.select('creative_id','creative_desc','creative_quality_tier','creative_aspect_ratio','creative_company','ad_tag_size'), on = "creative_id", how = 'right').join(dim_channel.select('channel_id','platform_desc',), on = 'channel_id', how = 'left').join(dim_product.select('product_id','brand','segment'), on = 'product_id', how= 'left')

# COMMAND ----------

creative_summary_testpage.filter(col('creative_desc') == '0d267678-b1d4-4633-9f4d-28cdd56b57a2-GFA0jBnRtliB3a8CAAilQXVNZQNObmdjAAAF.mp4').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### brand NULL issue
# MAGIC

# COMMAND ----------

# creative_summary.select('brand').distinct().display()
creative_summary.filter(col('brand').isNull()).display()

# COMMAND ----------

dim_product.filter(col('product_id').isin(['CREATIVEX_US_MW_53'])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
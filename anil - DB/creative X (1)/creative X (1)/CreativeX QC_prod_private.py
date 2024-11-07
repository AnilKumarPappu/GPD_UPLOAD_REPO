# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def prod_synapse_jdbc_connection(dbtable):
    url = "jdbc:sqlserver://xsegglobalmktgprodsynapse-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
    user_name = dbutils.secrets.get(
        scope="globalxsegsrmdatafoundationprodsecretscope", key="globalxsegsrmdatafoundationprodsynapseuserid"
    )
    password = dbutils.secrets.get(
        scope="globalxsegsrmdatafoundationprodsecretscope", key="globalxsegsrmdatafoundationprodsynapsepass"
    )

    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user_name)
        .option("password", password)
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
dim_country = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_country')

# COMMAND ----------

fact_creativeX.join(dim_country.select('country_id','country_desc'),on = 'country_id',how = 'left').filter(col('country_id').isin(['GB'])).join(dim_campaign.select('gpd_campaign_id','campaign_desc'), on = 'gpd_campaign_id', how = 'left').join(dim_channel.select('channel_id','channel_desc'),on = 'channel_id',how = 'left').join(dim_creative.select('creative_id','creative_desc'), on = 'creative_id',how = 'left').join(dim_product.select('product_id','product','brand'),on= 'product_id',how = 'left').select('country_id','country_desc','gpd_campaign_id','campaign_desc','channel_id','channel_desc','creative_id','creative_desc','product_id','product','brand','date_id',
 'currency',
 'status',
 'guideline_id',
 'guideline_name',
 'guideline_value',
 'call_to_action_flag',
 'spend',
 'clicks',
 'impressions',
 'ctr',
 'cpm',
 'cpc',
 'video_3_sec_views',
 'video_15_sec_views','video_30_sec_views',
 'video_completion_25',
 'video_completion_50',
 'video_completion_75',
 'video_fully_played',
 'creative_quality_score',
 'CountryID',
 'VendorID',
 'country_vendor').display()

# COMMAND ----------

fact_creativeX.columns

# COMMAND ----------

df_fact_performance = prod_synapse_jdbc_connection('mm.vw_mw_gpd_fact_performance')


# COMMAND ----------

dim_creative = dim_creative.filter(col("VendorID") == "CREATIVEX")
dim_channel = dim_channel.filter(col("VendorID") == "CREATIVEX")
dim_campaign = dim_campaign.filter(col("VendorID") == "CREATIVEX")
dim_product = dim_product.filter(col("VendorID") == "CREATIVEX")
dim_strategy = dim_strategy.filter(col("VendorID") == "CREATIVEX")
dim_mediabuy = dim_mediabuy.filter(col("VendorID") == "CREATIVEX")
dim_site = dim_site.filter(col("VendorID") == "CREATIVEX")


# COMMAND ----------

fact_creativeX.join(dim_product, on = 'product_id',how = 'left_anti').select('product_id').distinct().display()

# COMMAND ----------

# list of countries which are in creativeX but not in datorama
country_c = fact_creativeX.join(dim_country, on = 'country_id', how = 'left').select('country_id','country_desc').distinct()
country_D = df_fact_performance.join(dim_country, on = 'country_id', how = 'left').select('country_id','country_desc').distinct()
country_c.join(country_D, on = 'country_id', how = 'left_anti').display()

# COMMAND ----------

df_fact_performance.join(dim_country, on = 'country_id', how = 'left').select('country_id','country_desc').distinct().display()

# COMMAND ----------

dim_campaign = prod_synapse_jdbc_connection('mm.vw_mw_gpd_dim_campaign')

# COMMAND ----------

dim_campaign_C = dim_campaign.filter(col("VendorID") == "CREATIVEX")
dim_campaign_D = dim_campaign.filter(col("VendorID") == "DATORAMA")


# COMMAND ----------

dim_campaign_C.display()



# COMMAND ----------

# dim_campaign_C.select('campaign_desc','country_id').distinct().display() #475
dim_campaign_D.select('campaign_desc','country_id').distinct().display() #4606
# dim_campaign_C.groupBy('campaign_desc').count().display()



# COMMAND ----------

dim_campaign_D.display()

# COMMAND ----------

dim_campaign_C.join(dim_campaign_D, on = 'campaign_desc', how = 'left_anti').select('campaign_desc').distinct().display()

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
# MAGIC ### Checking granularity of fact_creativeX

# COMMAND ----------

fact_creativeX.groupBy("creative_id","guideline_id").count().filter(col("count")>1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### #No of countries in creative X

# COMMAND ----------

fact_creativeX.select("country_id").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Latest data available according to country

# COMMAND ----------

fact_creativeX.groupBy("country_id").agg(
    min("date_id"), max("date_id"), count("date_id")
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


# COMMAND ----------

dim_campaign.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### New primary key logic (VendorID + Previous PK) should stand true.

# COMMAND ----------

dim_channel.withColumn("new_id", concat(col("vendor_id"),lit("_"), col("country_id"),lit("_"), col("channel_code")) ).filter(col("new_id") != col("channel_id")).display()


# COMMAND ----------

dim_campaign.withColumn("new_id", concat(col("vendor_id"),lit("_"), col("country_id"),lit("_"), col("gpd_campaign_code")) ).filter(col("new_id") != col("gpd_campaign_id")).display()

# COMMAND ----------

dim_creative.withColumn("new_id", concat(col("vendor_id"),lit("_"), col("country_id"),lit("_"), col("creative_code")) ).filter(col("new_id") != col("creative_id")).display()


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
# MAGIC ### Check whether all product Id s in fact table are in product table

# COMMAND ----------

fact_creativeX.join(dim_product, on = "product_id", how = 'left').filter(col("portfolio").isNull()).display()
# dim_product.join(fact_creativeX, on = "product_id", how = 'left').filter(col("guideline_id").isNull()).display()

# COMMAND ----------

# dim_product.filter(col("product_id")=="CREATIVEX_GB_MW_49").display()
pro = dim_product.select(col("product_id")).distinct()
fact = fact_creativeX.select(col("product_id")).distinct()
fact.subtract(pro).display()
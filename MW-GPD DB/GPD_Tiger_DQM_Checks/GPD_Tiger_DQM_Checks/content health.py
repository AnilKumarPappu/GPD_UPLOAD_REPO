# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Importing libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

#Prod Connection
def jdbc_connection(dbtable):
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


# Dev Connection

# def jdbc_connection(dbtable):
#     url = "jdbc:sqlserver://globalxsegsrmdatafoundationdevsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
#     user_name = dbutils.secrets.get(
#         scope="mwoddasandbox1005devsecretscope", key="databricksusername"
#     )
#     password = dbutils.secrets.get(
#         scope="mwoddasandbox1005devsecretscope", key="databrickspassword2"
#     )
#     df = (
#         spark.read.format("jdbc")
#         .option("url", url)
#         .option("dbtable", dbtable)
#         .option("user", user_name)
#         .option("password", password)
#         .option("authentication", "ActiveDirectoryPassword")
#         .load()
#     )
#     return df

# COMMAND ----------

#Prod Tables
df_reach = jdbc_connection('mm.vw_mw_gpd_fact_reach')
df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')
df_planned_spend = jdbc_connection('mm.vw_mw_gpd_fact_plannedspend')
dim_campaign = jdbc_connection('mm.vw_mw_gpd_dim_campaign')
dim_channel = jdbc_connection('mm.vw_mw_gpd_dim_channel')
dim_creative = jdbc_connection('mm.vw_mw_gpd_dim_creative')
dim_strategy = jdbc_connection('mm.vw_mw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection('mm.vw_mw_gpd_dim_mediabuy')
dim_site = jdbc_connection('mm.vw_mw_gpd_dim_site')
dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')

# COMMAND ----------

# MAGIC %md
# MAGIC Preliminary checks

# COMMAND ----------

# MAGIC %md
# MAGIC 1. there should not be any empty table 

# COMMAND ----------


print('df_reach--->', df_reach.count())
print('df_planned_spend--->', df_planned_spend.count())
print('dim_campaign--->', dim_campaign.count())
print('dim_channel--->', dim_channel.count())
print('dim_strategy--->',dim_strategy.count())
print('dim_mediabuy--->', dim_mediabuy.count())
print('dim_site--->', dim_site.count())
print('df_fact_performance--->', df_fact_performance.count())
print('dim_creative--->' ,dim_creative.count())
print('dim_country--->',dim_country.count())
print('dim_product--->',dim_product.count())

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE--> Filtering countries as per our requirements. Skip if all countries are needed

# COMMAND ----------

countries = []

skip_countries = [

]

if countries:
    df_reach = df_reach.filter(col("country_id").isin(countries))
    df_fact_performance = df_fact_performance.filter(col("country_id").isin(countries))
    df_planned_spend = df_planned_spend.filter(col("country_id").isin(countries))
    dim_campaign = dim_campaign.filter(col("country_id").isin(countries))
    dim_channel = dim_channel.filter(col("country_id").isin(countries))
    dim_creative = dim_creative.filter(col("country_id").isin(countries))
    dim_strategy = dim_strategy.filter(col("country_id").isin(countries))
    dim_mediabuy = dim_mediabuy.filter(col("country_id").isin(countries))
    dim_site = dim_site.filter(col("country_id").isin(countries))

if skip_countries:
    df_reach = df_reach.filter(~col("country_id").isin(countries))
    df_fact_performance = df_fact_performance.filter(~col("country_id").isin(countries))
    df_planned_spend = df_planned_spend.filter(~col("country_id").isin(countries))
    dim_campaign = dim_campaign.filter(~col("country_id").isin(countries))
    dim_channel = dim_channel.filter(~col("country_id").isin(countries))
    dim_creative = dim_creative.filter(~col("country_id").isin(countries))
    dim_strategy = dim_strategy.filter(~col("country_id").isin(countries))
    dim_mediabuy = dim_mediabuy.filter(~col("country_id").isin(countries))
    dim_site = dim_site.filter(~col("country_id").isin(countries))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC 2. Check if the primary keys are valid in dim tables & their count
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
dim_mediabuy.groupBy("media_buy_id").count().filter(col("count") > 1).join(
    dim_mediabuy, "media_buy_id"
).display()
dim_channel.groupBy("channel_id").count().filter(col("count") > 1).join(
    dim_channel, "channel_id"
).display()
dim_site.groupBy("site_id").count().filter(col("count") > 1).join(
    dim_site, "site_id"
).display()
dim_strategy.groupBy("strategy_id").count().filter(col("count") > 1).join(
    dim_strategy, "strategy_id"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Dashboard Breaking Issues

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### A. Granularity of Source Tables

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ##### 1. Datorama Data
# MAGIC ### There shouldn't be Multiple Campaign name for One gpd_campaign_id in  (fact_perf + dim_campaign tables)

# COMMAND ----------

df_fact_performance.select('gpd_campaign_id', 'campaign_desc').dropDuplicates().groupBy('gpd_campaign_id').agg(count('campaign_desc')).filter(col('count(campaign_desc)') > 1 ).display()

dim_campaign.groupBy('gpd_campaign_id').count().filter(col("count") > 1).display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC %md
# MAGIC ###B. There shouldn't be any null values in any fact tables & dim tables

# COMMAND ----------

df_fact_performance.select(
    [count(when(isnull(c), c)).alias(c) for c in df_fact_performance.columns]
).display()

df_planned_spend.select(
    [count(when(isnull(c), c)).alias(c) for c in df_planned_spend.columns]
).display()

df_reach.select([count(when(isnull(c), c)).alias(c) for c in df_reach.columns]).display()


dim_campaign.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_campaign.columns]
).display()

dim_product.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_product.columns]
).display()


dim_mediabuy.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_mediabuy.columns]
).display()

dim_creative.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_creative.columns]
).display()


dim_site.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_site.columns]
).display()

dim_channel.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_channel.columns]
).display()





# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### B1. There should not be any duplicate rows in Table

# COMMAND ----------

print(f"{df_fact_performance.count() - df_fact_performance.dropDuplicates().count()} duplicates in df_fact_performance")

print(f"{df_planned_spend.count() - df_planned_spend.dropDuplicates().count()} duplicates in df_planned_spend")

print(f"{df_reach.count() - df_reach.dropDuplicates().count()} duplicates in df_reach")

print(f"{dim_country.count() - dim_country.dropDuplicates().count()} duplicates in dim_country")

print(f"{dim_product.count() - dim_product.dropDuplicates().count()} duplicates in dim_product")

print(f"{dim_campaign.count() - dim_campaign.dropDuplicates().count()} duplicates in dim_campaign")

print(f"{dim_channel.count() - dim_channel.dropDuplicates().count()} duplicates in dim_channel")

print(f"{dim_creative.count() - dim_creative.dropDuplicates().count()} duplicates in dim_creative")

print(f"{dim_mediabuy.count() - dim_mediabuy.dropDuplicates().count()} duplicates in dim_mediabuy")

print(f"{dim_site.count() - dim_site.dropDuplicates().count()} duplicates in dim_site")

print(f"{dim_strategy.count() - dim_strategy.dropDuplicates().count()} duplicates in dim_strategy")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### C. Data for fact_pefromace table should be available in all dim_tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC 1. fact_performance & dim_product
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_product, on ='product_id'
                         , how='left').filter(dim_product.country_id.isNull()).select(
                                                df_fact_performance.country_id,
                                                df_fact_performance.product_id,
                                                'gpd_campaign_id',
                                                'campaign_desc',
                                                'product',
                                                'brand',
                                                'campaign_start_date',
                                                'campaign_end_date'
                         ).dropDuplicates()

print("Number of missing product_id in dim_product Table:-  ", 
      df.select('product_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC 2. fact_performance & dim_channel
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_channel, on ='channel_id'
                                             , how='left').filter(col('channel_desc').isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.channel_id,
                                                                'gpd_campaign_id',
                                                                'campaign_desc',
                                                                'channel_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing Channel_id in dim_channel Table:-  ", 
      df.select('channel_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC  3. fact_performance & dim_creative
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_creative, on ='creative_id',
                              how='left').filter(dim_creative.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.creative_id,
                                                                'creative_desc',
                                                                'campaign_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing creative_id in dim_creative Table:-  ", 
      df.select('creative_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC  4. fact_performance & dim_mediabuy
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_mediabuy, on ='media_buy_id',
                              how='left').filter(dim_mediabuy.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.media_buy_id,
                                                                'media_buy_desc',
                                                                'campaign_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing mediabuy_id in dim_mediabuy Table:-  ", 
      df.select('media_buy_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC  5. fact_performance & dim_campaign
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_campaign, on ='gpd_campaign_id',
                              how='left').filter(dim_campaign.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.gpd_campaign_id,
                                                                df_fact_performance.campaign_desc,
                                                                dim_campaign.campaign_desc,
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing gpd_campaign_id in dim_campaign Table:-  ", 
      df.select('gpd_campaign_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC %md
# MAGIC  6. fact_performance & dim_site
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_site, on ='site_id',
                              how='left').filter(dim_site.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.site_id,
                                                                df_fact_performance.campaign_desc,
                                                                'site_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing site_id in dim_site Table:-  ", 
      df.select('site_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### * Data-type should be correct for all table

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC %md
# MAGIC ### 4. Countries should be consistent in all fact tables
# MAGIC --> It is expected that the data for countries should be consistent among all tables. If not, it should be raised to the source.

# COMMAND ----------

df1 = (
    df_fact_performance.select("country_id")
    .distinct()
    .withColumn("index", monotonically_increasing_id())
    .withColumnRenamed("country_id", "performance")
)
df2 = (
    df_planned_spend.select("country_id")
    .distinct()
    .withColumn("index", monotonically_increasing_id())
    .withColumnRenamed("country_id", "planned_spend")
)
df3 = (
    df_reach.select("country_id")
    .distinct()
    .withColumn("index", monotonically_increasing_id())
    .withColumnRenamed("country_id", "reach")
)
df1.join(df2, "index", "outer").join(df3, "index", "outer").drop("index").display()

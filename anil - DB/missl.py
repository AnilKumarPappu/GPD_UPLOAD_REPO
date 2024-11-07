# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

#Prod Connection
def jdbc_connection(dbtable):
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
df_fact_creativetest = jdbc_connection( "mm.vw_mw_gpd_fact_creativetest")
dim_calender = jdbc_connection('mm.vw_dim_calendar')


# COMMAND ----------

# dim_country.display()
# df_fact_performance.filter(col("country_id") == "GB").display()
# df_planned_spend.filter(col("country_id") == "GB").display()
# df_reach.filter(col("country_id") == "GB").display()
# dim_campaign.filter(col("country_id") == "GB").display()
# dim_channel.filter(col("country_id") == "GB").display()
# dim_creative.filter(col("country_id") == "GB").display()
# dim_mediabuy.filter(col("country_id") == "GB").display()
# dim_product.filter(col("country_id") == "GB").display()
# df_fact_creativetest.filter(col("country_id") == "GB").display()
dim_calender.display()


# COMMAND ----------

df_fact_performance.join(dim_creative.filter(col('naming_convention_flag') == 0).select('creative_id','creative_audience_type','creative_audience_desc'), on = 'creative_id', how = 'left').join(dim_mediabuy.filter(col('naming_convention_flag') == 0).select('media_buy_id','audience_type','audience_desc'), on = 'media_buy_id', how = 'left').filter((col('creative_audience_type') != col('audience_type')) | (col('creative_audience_desc') != col('audience_desc'))).select('gpd_campaign_id','creative_id','media_buy_id','creative_audience_type','audience_type','creative_audience_desc','audience_desc').dropDuplicates().display()

# COMMAND ----------

df_fact_performance.join(dim_creative.select('creative_id','creative_audience_type','creative_audience_desc'), on = 'creative_id', how = 'left').join(dim_mediabuy.select('media_buy_id','audience_type','audience_desc'), on = 'media_buy_id', how = 'left').groupBy('gpd_campaign_id','creative_audience_type').agg(sum('impressions')).display()

# COMMAND ----------

df_fact_performance.join(dim_creative.select('creative_id','creative_audience_type','creative_audience_desc'), on = 'creative_id', how = 'left').join(dim_mediabuy.select('media_buy_id','audience_type','audience_desc'), on = 'media_buy_id', how = 'left').groupBy('creative_audience_type','gpd_campaign_id').agg(sum('impressions')).display()

# COMMAND ----------

dim_creative.join(df_fact_performance.join(dim_channel.select('channel_id','channel_desc').dropDuplicates(), on = 'channel_id', how = 'left').filter(col('channel_desc') == 'SOCIAL').select('creative_id','channel_desc'), on = 'creative_id', how = 'left').join(df_fact_performance.select("creative_id",'product_id').dropDuplicates(),on = 'creative_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id',how = 'left').filter(col('brand') == 'Snickers').display()

# COMMAND ----------



# COMMAND ----------

dim_creative = jdbc_connection('mm.vw_mw_gpd_dim_creative')


# COMMAND ----------

dim_creative.filter((dim_creative.creative_desc.rlike("sn")) | (dim_creative.creative_desc.rlike("sn_"))).join(df_fact_performance.select("creative_id",'product_id').dropDuplicates(),on = 'creative_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id',how = 'left').display()

# COMMAND ----------

df_reach.join(dim_product.select('product_id','brand'), on = 'product_id',how = 'left').filter(col('brand') == 'kind').display()

# COMMAND ----------

df_fact_performance.join(dim_product.select('product_id','brand'), on = 'product_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').filter(col('campaign_desc').isin(['us_mms_core product_mw_na_cho_brand_olv_awareness_bid-adr_0524_ttd yt-q2 to q4-pbu-5-27 to 12-31-ma3-mms-043','us_dgic_dove ice cream_mw_na_cho_brand_olv_awareness_bidadr_ 0524_ttd yt-q2-pbu-5-1 to 8-4-ma3-na-009'])).groupBy('campaign_desc','brand','platform_desc').agg(sum('actual_spend_dollars')).display()

# COMMAND ----------

df_planned_spend.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').select('platform_desc','campaign_desc').dropDuplicates().groupBy('platform_desc').agg(count('campaign_desc')).display()

# COMMAND ----------

df_planned_spend.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').select('platform_desc','gpd_campaign_id').dropDuplicates().groupBy('platform_desc').agg(count('gpd_campaign_id')).display()

# COMMAND ----------

df_planned_spend.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').select('platform_desc','campaign_desc').dropDuplicates().display()

# COMMAND ----------

df_fact_creativetest.select('Country_id').dropDuplicates().display()

# COMMAND ----------

dim_mediabuy.display()

# COMMAND ----------

# MAGIC %md
# MAGIC count of campaign key at campaign desc and platform level

# COMMAND ----------

df_reach.join(dim_campaign.select('gpd_campaign_id','campaign_desc','gpd_campaign_code'),on = 'gpd_campaign_id', how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').select(dim_campaign.campaign_desc,'platform_desc','gpd_campaign_code').dropDuplicates().groupBy(dim_campaign.campaign_desc,'platform_desc' ).agg(count('gpd_campaign_code')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC some random check
# MAGIC

# COMMAND ----------

xlsx_file_path = "/Workspace/Users/anilkumar.pappu@effem.com/data files/Mars_Financial Detail_7.10.24.xlsx"
financial_data = pd.read_excel(xlsx_file_path, dtype=str)
financial_data = spark.createDataFrame(financial_data)

# COMMAND ----------

# financial_data.dtypes
financial_data = financial_data.withColumn("Campaign", financial_data["Campaign"].cast(StringType()))

# COMMAND ----------

data = [("John Doe", "123 Main St, Springfield"), 
        ("Jane Smith", "456 Elm St, Springfield")]
df = spark.createDataFrame(data, ["Name", "Address"])

# COMMAND ----------

split_col = split(financial_data['Campaign'], '\|')

split_df = financial_data.withColumn('Street', split_col.getItem(0)) \
             .withColumn('campaign_desc', split_col.getItem(1)) \
             .select('Street', 'campaign_desc')
split_df.display()

# COMMAND ----------

# dim_campaign.join(split_df, on = 'campaign_desc', how = 'left').display()
df_fact_performance.select('campaign_desc').drop_duplicates().join(split_df, on = 'campaign_desc', how = 'inner').drop_duplicates().display()

# COMMAND ----------

split_col2 = split(split_df['campaign_desc'], r'-ma3|-MA3')

split_df2 = split_df.withColumn('Street2', split_col2.getItem(0)) \
             .withColumn('campaign_desc', split_col2.getItem(1)) \
             .select('Street2', 'campaign_desc')
split_df2.display()

# COMMAND ----------

df_fact_performance.select('campaign_desc').drop_duplicates().join(split_df2, on = 'campaign_desc', how = 'inner').display()

# COMMAND ----------

dim_campaign.display()

# COMMAND ----------

financial_data.display()

# COMMAND ----------

df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### checking olive data in AOH AND DNA

# COMMAND ----------

df_reach.display()

# COMMAND ----------

df_planned_spend.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').select("gpd_campaign_id").distinct().display()
df_planned_spend.join(dim_product, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
df_planned_spend.join(dim_channel, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()
# df_planned_spend.join(dim_creative, on = "creative_id", how = 'left_anti').select("creative_id").distinct().display()

# COMMAND ----------

df_fact_performance.groupBy("country_id").agg(
    min("date"), max("date"), count("date")
).display()


df_planned_spend.groupBy("country_id").agg(
    min("campaign_start_date"), max("campaign_end_date"), count("country_id")
).display()

df_reach.groupBy("country_id").agg(
    min("campaign_platform_start_date"), max("campaign_platform_end_date"), count("country_id")
).display()

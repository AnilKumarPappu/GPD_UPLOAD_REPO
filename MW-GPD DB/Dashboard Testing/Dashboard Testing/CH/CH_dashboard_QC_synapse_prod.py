# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def jdbc_connection_synapse(dbtable):
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

df_reach_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_fact_reach')
df_fact_performance_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_fact_performance')
df_planned_spend_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_fact_plannedspend')
dim_campaign_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_campaign')
dim_channel_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_channel')
dim_creative_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_creative')
dim_strategy_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_strategy')
dim_mediabuy_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_mediabuy')
dim_site_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_site')
dim_country_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_country')
dim_product_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_product')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Latest date when Dashboard is Updated.

# COMMAND ----------

# Testing dashboard on 5th October

last_dashboard_updated_date = '20231220'

df_fact_performance_synapse = df_fact_performance_synapse.filter(col('date') <= last_dashboard_updated_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Regional summary (table)

# COMMAND ----------

# in campaign level total numbers
df_fact_performance_synapse.join(dim_campaign_synapse.select('gpd_campaign_id','campaign_desc_free_field'), on = 'gpd_campaign_id', how= 'inner').join(dim_creative_synapse.select('creative_id','creative_campaign_desc_free_field','dimension','creative_desc'), on = 'creative_id', how = 'inner').join(dim_channel_synapse.select('channel_id','platform_desc'), on ='channel_id', how = 'inner' ).join(dim_mediabuy_synapse.select('media_buy_id','placement_type'),on = 'media_buy_id', how = 'inner' ).groupBy('campaign_desc_free_field').agg(sum('views').alias('views'), sum('video_plays').alias('video_plays'),sum('impressions').alias('impressions'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played').alias('video_fully_played')).withColumn('VTR', col('views')/col('impressions') ).withColumn('VCR', col('video_fully_played')/col('views')).display()
DV360
AMAZON DSP
TERDX
VERIZON


# COMMAND ----------

df_fact_performance_synapse.join(dim_campaign_synapse.select('gpd_campaign_id','campaign_desc_free_field'), on = 'gpd_campaign_id', how= 'inner').join(dim_creative_synapse.select('creative_id','creative_campaign_desc_free_field','dimension','creative_desc'), on = 'creative_id', how = 'inner').join(dim_channel_synapse.select('channel_id','platform_desc'), on ='channel_id', how = 'inner' ).join(dim_mediabuy_synapse.select('media_buy_id','placement_type'),on = 'media_buy_id', how = 'inner' ).groupBy('campaign_desc_free_field','country_id').agg(sum('views').alias('views'), sum('video_plays').alias('video_plays'),sum('impressions').alias('impressions'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played').alias('video_fully_played')).withColumn('VTR', col('views')/col('impressions') ).withColumn('VCR', col('video_fully_played')/col('views')).display()
#,'platform_desc','placement_type','dimension','creative_campaign_desc_free_field','creative_desc'

# COMMAND ----------

dim_creative_synapse.display()

# COMMAND ----------

df_planned_spend_synapse.select('country_id','plan_line_id','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').filter(col('plan_start_date') >= '20230601').dropDuplicates().display()

# COMMAND ----------

# countries = [
#     'US',
#     'CA'
# ]

# skip_countries = [

# ]

# if countries:
#     df_reach = df_reach.filter(col("country_id").isin(countries))
#     df_fact_performance = df_fact_performance.filter(col("country_id").isin(countries))
#     df_planned_spend = df_planned_spend.filter(col("country_id").isin(countries))
#     dim_campaign = dim_campaign.filter(col("country_id").isin(countries))
#     dim_channel = dim_channel.filter(col("country_id").isin(countries))
#     dim_creative = dim_creative.filter(col("country_id").isin(countries))
#     dim_strategy = dim_strategy.filter(col("country_id").isin(countries))
#     dim_mediabuy = dim_mediabuy.filter(col("country_id").isin(countries))
#     dim_site = dim_site.filter(col("country_id").isin(countries))

# if skip_countries:
#     df_reach = df_reach.filter(~col("country_id").isin(countries))
#     df_fact_performance = df_fact_performance.filter(~col("country_id").isin(countries))
#     df_planned_spend = df_planned_spend.filter(~col("country_id").isin(countries))
#     dim_campaign = dim_campaign.filter(~col("country_id").isin(countries))
#     dim_channel = dim_channel.filter(~col("country_id").isin(countries))
#     dim_creative = dim_creative.filter(~col("country_id").isin(countries))
#     dim_strategy = dim_strategy.filter(~col("country_id").isin(countries))
#     dim_mediabuy = dim_mediabuy.filter(~col("country_id").isin(countries))
#     # dim_site = dim_site.filter(~col("country_id").isin(countries))

# COMMAND ----------

## Campaign level
df_fact_performance_synapse.filter((col('date')>='20231002') & (col('date')<='20231012')).filter(col('country_id')=='TW').filter(col('campaign_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_mw_apac_gnm_brand_olv_awareness_bid-adr_1023_youtube').groupBy('country_id','campaign_desc').agg(sum('impressions'),sum('views'),sum('video_fully_played')).display()

# COMMAND ----------

## creative level
# add date range
df_fact_performance_synapse.filter(col('country_id')=='TW').filter(col('campaign_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_mw_apac_gnm_brand_olv_awareness_bid-adr_1023_youtube').join(dim_creative_synapse,on= 'creative_id',how= 'inner').filter(col('creative_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_zh_dv36o_yt_bid-adr_demo-only_18-44_extra-gum_v1_video_none_15s_skippable-vtr_na_mars_comb-in-between-moments-affinity (opid-3745613)_imp').agg(sum('impressions'),sum('views'),sum('video_fully_played')).display()

# COMMAND ----------

## Campaign level
df_fact_performance_synapse.filter(col('country_id')=='AE').filter(col('campaign_desc')=='ae_gal_ramadan-campaign_youtube_mw_mea_cho_brand_olv_awareness_bid-adr_0323_#uae-mw-3').groupBy('country_id','campaign_desc').agg(sum('impressions'),sum('views'),sum('video_fully_played')).display()

# COMMAND ----------

# campaigns and the count
df_fact_performance_synapse.filter((col('date')<='20231007')& (col('date')>= '20230110')).filter(col('country_id')=='EG').select('campaign_desc').dropDuplicates().display()

# COMMAND ----------

# creative count
df_fact_performance_synapse.filter(col('country_id')=='TW').filter(col('campaign_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_mw_apac_gnm_brand_olv_awareness_bid-adr_1023_youtube').join(dim_creative_synapse,on= 'creative_id',how= 'inner').select('creative_desc').dropDuplicates().display()

# COMMAND ----------

dim_country_synapse.display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='TW').display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


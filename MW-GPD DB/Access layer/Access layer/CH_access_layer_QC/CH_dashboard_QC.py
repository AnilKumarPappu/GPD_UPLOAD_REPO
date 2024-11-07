# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def jdbc_connection(dbtable):
    url = "jdbc:sqlserver://marsanalyticsprodsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationprodsqldb"
    user_name = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databricksusername"
    )
    password = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databrickspassword"
    )
    df = (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user_name)
        .option("password", password)
        .option("authentication", "ActiveDirectoryPassword")
        .load()
    )
    return df

# COMMAND ----------

df_reach = jdbc_connection('mm.vw_gpd_fact_reach')
df_fact_performance = jdbc_connection('mm.vw_gpd_fact_performance')
df_planned_spend = jdbc_connection('mm.vw_gpd_fact_plannedspend')
dim_campaign = jdbc_connection('mm.vw_gpd_dim_campaign')
dim_channel = jdbc_connection('mm.vw_gpd_dim_channel')
dim_creative = jdbc_connection('mm.vw_gpd_dim_creative')
dim_strategy = jdbc_connection('mm.vw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection('mm.vw_gpd_dim_mediabuy')
dim_site = jdbc_connection('mm.vw_gpd_dim_site')
dim_country = jdbc_connection('mm.vw_gpd_dim_country')
dim_product = jdbc_connection('mm.vw_gpd_dim_product')

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

df_fact_performance.filter(col('country_id')=='CO').filter(col('campaign_desc')=='co_mms_diamadres-p5_mw_latam_con_brand_display_multiple_bid-dda_0423_126262').groupBy('country_id','campaign_desc').agg(sum('impressions'),sum('views')).display()

# COMMAND ----------

## Campaign level
df_fact_performance.filter((col('date')>='20231002') & (col('date')<='20231012')).filter(col('country_id')=='TW').filter(col('campaign_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_mw_apac_gnm_brand_olv_awareness_bid-adr_1023_youtube').groupBy('country_id','campaign_desc').agg(sum('impressions'),sum('views'),sum('video_fully_played')).display()

# COMMAND ----------

## creative level
df_fact_performance_synapse.filter(col('country_id')=='TW').filter(col('campaign_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_mw_apac_gnm_brand_olv_awareness_bid-adr_1023_youtube').join(dim_creative_synapse,on= 'creative_id',how= 'inner').filter(col('creative_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_zh_dv36o_yt_bid-adr_demo-only_18-44_extra-gum_v1_video_none_15s_skippable-vtr_na_mars_comb-in-between-moments-affinity (opid-3745613)_imp').agg(sum('impressions'),sum('views'),sum('video_fully_played')).display()

# COMMAND ----------

## Campaign level
df_fact_performance.filter(col('country_id')=='DE').filter(col('campaign_desc')=='de_sni_otg-social-p5-and-p6_mw_eur_cho_brand_social_awareness_bid-nadr_0523_#127304').groupBy('country_id','campaign_desc').agg(sum('impressions'),sum('views'),sum('video_fully_played')).display()

# COMMAND ----------

# creative count
# add date range
df_fact_performance_synapse.filter(col('country_id')=='TW').filter(col('campaign_desc')=='tw_exg_tw-mw-30-extrap11campaign-yt-oct2023_mw_apac_gnm_brand_olv_awareness_bid-adr_1023_youtube').join(dim_creative_synapse,on= 'creative_id',how= 'inner').select('creative_desc').dropDuplicates().display()

# COMMAND ----------

# campaigns and the count
df_fact_performance_synapse.filter((col('date')<='20231007')& (col('date')>= '20230110')).filter(col('country_id')=='EG').select('campaign_desc').dropDuplicates().display()

# COMMAND ----------



# COMMAND ----------


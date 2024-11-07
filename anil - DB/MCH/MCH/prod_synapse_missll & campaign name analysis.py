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
    # user_name = "anilkumar.pappu@effem.com"
    # password = "Tiger@mustang"
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

# df_reach_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_fact_reach')
df_fact_performance_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_fact_performance')
df_planned_spend_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_fact_plannedspend')
# dim_campaign_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_campaign')
# dim_channel_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_channel')
# dim_creative_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_creative')
# dim_strategy_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_strategy')
# dim_mediabuy_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_mediabuy')
# dim_site_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_site')
# dim_country_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_country')
# dim_product_synapse = jdbc_connection_synapse('mm.vw_mw_gpd_dim_product')
# dim_calendar_synapse = jdbc_connection_synapse('mm.vw_mw_dim_calendar')

# COMMAND ----------

dim_campaign_synapse.select('campaign_name').display()

# COMMAND ----------

# dim_product_synapse.select('brand').distinct().display()
df_fact_performance_synapse.display()

# COMMAND ----------

dim_campaign_synapse.display()

# COMMAND ----------

df_fact_performance_synapse.select('creative_id','product_id').join(dim_creative_synapse,on = 'creative_id').join(dim_product_synapse.select('product_id','brand'), on = 'product_id').filter(col('brand').isin(["M&M's","Orbit",'Snickers'])).filter(col('country_id')== 'US').filter(col('naming_convention_flag')==0).withColumn("creative_id_dco", split(col("creative_desc"), "_")).withColumn("last_element", col("creative_id_dco").getItem(18)).select('last_element').distinct().display()

# COMMAND ----------


dim_creative_synapse.filter(col('country_id')== 'US').filter(col('naming_convention_flag')==0).withColumn("creative_id_dco", split(col("creative_desc"), "_")).withColumn("last_element", size(col("creative_id_dco"))).display()
# .withColumn("last_element", col("creative_id_dco").getItem(18)).display()
# .withColumn("last_element", size(col("creative_id_dco"))).display()

# COMMAND ----------

df_fact_performance_synapse.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## new campaign analysis

# COMMAND ----------

new_df = dim_campaign_synapse.select(concat_ws('_',dim_campaign_synapse.campaign_market,dim_campaign_synapse.campaign_subproduct,dim_campaign_synapse.campaign_desc_free_field, dim_campaign_synapse.segment,dim_campaign_synapse.region,dim_campaign_synapse.portfolio,dim_campaign_synapse.business_channel,dim_campaign_synapse.media_channel)
              .alias("new_campaign"), "country_id","gpd_campaign_id","gpd_campaign_code","campaign_desc","campaign_name","campaign_market","campaign_subproduct","campaign_desc_free_field","segment","region","portfolio","business_channel","media_channel")

new_df2 = new_df.filter(~col("campaign_market").isin(["Default Campaign"]) )


# we are including all campaigns in dashboard even they are with NA values, these below filter are just for analysis
# new_df3 = new_df2.filter(col("campaign_desc_free_field")!= "NA")
# new_df3.groupby("new_campaign").count().display()

# .withColumn("new_campaign", col("campaign_market")+col("campaign_subproduct")+col("campaign_desc_free_field")+col("segment")+col("region")+col("portfolio")+col("business_channel")+col("media_channel")).display()

# COMMAND ----------

campaing_name_check_df = dim_campaign_synapse.select(concat_ws('_',dim_campaign_synapse.campaign_market,dim_campaign_synapse.campaign_subproduct,dim_campaign_synapse.campaign_desc_free_field, dim_campaign_synapse.segment,dim_campaign_synapse.region,dim_campaign_synapse.portfolio,dim_campaign_synapse.business_channel,dim_campaign_synapse.media_channel)
              .alias("new_campaign"), "country_id","gpd_campaign_id","gpd_campaign_code","campaign_desc","campaign_name","campaign_market","campaign_subproduct","campaign_desc_free_field","segment","region","portfolio","business_channel","media_channel")

# COMMAND ----------

campaing_name_check_df.filter(col('new_campaign') != col('campaign_name')).display()

# COMMAND ----------

# Null values check in all columns
# dim_campaign_synapse.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dim_campaign_synapse.columns]
#    ).display()

new_df.select([count(when(
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in new_df.columns]).display()

# COMMAND ----------



# COMMAND ----------

new_df.display()
# at 2116 row the values are not correct,
# that is because of naming convention issue, this we show in dashboard but we mark as not campatible in performance summary page

# COMMAND ----------

new_df.select("campaign_market")

# COMMAND ----------



# COMMAND ----------

##################################################

# COMMAND ----------

# dim_campaign_synapse.groupBy("campaign_desc").count().display()
dim_campaign_synapse.filter(col("campaign_desc")== "uk_mms_p8-crispy_mw_eur_cho_brand_social_awareness_bid-nadr_0723_133545").display()

# COMMAND ----------

dim_campaign_synapse.groupBy("campaign_desc").count().display()


# COMMAND ----------

dim_campaign_synapse.filter(col("campaign_desc_free_field") == "brand-p5").display()

# COMMAND ----------

df_fact_performance_synapse.select('gpd_campaign_id','media_buy_id').filter(col('gpd_campaign_id').isin('GB_29338839','GB_53484662','GB_23853894994270396','GB_53087063','GB_d3e0e28e-4a3f-47d0-9b75-ca87d99128c3')).join(dim_campaign_synapse.select('gpd_campaign_id','campaign_desc','campaign_desc_free_field'), on = 'gpd_campaign_id',how = 'inner').join(dim_mediabuy_synapse.select('media_buy_id','media_buy_desc','mediabuy_campaign_desc_free_field'), on = 'media_buy_id', how = 'inner').select('campaign_desc_free_field','mediabuy_campaign_desc_free_field').dropDuplicates().display()

# COMMAND ----------

df_fact_performance_synapse.withColumn("year", expr("substr(date, 1, 4)")).join(dim_channel_synapse.select('channel_id','platform_desc'),on= 'channel_id',how= 'inner').groupBy('country_id','year','platform_desc').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

df_fact_performance_synapse.withColumn("year", expr("substr(date, 1, 4)")).filter((col('country_id')=='US') & (col('year') == '2023')).join(dim_channel_synapse.select('channel_id','platform_desc'),on= 'channel_id',how= 'inner').groupBy('gpd_campaign_id','campaign_desc','platform_desc').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

df_fact_performance_synapse.withColumn("year", expr("substr(date, 1, 4)")).filter((col('country_id')=='US') & (col('year') == '2023')).join(dim_channel_synapse.select('channel_id','platform_desc'),on= 'channel_id',how= 'inner').groupBy('gpd_campaign_id','campaign_desc','platform_desc').agg(sum('clicks'),sum('impressions'),sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

prod_ih_spend = jdbc_connection_synapse('mm.vw_gpd_fact_ih_spend')
prod_ih_ingoing = jdbc_connection_synapse('mm.vw_gpd_fact_ih_ingoingspend')
prod_dim_country = jdbc_connection_synapse('mm.vw_gpd_dim_country')
prod_dim_product = jdbc_connection_synapse('mm.vw_gpd_dim_product')
prod_dim_calendar = jdbc_connection_synapse('mm.vw_dim_calendar')  ## check the connection name

# COMMAND ----------

dim_channel_synapse.display()

# COMMAND ----------

prod_ih_spend.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### video in search channel
# MAGIC

# COMMAND ----------

df_fact_performance_synapse.join(dim_country_synapse.filter(col('gpd_region_desc').isin('EU/CEABT','GEM')),on = 'country_id',how = 'inner').join(dim_channel_synapse.filter(col('channel_desc')=='Search').select('channel_id','channel_desc','platform_desc'),on = 'channel_id',how= 'inner').groupBy('country_id','gpd_campaign_id','campaign_start_date','campaign_end_date').agg(sum('clicks'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played'),sum('views')).display()

# COMMAND ----------

df_fact_performance_synapse.join(dim_country_synapse.filter(col('gpd_region_desc').isin('EU/CEABT','GEM')),on = 'country_id',how = 'inner').join(dim_channel_synapse.select('channel_id','channel_desc','platform_desc'),on = 'channel_id',how= 'inner').groupBy('country_id','gpd_campaign_id','campaign_start_date','campaign_end_date','channel_desc').agg(sum('clicks'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played'),sum('views')).filter(col('sum(video_fully_played)')==0).display()

# COMMAND ----------

df_fact_performance_synapse.join(dim_country_synapse.filter(col('gpd_region_desc').isin('EU/CEABT','GEM')),on = 'country_id',how = 'inner').join(dim_channel_synapse.select('channel_id','channel_desc','platform_desc'),on = 'channel_id',how= 'inner').groupBy('country_id','gpd_campaign_id','campaign_start_date','campaign_end_date','channel_desc').agg(sum('clicks'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played'),sum('views')).filter((col('sum(video_fully_played)')!=0 ) & (col('sum(video_completion_25)')!=0) &  ( col('sum(video_completion_50)')!=0 ) & (col('sum(video_completion_75)')!=0)).display()

# COMMAND ----------

dim_country_synapse.display()


# COMMAND ----------

dim_channel_synapse.select('channel_desc').dropDuplicates().display()
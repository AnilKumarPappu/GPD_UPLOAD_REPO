# Databricks notebook source
# MAGIC %md 
# MAGIC #### Loading Prod data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
#import adal

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

# PROD Data tables
df_planned_spend = jdbc_connection_synapse('mm.vw_mw_gpd_fact_plannedspend')
df_fact_reach = jdbc_connection_synapse('mm.vw_mw_gpd_fact_reach')
df_dim_campaign = jdbc_connection_synapse('mm.vw_mw_gpd_dim_campaign')
df_dim_channel = jdbc_connection_synapse('mm.vw_mw_gpd_dim_channel')
df_dim_creative = jdbc_connection_synapse('mm.vw_mw_gpd_dim_creative')
df_dim_strategy = jdbc_connection_synapse('mm.vw_mw_gpd_dim_strategy')
df_dim_site = jdbc_connection_synapse('mm.vw_mw_gpd_dim_site')
df_dim_mediabuy = jdbc_connection_synapse('mm.vw_mw_gpd_dim_mediabuy')
df_dim_product = jdbc_connection_synapse('mm.vw_mw_gpd_dim_product')
df_dim_country = jdbc_connection_synapse('mm.vw_mw_gpd_dim_country')
df_fact_performance = jdbc_connection_synapse('mm.vw_mw_gpd_fact_performance')


# COMMAND ----------

# df_planned_spend.count()
# df_fact_reach.count()
# df_dim_campaign.count()
# df_dim_channel.count()
# df_dim_creative.count()
# df_dim_strategy.count()
# df_dim_site.count()
# df_dim_mediabuy.count()
# df_dim_product.count()
# df_dim_country.count()
# df_fact_performance.count()


# COMMAND ----------


df_dim_channel = df_dim_channel.filter(col('platform_desc') != 'Campaign Manager')
df_fact_performance = df_fact_performance.filter(col('campaign_manager_flag') == 0)



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Latest date when Dashboard is Updated.

# COMMAND ----------

# Testing dashboard on 5th October

last_dashboard_updated_date = '20231220'

df_fact_performance = df_fact_performance.filter(col('date') <= last_dashboard_updated_date)

# COMMAND ----------

from pyspark.sql.functions import substring
df_fact_performance = df_fact_performance.withColumn('year', substring(df_fact_performance['date'], 1, 4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter applied on dashboard, need to ask powerbi team if there are any
# MAGIC

# COMMAND ----------

df_fact_performance = df_fact_performance.join(df_dim_product.select('product_id','brand'),on = 'product_id', how= 'inner').filter(~col('channel_id').isin(['Chocolate Shopper','Wrigley Shopper','PK'])).drop('brand').filter(~col('channel_id').isin(['US_6','DATORAMA_US_MW_10','DATORAMA_US_MW_8']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### multi-region view
# MAGIC

# COMMAND ----------

df_fact_performance.select('country_id','product_id','gpd_campaign_id','actual_spend_dollars','impressions','clicks','views','video_fully_played').join(df_dim_country.select('country_id','gpd_region_desc'), on = 'country_id',how = 'inner').join(df_dim_product.select('product_id','portfolio','brand'),on = 'product_id', how = 'inner').join(df_dim_campaign.select('gpd_campaign_id','campaign_desc_free_field'), on = 'gpd_campaign_id', how= 'inner').groupBy('campaign_desc_free_field','gpd_region_desc','country_id','portfolio','brand').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('impressions').alias('impressions'),sum('clicks').alias('clicks'),sum('views').alias('views'),sum('video_fully_played').alias('video_fully_played')).withColumn('CTR', 100*col('clicks')/col('impressions')).withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('video_fully_played')).display()

# COMMAND ----------

# at campaign level
df_fact_performance.select('country_id','product_id','gpd_campaign_id','actual_spend_dollars','impressions','clicks','views','video_fully_played').join(df_dim_country.select('country_id','gpd_region_desc'), on = 'country_id',how = 'inner').join(df_dim_product.select('product_id','portfolio','brand'),on = 'product_id', how = 'inner').join(df_dim_campaign.select('gpd_campaign_id','campaign_desc_free_field'), on = 'gpd_campaign_id', how= 'inner').groupBy('campaign_desc_free_field').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('impressions').alias('impressions'),sum('clicks').alias('clicks'),sum('views').alias('views'),sum('video_fully_played').alias('video_fully_played')).withColumn('CTR', 100*col('clicks')/col('impressions')).withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('video_fully_played')).display()
# 'gpd_region_desc','country_id','portfolio','brand')

# COMMAND ----------

# MAGIC %md
# MAGIC ### performance summary
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####### campaign performance

# COMMAND ----------

df_fact_performance.join(df_dim_channel.select('channel_id','channel_desc','platform_desc'), on ='channel_id', how = 'inner' ).join(df_dim_campaign.select('gpd_campaign_id','campaign_desc_free_field','campaign_type','naming_convention_flag'), on = 'gpd_campaign_id', how= 'inner').join(df_dim_strategy.select('strategy_id','objective'), on = 'strategy_id',how = 'inner').groupBy('campaign_desc_free_field','channel_desc','platform_desc','objective','campaign_type','naming_convention_flag').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('impressions').alias('impressions'),sum('video_fully_played').alias('video_fully_played'),sum('views').alias('views'),sum('clicks').alias('clicks')).withColumn('CTR', 100*col('clicks')/col('impressions')).withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).withColumn('VCR', 100*col('video_fully_played')/col('views')).withColumn('VTR', 100*col('views')/col('impressions')).withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('video_fully_played')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######### campaign reach

# COMMAND ----------

df_fact_performance.join(df_fact_reach.select('gpd_campaign_id','frequency','in_platform_reach'), on = 'gpd_campaign_id', how = 'inner').join(df_dim_channel.select('channel_id','channel_desc','platform_desc'), on ='channel_id', how = 'inner' ).join(df_dim_campaign.select('gpd_campaign_id','campaign_desc_free_field','naming_convention_flag'), on = 'gpd_campaign_id', how= 'inner').join(df_dim_strategy.select('strategy_id','objective'), on = 'strategy_id',how = 'inner').groupBy('campaign_desc_free_field','channel_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','campaign_desc','naming_convention_flag','objective').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('impressions').alias('impressions'),avg('in_platform_reach').alias('reach'),avg('frequency').alias('frequency'),sum('video_fully_played').alias('video_fully_played'),sum('views').alias('views'),sum('clicks').alias('clicks')).withColumn('CTR', 100*col('clicks')/col('impressions')).withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).withColumn('VCR', 100*col('video_fully_played')/col('views')).withColumn('VTR', 100*col('views')/col('impressions')).withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('video_fully_played')).display()

# COMMAND ----------

df_fact_performance = df_fact_performance.filter(col('country_id')=='US')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Campaign Summary Screeen(exclude)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Without any filter (Overall Dashboard View)

# COMMAND ----------

df_fact_performance.count()

# COMMAND ----------

df_fact_performance.select('country_id',
                           'gpd_campaign_id',
                           'campaign_desc',
                           'date',
                           'year',
                           'impressions',
                           'clicks',
                           'views',
                           'actual_spend_local_currency',
                           'actual_spend_dollars',
                           'video_fully_played',
                           'video_plays'
                           ).dropDuplicates().groupBy('country_id', 'year').agg(
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency'),
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('impressions').alias('impressions'),
                                                            sum('clicks').alias('clicks'),
                                                            sum('views').alias('views'),
                                                            sum('video_fully_played').alias('complete views'),

                                                            ).\
                           withColumn('CTR', 100*col('clicks')/col('impressions')).\
                           withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).\
                           withColumn('CPM_Local', 1000*col('actual_spend_local_currency')/col('impressions')).\
                           withColumn('VCR', 100*col('complete views')/col('impressions')).\
                           withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('complete views')).\
                           withColumn('CPCV_Local', col('actual_spend_local_currency')/col('complete views')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###### On Brand Level

# COMMAND ----------

df_fact_performance.join(df_dim_product, on = 'product_id', how='left').select(
                            df_fact_performance.country_id,
                           'gpd_campaign_id',
                           'campaign_desc',
                           'brand',
                           'date',
                           'year',
                           'impressions',
                           'clicks',
                           'views',
                           'actual_spend_local_currency',
                           'actual_spend_dollars',
                           'video_fully_played',
                           'video_plays'
                           ).dropDuplicates().groupBy('country_id', 'year', 'brand').agg(
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency'),
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('impressions').alias('impressions'),
                                                            sum('clicks').alias('clicks'),
                                                            sum('views').alias('views'),
                                                            sum('video_fully_played').alias('complete views'),

                                                            ).\
                           withColumn('CTR', 100*col('clicks')/col('impressions')).\
                           withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).\
                           withColumn('CPM_Local', 1000*col('actual_spend_local_currency')/col('impressions')).\
                           withColumn('VCR', 100*col('complete views')/col('impressions')).\
                           withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('complete views')).\
                           withColumn('CPCV_Local', col('actual_spend_local_currency')/col('complete views')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### On Channel Level & Platform

# COMMAND ----------

df_fact_performance.join(df_dim_channel, on = 'channel_id', how='left').select(
                            df_fact_performance.country_id,
                           'gpd_campaign_id',
                           'campaign_desc',
                           'channel_desc',
                           'platform_desc',
                           'date',
                           'year',
                           'impressions',
                           'clicks',
                           'views',
                           'actual_spend_local_currency',
                           'actual_spend_dollars',
                           'video_fully_played',
                           'video_plays'
                           ).dropDuplicates().groupBy('country_id', 'year', 'channel_desc', 'platform_desc').agg(
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency'),
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('impressions').alias('impressions'),
                                                            sum('clicks').alias('clicks'),
                                                            sum('views').alias('views'),
                                                            sum('video_fully_played').alias('complete views'),

                                                            ).\
                           withColumn('CTR', 100*col('clicks')/col('impressions')).\
                           withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).\
                           withColumn('CPM_Local', 1000*col('actual_spend_local_currency')/col('impressions')).\
                           withColumn('VCR', 100*col('complete views')/col('impressions')).\
                           withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('complete views')).\
                           withColumn('CPCV_Local', col('actual_spend_local_currency')/col('complete views')).display()


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Campaign Details Screeen(exclude)

# COMMAND ----------

campaign_name = 'AE_Extra_GYDB3_P8_0722'

# COMMAND ----------


df_fact_performance.filter(col('campaign_desc')==campaign_name).join(df_dim_channel, on = 'channel_id', how='left').select(
                            df_fact_performance.country_id,
                           'gpd_campaign_id',
                           'campaign_desc',
                           'channel_desc',
                           'platform_desc',
                           'date',
                           'year',
                           'impressions',
                           'clicks',
                           'views',
                           'actual_spend_local_currency',
                           'actual_spend_dollars',
                           'video_fully_played',
                           'video_plays'
                           ).dropDuplicates().groupBy('country_id', 'year', 'channel_desc', 'platform_desc', 'campaign_desc').agg(
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency'),
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('impressions').alias('impressions'),
                                                            sum('clicks').alias('clicks'),
                                                            sum('views').alias('views'),
                                                            sum('video_fully_played').alias('complete views'),

                                                            ).\
                           withColumn('CTR', 100*col('clicks')/col('impressions')).\
                           withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).\
                           withColumn('CPM_Local', 1000*col('actual_spend_local_currency')/col('impressions')).\
                           withColumn('VCR', 100*col('complete views')/col('impressions')).\
                           withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('complete views')).\
                           withColumn('CPCV_Local', col('actual_spend_local_currency')/col('complete views')).display()


# COMMAND ----------

df_planned_spend.filter(col('campaign_desc') == campaign_name).select(
                                                        'country_id','plan_line_id','campaign_desc',
                                                        'planned_budget_local_currency',
                                                        'planned_budget_dollars').dropDuplicates().groupBy('campaign_desc').\
                                                        agg(sum('planned_budget_local_currency').alias('planned_budget_local_currency'),
                                                            sum('planned_budget_dollars').alias('planned_budget_dollars')
                                                            
                                                            ).display()

# COMMAND ----------

df_fact_reach.filter(col('campaign_desc') == campaign_name).select(
                                                        'country_id','campaign_desc',
                                                        'in_platform_reach',
                                                        'frequency').dropDuplicates().groupBy('campaign_desc').\
                                                        agg(sum('in_platform_reach').alias('in_platform_reach'),
                                                            sum('frequency').alias('frequency')
                                                            
                                                            ).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Campaign Comparison Screeen

# COMMAND ----------

campaign_name_1 = 'uae-mw-16-p6'
campaign_name_2 = 'ramadan-campaign'
campaign_names = [campaign_name_1, campaign_name_2]

# COMMAND ----------


df_fact_performance.join(df_dim_campaign.select('gpd_campaign_id','campaign_desc_free_field'), on = 'gpd_campaign_id', how= 'inner').filter(col('campaign_desc_free_field').isin(campaign_names)).join(df_dim_channel, on = 'channel_id', how='left').select(
                                                        df_fact_performance.country_id,
                                                        'gpd_campaign_id',
                                                        'campaign_desc',
                                                        'channel_desc',
                                                        'platform_desc',
                                                        'date',
                                                        'year',
                                                        'impressions',
                                                        'clicks',
                                                        'views',
                                                        'actual_spend_local_currency',
                                                        'actual_spend_dollars',
                                                        'video_fully_played',
                                                        'video_plays'
                           ).dropDuplicates().groupBy('country_id', 'year', 'channel_desc', 'platform_desc', 'campaign_desc').agg(
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency'),
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('impressions').alias('impressions'),
                                                            sum('clicks').alias('clicks'),
                                                            sum('views').alias('views'),
                                                            sum('video_fully_played').alias('complete views'),

                                                            ).\
                           withColumn('CTR', 100*col('clicks')/col('impressions')).\
                           withColumn('CPM_Dollar', 1000*col('actual_spend_dollars')/col('impressions')).\
                           withColumn('CPM_Local', 1000*col('actual_spend_local_currency')/col('impressions')).\
                           withColumn('VCR', 100*col('complete views')/col('impressions')).\
                           withColumn('CPCV_Dollar', col('actual_spend_dollars')/col('complete views')).\
                           withColumn('CPCV_Local', col('actual_spend_local_currency')/col('complete views')).display()


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Spend Summary Screeen

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Without any filter (Overall Dashboard View)

# COMMAND ----------

from pyspark.sql import functions as F

df_fact_performance.groupBy('country_id', 'year').\
                                            agg(
                                            F.sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                            F.sum('actual_spend_local_currency').alias('actual_spend_local_currency')
                                        ).join(df_dim_country.select('country_id','gpd_region_desc','country_desc'),on = 'country_id',how = 'inner').display()


# COMMAND ----------



# COMMAND ----------


df_planned_spend.select('country_id', 'plan_line_id', 'planned_budget_dollars', 'planned_budget_local_currency').\
                                        dropDuplicates().\
                                        groupBy('country_id', ).\
                                            agg(
                                            F.sum('planned_budget_dollars').alias('planned_budget_dollars'),
                                            F.sum('planned_budget_local_currency').alias('planned_budget_local_currency')
                                        ).display()
                                 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### On Channel Level

# COMMAND ----------

df_fact_performance.join(df_dim_channel, on='channel_id', how='left').select('gpd_campaign_id', df_fact_performance.country_id, 'channel_desc', 'date', 'year', 'actual_spend_dollars', 'actual_spend_local_currency').dropDuplicates().groupBy('country_id', 'channel_desc', 'year').agg(
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency')
                                                                                                            ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### On Platform Level

# COMMAND ----------

df_fact_performance.join(df_dim_channel, on='channel_id', how='left').select('gpd_campaign_id', df_fact_performance.country_id, 'platform_desc', 'date','year', 'actual_spend_dollars', 'actual_spend_local_currency').dropDuplicates().groupBy('country_id', 'platform_desc', 'year').agg(
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency')
                                                                                                            ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### On Brand Level

# COMMAND ----------

df_fact_performance.join(df_dim_product, on='product_id', how='left').select('gpd_campaign_id', df_fact_performance.country_id, 'brand', 'date','year', 'actual_spend_dollars', 'actual_spend_local_currency').dropDuplicates().groupBy('country_id', 'brand', 'year').agg(
                                                            sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                                            sum('actual_spend_local_currency').alias('actual_spend_local_currency')
                                                                                                            ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### High level checks
# MAGIC

# COMMAND ----------

# country
df_fact_performance_synapse.join(dim_country_synapse,on = 'country_id',how = 'inner').filter(col('gpd_region_desc') == 'EU/CEABT').groupBy('country_id').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

# country and year
df_fact_performance_synapse.join(dim_country_synapse,on = 'country_id',how = 'inner').filter(col('gpd_region_desc') == 'EU/CEABT').groupBy('country_id','year').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

# country and brand
df_fact_performance_synapse.join(dim_product_synapse.select('product_id','brand'), on= 'product_id',how= 'inner').withColumn("year", expr("substr(date, 1, 4)")).join(dim_country_synapse,on = 'country_id',how = 'inner').filter(col('gpd_region_desc') == 'EU/CEABT').groupBy('country_id','brand').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------


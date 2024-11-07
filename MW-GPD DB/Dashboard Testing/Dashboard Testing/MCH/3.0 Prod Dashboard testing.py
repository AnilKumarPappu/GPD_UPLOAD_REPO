# Databricks notebook source
# MAGIC %md 
# MAGIC #### Loading 3.0 Prod data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
#import adal

# COMMAND ----------

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

# COMMAND ----------

# PROD Data tables
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

df_reach.display()

# COMMAND ----------

print(df_reach.count())
print(df_fact_performance.count())
print(df_planned_spend.count())
print(dim_campaign.count())
print(dim_channel.count())
print(dim_creative.count())
print(dim_strategy.count())
print(dim_mediabuy.count())
print(dim_site.count())
print(dim_country.count())
print(dim_product.count())


 
 
 
 
 
 
 

# COMMAND ----------

df_fact_performance.groupBy("country_id").agg(
    min("date"), max("date"), count("date")
).display()

# COMMAND ----------


df_dim_channel = df_dim_channel.filter(col('platform_desc') != 'Campaign Manager')
df_fact_performance = df_fact_performance.filter(col('campaign_manager_flag') == 0)



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Latest date when Dashboard is Updated.

# COMMAND ----------

# Testing dashboard on 5th October

last_dashboard_updated_date = '20231019'

df_fact_performance = df_fact_performance.filter(col('date') <= last_dashboard_updated_date)

# COMMAND ----------

from pyspark.sql.functions import substring
df_fact_performance = df_fact_performance.withColumn('year', substring(df_fact_performance['date'], 1, 4))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Campaign Summary Screeen

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Without any filter (Overall Dashboard View)

# COMMAND ----------

df_fact_performance = df_fact_performance.filter(col('country_id')=='US')

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
# MAGIC #### Campaign Details Screeen

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

campaign_name_1 = 'ke_jft_p2_mw_mea_cho_brand_social_awareness_dir-nadr_0223_104392'
campaign_name_2 = 'KE_Juicy Fruit_Unexplainably Juicy P13_1222'
campaign_names = [campaign_name_1, campaign_name_2]

# COMMAND ----------


df_fact_performance.filter(col('campaign_desc').isin(campaign_names)).join(df_dim_channel, on = 'channel_id', how='left').select(
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

df_fact_performance.select('country_id', 'gpd_campaign_id', 'date', 'year', 'actual_spend_dollars', 'actual_spend_local_currency').\
                                        dropDuplicates().\
                                        groupBy('country_id', 'year').\
                                            agg(
                                            F.sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                            F.sum('actual_spend_local_currency').alias('actual_spend_local_currency')
                                        ).display()
                                 

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


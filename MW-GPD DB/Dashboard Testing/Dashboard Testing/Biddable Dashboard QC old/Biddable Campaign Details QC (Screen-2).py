# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Importing tables
# MAGIC

# COMMAND ----------

# prod
def jdbc_connection_prod(dbtable):
    url = 'jdbc:sqlserver://marsanalyticsprodsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationprodsqldb'
    user_name =dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databricksusername')
    password = dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databrickspassword')
    df = spark.read \
    .format('com.microsoft.sqlserver.jdbc.spark') \
    .option('url',url) \
    .option('dbtable',dbtable) \
    .option('user',user_name) \
    .option('password',password) \
    .option('authentication','ActiveDirectoryPassword') \
    .load()
    return df

# COMMAND ----------

# prod df
df_planned_spend = jdbc_connection_prod('mm.vw_gmd_fact_plannedspend')
df_performance = jdbc_connection_prod('mm.vw_gmd_fact_performance')
df_dim_country = jdbc_connection_prod('mm.vw_gmd_dim_country')
df_dim_product = jdbc_connection_prod('mm.vw_gmd_dim_product')
df_dim_calendar = jdbc_connection_prod('mm.vw_dim_calendar')
df_reach = jdbc_connection_prod('mm.vw_gmd_fact_reach')
df_dim_campaign = jdbc_connection_prod('mm.vw_gmd_dim_campaign')

# COMMAND ----------

df_planned_spend = df_planned_spend.filter(col('platform') != 'Campaign Manager')
df_performance = df_performance.filter(col('platform') != 'Campaign Manager')

# COMMAND ----------


df_performance.filter(col('country_id')=='MX').select('country_id', 'campaign_name').dropDuplicates().display()

# COMMAND ----------

camp_name = 'MX_Conejos_Momentos P13_1222'
#camp_name = 'PL_Snickers_Core_0522'
#camp_name = 'eg_gal_eg-galaxy-p3_mw_mea_cho_brand_social_awareness_bid-adr_0223'
# camp_name = 'FR_M&Ms_VNP P2_0122'
# camp_name = 'de_mms_p4-pom-2.0_mw_eur_cho_brand_olv_views_bid-nadr_0323_essen'
# camp_name = 'ke_jft_p1_mw_mea_gum_brand_social_awareness_bid-nadr_1123_110025'
# camp_name =  'FR_Snickers_IC_SOCIAL Amp EL'
# camp_name = 'KE_Juicy Fruit_P13_1222'
# camp_name = 'NL_Maltesers_Courtrooms/ Moments_0422'
# camp_name = 'pl_twx_p1-white-january_mw_eur_cho_brand_olv_awareness_bid-nadr_0123'
# camp_name = 'SA_BEKIND_World Kindness Day_1022'
# camp_name = 'SA_Dove / Galaxy_Desserts of Arabia P11_1022'
# camp_name = 'ae_sni_p3-tekken&zombie_mw_mea_cho_brand_social_awareness_bid-nadr_0223_23737'
# camp_name = 'ae_exg_p1-gydb-3.0_mw_mea_gum_brand_olv_awareness_bid-dda_0123_olv'
# camp_name = 'PL_Snickers_Core_0522'
# camp_name = 'DE_Extra Gum_Shine Bright Global Assets_0322'

# COMMAND ----------


df_performance.filter(col('campaign_name') == camp_name).groupBy('country_id','channel','platform','objective','campaign_name')\
                                                            .agg(sum('clicks').alias('clicks'),\
                                                                  sum('Impressions').alias('impressions'),\
                                                                  sum('views').alias('views'),\
                                                                  sum('actual_spend_local_currency').alias('actual_spend_local_currency'),\
                                                                  sum('actual_spend_dollars').alias('actual_spend_dollars'),\
                                                                  sum('video_fully_played').alias('video_fully_played'))\
                                                            .withColumn('VTR', col('views')/col('impressions') )\
                                                                 .withColumn('CTR', col('clicks')/col('impressions'))\
                                                                 .withColumn('CPM', (col('actual_spend_dollars')/col('impressions'))*1000)\
                                                                 .withColumn('CPCV', col('actual_spend_local_currency')/col('video_fully_played'))\
                                                                 .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

# Campaign level VCR


df_performance.filter(col('campaign_name') == camp_name).groupBy('country_id','campaign_name')\
                                                            .agg(sum('clicks').alias('clicks'),\
                                                                  sum('Impressions').alias('impressions'),\
                                                                  sum('views').alias('views'),\
                                                                  sum('actual_spend_local_currency').alias('actual_spend_local_currency'),\
                                                                  sum('actual_spend_dollars').alias('actual_spend_dollars'),\
                                                                  sum('video_fully_played').alias('video_fully_played'))\
                                                            .withColumn('VTR', col('views')/col('impressions') )\
                                                                 .withColumn('CTR', col('clicks')/col('impressions'))\
                                                                 .withColumn('CPM', (col('actual_spend_local_currency')/col('impressions'))*1000)\
                                                                 .withColumn('CPCV', col('actual_spend_local_currency')/col('video_fully_played'))\
                                                                 .withColumn('VCR', col('video_fully_played')/col('impressions')).display()

# COMMAND ----------

df_reach.filter(col('campaign_name') == camp_name).display()

# COMMAND ----------

df_planned_spend.filter(col('campaign_name') == camp_name).select('plan_line_id','planned_budget_dollars','planned_budget_local_currency' ).dropDuplicates().agg(sum('planned_budget_dollars').alias('Planned spend $'), sum('planned_budget_local_currency').alias('Planed Spend LC')).display()

# COMMAND ----------

camp_name = 'PL_Snickers_Core_0522'

# COMMAND ----------



# COMMAND ----------

df_planned_spend.filter(col('campaign_name') == camp_name).select('plan_line_id','planned_budget_dollars','planned_budget_local_currency' ).display()

# COMMAND ----------

table_1 = 'mm_test.vw_gmd_dim_country'
table_2 = 'mm_test.vw_gmd_dim_product'
table_3 = 'mm_test.vw_dim_calendar'

df_country = spark.read \
.format('com.microsoft.sqlserver.jdbc.spark') \
.option('url',url) \
.option('dbtable',table_1) \
.option('user',user_name) \
.option('password',password) \
.option('authentication','ActiveDirectoryPassword') \
.load()

df_product = spark.read \
.format('com.microsoft.sqlserver.jdbc.spark') \
.option('url',url) \
.option('dbtable',table_2) \
.option('user',user_name) \
.option('password',password) \
.option('authentication','ActiveDirectoryPassword') \
.load()

df_calendar = spark.read \
.format('com.microsoft.sqlserver.jdbc.spark') \
.option('url',url) \
.option('dbtable',table_3) \
.option('user',user_name) \
.option('password',password) \
.option('authentication','ActiveDirectoryPassword') \
.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Tables

# COMMAND ----------

df_calendar.display()

# COMMAND ----------

df_performance_product = df_performance.join(df_product, 'product_id', how = 'left').drop('format','media_buy_type','strategy_key','strategy_name','target_audience','delivery_audience').dropDuplicates()

# COMMAND ----------

df_performance_product_country = df_performance_product.join(df_country.select('country_id','countrydesc','marketregioncode'),'country_id', how = 'left').dropDuplicates()

# COMMAND ----------

df_performance_product_country_reach = df_performance_product_country.join(df_reach.select('campaign_id','in_platform_reach','frequency'),'campaign_id', how = 'left').dropDuplicates()

# COMMAND ----------

df_performance_product_country_reach_plannedspend = df_performance_product_country_reach.join(df_plannedspend.select('plan_line_id','campaign_id','planned_budget_dollars'),'campaign_id', how = 'left').dropDuplicates()

# COMMAND ----------

df_final = df_performance_product_country_reach_plannedspend.alias('a').join(df_calendar.select('calendar_date','cal_week_of_year').alias('b'),col('a.campaign_platform_start_date') == col('b.calendar_date'))

# COMMAND ----------

# MAGIC %md ## Analyzing Merged Data
# MAGIC

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_performance.count(), df_plannedspend.count(), df_reach.count(), df_country.count(), df_product.count(), df_calendar.count()

# COMMAND ----------

#Previous counts: (837484, 837475, 837475, 837475, 6215075, 6215075)

df_performance.count(), df_performance_product.count(), df_performance_product_country.count(), df_performance_product_country_reach.count(), df_performance_product_country_reach_plannedspend.count(),df_final.count()

# COMMAND ----------

#df_final.select('marketregioncode','countrydesc','cell','brand','channel','platform','objective','campaign_name','campaign_platform_start_date','campaign_platform_end_date','cal_week_of_year','actual_spend_local_currency','actual_spend_dollars','planned_budget_dollars','Impressions','views','clicks','in_platform_reach','frequency').dropDuplicates().display()
df_final.select('campaign_name').distinct().display()

# COMMAND ----------

# MAGIC %md ## Creating Custom Tables for Use Cases
# MAGIC

# COMMAND ----------

case_one = df_final.select('marketregioncode','countrydesc','brand','campaign_name','actual_spend_dollars','planned_budget_dollars','Impressions','views','video_fully_played').groupby('marketregioncode','countrydesc','brand','campaign_name','planned_budget_dollars').sum('actual_spend_dollars','Impressions','views','video_fully_played')

# COMMAND ----------

case_one = case_one.withColumnRenamed('sum(video_fully_played)', 'video_fully_played')
case_one = case_one.withColumnRenamed('sum(Impressions)', 'Impressions')
case_one = case_one.withColumnRenamed('sum(actual_spend_dollars)','actual_spend_dollars')
case_one = case_one.withColumnRenamed('sum(views)','views')
#case_one = case_one.withColumnRenamed('sum(planned_spend_dollars)','planned_spend_dollars')
case_one = case_one.withColumn('VTR',(case_one.video_fully_played/case_one.Impressions) * 100)
case_one = case_one.withColumn('VCR',(case_one.video_fully_played/case_one.views) * 100)
case_one = case_one.withColumn('Spend_Pacing',(case_one.actual_spend_dollars/case_one.planned_budget_dollars)*100)
case_one.display()


# COMMAND ----------

case_two = df_final.select('campaign_name','campaign_platform_start_date','campaign_platform_end_date', 'channel','platform','objective', 'actual_spend_dollars','Impressions','clicks','in_platform_reach','frequency','views','video_fully_played').groupby('campaign_name','campaign_platform_start_date','campaign_platform_end_date','channel','platform','objective').sum('actual_spend_dollars','Impressions','clicks','in_platform_reach','frequency','views','video_fully_played')

# COMMAND ----------

case_two = case_two.withColumnRenamed('sum(video_fully_played)', 'video_fully_played')
case_two = case_two.withColumnRenamed('sum(Impressions)', 'Impressions')
case_two = case_two.withColumnRenamed('sum(actual_spend_dollars)','actual_spend_dollars')
case_two = case_two.withColumnRenamed('sum(in_platform_reach)','reach')
case_two = case_two.withColumnRenamed('sum(frequency)','frequency')
case_two = case_two.withColumnRenamed('sum(views)','views')
case_two = case_two.withColumnRenamed('sum(clicks)','clicks')
case_two = case_two.withColumn('CPM',(case_two.actual_spend_dollars/case_two.Impressions) * 1000)
case_two = case_two.withColumn('CTR',case_two.clicks/case_two.Impressions * 100)
case_two = case_two.withColumn('VTR',(case_two.video_fully_played/case_two.Impressions) * 100)
case_two.display()

# COMMAND ----------

windowval = (Window.partitionBy('campaign_name').orderBy('date')
             .rangeBetween(Window.unboundedPreceding, 0))
case_three = df_final.select('campaign_name','date','planned_budget_dollars','actual_spend_dollars').dropDuplicates().withColumn('cum_actual_spend', F.sum('actual_spend_dollars').over(windowval))
case_three = case_three.select('campaign_name','date','planned_budget_dollars','cum_actual_spend').dropDuplicates()
case_three.display()

# COMMAND ----------

# Campaign-level Actual Spend, Planned Spend and Spend Pacing
#temp = df_final.select('marketregioncode','countrydesc','cell','brand','campaign_name','campaign_platform_start_date','campaign_platform_end_date','actual_spend_local_currency','actual_spend_dollars','planned_budget_dollars').groupby('marketregioncode','countrydesc','cell','brand','campaign_name').sum('actual_spend_dollars','planned_budget_dollars')

# COMMAND ----------

# MAGIC %md ## Testing Dashboard Data
# MAGIC

# COMMAND ----------

#Case 1:
case_one.where(col('campaign_name') == 'GB_Extra Gum_Style Up P3_0322').display()
case_two.where(col('campaign_name') == 'GB_Extra Gum_Style Up P3_0322').display()
case_three.where(col('campaign_name') == 'GB_Extra Gum_Style Up P3_0322').display()

# COMMAND ----------

#Case 2:
case_one.where(col('campaign_name') == 'NL_Maltesers_Courtrooms/ Moments_0422').display()
case_two.where(col('campaign_name') == 'NL_Maltesers_Courtrooms/ Moments_0422').display()
case_three.where(col('campaign_name') == 'NL_Maltesers_Courtrooms/ Moments_0422').display()

# COMMAND ----------

#Case 3:
case_one.where(col('campaign_name') == 'GB_Skittles_Pride_0621').display()
case_two.where(col('campaign_name') == 'GB_Skittles_Pride_0621').display()
case_three.where(col('campaign_name') == 'GB_Skittles_Pride_0621').display()

# COMMAND ----------


case_one.where(col('campaign_name') == 'gb_mal_p1-lotls-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0123_na').display()
case_two.where(col('campaign_name') == 'gb_mal_p1-lotls-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0123_na').display()
case_three.where(col('campaign_name') == 'gb_mal_p1-lotls-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0123_na').display()

# COMMAND ----------

df_plannedspend.select('campaign_name','plan_line_id').dropDuplicates().groupby('campaign_name').count().display()

# COMMAND ----------

df_plannedspend.filter(col('campaign_name') == 'GB_M&Ms_VNP P11_1022').display()

# COMMAND ----------

df_performance.select('campaign_name','clicks','Impressions','views','actual_spend_dollars').filter(col('campaign_name') == 'GB_M&Ms_VNP P11_1022').groupby('campaign_name').sum('clicks','Impressions','views','actual_spend_dollars').display()

# COMMAND ----------

df_reach.filter(col('campaign_name') == 'GB_M&Ms_VNP P11_1022').display()

# COMMAND ----------

df_performance.select('country_id').distinct().display()

# COMMAND ----------


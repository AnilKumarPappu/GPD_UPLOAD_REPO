# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.column import *

# COMMAND ----------

# prod
def jdbc_connection(dbtable):
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

df_planned_spend = jdbc_connection('mm.vw_gmd_fact_plannedspend')
df_fact_performance = jdbc_connection('mm.vw_gmd_fact_performance')
df_dim_country = jdbc_connection('mm.vw_gmd_dim_country')
df_dim_product = jdbc_connection('mm.vw_gmd_dim_product')
df_dim_calendar = jdbc_connection('mm.vw_dim_calendar')
df_reach = jdbc_connection('mm.vw_gmd_fact_reach')
df_dim_campaign = jdbc_connection('mm.vw_gmd_dim_campaign')

# COMMAND ----------

# df_planned_spend = df_planned_spend.filter(col('platform') != 'Campaign Manager')
df_fact_performance = df_fact_performance.filter(col('platform') != 'Campaign Manager')

# COMMAND ----------

df_fact_performance = df_fact_performance.filter(col('country_id').isin(['MX']))

# COMMAND ----------

# Covers screen 2
df_fact_performance.groupBy('country_id').agg(sum('actual_spend_local_currency').alias('actual_spends_local_currency'),
                                              sum('actual_spend_dollars').alias('actual_spends_dollars'),\
                                              sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('views').alias('views'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends_local_currency')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends_local_currency')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

df_planned_spend.select('country_id', 'plan_line_id',  'planned_budget_dollars',
 'planned_budget_local_currency').dropDuplicates().groupBy('country_id').agg(sum('planned_budget_dollars').alias('total_planned_budget_dollars'), sum('planned_budget_local_currency').alias('total_planned_budget_local_currency')).display()

# COMMAND ----------

# Covers screen 2
df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
df_fact_performance2.groupBy('country_id','brand').agg(sum('actual_spend_local_currency').alias('actual_spends_local_currency'),
                                              sum('actual_spend_dollars').alias('actual_spends_dollars'),\
                                              sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('views').alias('views'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends_local_currency')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends_local_currency')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

# Calculate all metrics at Country x Brand level
df_planned_spend.join(df_dim_product, 'product_id').select('country_id','portfolio','brand', 'plan_line_id','planned_budget_dollars','planned_budget_local_currency').dropDuplicates().groupBy('country_id','brand').agg(sum('planned_budget_dollars').alias('planned_budget_dollars'), sum('planned_budget_local_currency').alias('planned_budget_local_currency')).display()

# COMMAND ----------

# .filter((col("country_id")=='GB') & (col("brand")=="M&M's") & (col("platform")=="Dv360-YouTube"))
df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
df_fact_performance2\
                                         .groupBy('country_id','brand','platform')\
                                         .agg(sum('actual_spend_local_currency').alias('actual_spends_local_currency'),\
                                              sum('actual_spend_dollars').alias('actual_spends_dollars'),\
                                              sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('views').alias('views'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends_local_currency')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends_local_currency')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

# .filter((col("country_id")=='GB') & (col("brand")=="M&M's") & (col("platform")=="Dv360-YouTube"))
df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
df_fact_performance2\
                                         .groupBy('country_id','brand')\
                                         .agg(sum('actual_spend_local_currency').alias('actual_spends_local_currency'),\
                                              sum('actual_spend_dollars').alias('actual_spends_dollars'),\
                                              sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('views').alias('views'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends_local_currency')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends_local_currency')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

country_id = 'MX'

# COMMAND ----------

df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')

df_fact_performance2.filter(col('country_id')==country_id).groupBy('country_id','platform').agg(sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('views').alias('views'),
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('actual_spend_local_currency').alias('actual_spends'),
                                              sum('actual_spend_dollars').alias('actual_spend_dollars'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

df_fact_performance2.filter(col('country_id')==country_id).groupBy('country_id','channel').agg(sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('views').alias('views'),
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('actual_spend_local_currency').alias('actual_spends'),
                                              sum('actual_spend_dollars').alias('actual_spend_dollars'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

df_fact_performance2.filter(col('country_id')==country_id).groupBy('country_id','brand').agg(sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('views').alias('views'),
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('actual_spend_local_currency').alias('actual_spends'),
                                              sum('actual_spend_dollars').alias('actual_spend_dollars'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

#Check 3 
df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
df_check1 = df_fact_performance2.filter((col("country_id")==country_id)).groupBy('country_id','campaign_name')\
                                                            .agg(sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('views').alias('views'),
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('actual_spend_dollars').alias('actual_spend_dollars'),
                                              sum('actual_spend_local_currency').alias('actual_spends'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views'))
df_check1.display() 

# COMMAND ----------

arr=df_check1
arr.agg(sum(arr.impressions).alias('Total_impressions'), sum(arr.views).alias('Total_Views'), sum(arr.clicks).alias('Total_clicks'),
       sum(arr.actual_spends).alias('Total_actual_spends'), sum(arr.actual_spend_dollars).alias('Total_actual_$$') ,sum(arr.video_fully_played).alias('Total_full_views')).display()
                                              

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functionality Test

# COMMAND ----------

df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
df_fact_performance2.filter(col('country_id')=="NL").groupBy('country_id','brand','campaign_name')\
                                                            .agg(sum('Impressions').alias('impressions'),\
                                              sum('clicks').alias('clicks'),\
                                              sum('views').alias('views'),
                                              sum('video_fully_played').alias('video_fully_played'),
                                              sum('actual_spend_local_currency').alias('actual_spends'))\
                                         .withColumn('VTR', col('video_fully_played')/col('impressions') )\
                                              .withColumn('CTR', (col('clicks')/col('impressions'))*100)\
                                              .withColumn('CPM', (col('actual_spends')/col('impressions'))*1000)\
                                              .withColumn('CPCV', col('actual_spends')/col('video_fully_played'))\
                                              .withColumn('VCR', col('video_fully_played')/col('views')).display()

# COMMAND ----------

df_actual_spend2 = df_fact_performance.join(df_dim_product, on = 'product_id', how = 'left').drop('product_id')
df_actual_spend3 = df_actual_spend2.fillna(0, subset='actual_spend_local_currency')
df_actual_spend3 = df_actual_spend3.filter(col('platform') != 'Campaign Manager')

# COMMAND ----------

df_actual_spend4 = df_actual_spend3.groupBy('country_id','brand','channel','platform','campaign_name')\
                    .agg(sum('actual_spend_local_currency').alias('actual_spend_local_currency'),\
                        sum('actual_spend_dollars').alias('actual_spend_dollars'))

# COMMAND ----------

df_actual_spend4.filter(col('country_id')=='sa').display()

# COMMAND ----------

df_actual_spend5 = df_actual_spend4.join(df_planned_spend, on=['country_id','channel','platform','campaign_name'], how = 'left')\
                                         .select('country_id','brand','channel','platform','campaign_name','plan_line_id','actual_spend_local_currency',\
                                                 'planned_budget_local_currency','actual_spend_dollars','planned_budget_dollars').dropDuplicates()\
                                         .groupBy('country_id','brand','channel','platform','campaign_name')\
                                         .agg(round(max('actual_spend_local_currency'),2).alias('actual_spend_local_currency'),\
                                                 round(sum('planned_budget_local_currency'),2).alias('planned_budget_local_currency'),\
                                                 round(max('actual_spend_dollars'),2).alias('actual_spend_dollars'),\
                                                 round(sum('planned_budget_dollars'),2).alias('planned_budget_dollars'))

# COMMAND ----------

df_actual_spend5.filter(col('actual_spend_local_currency')==col('planned_budget_local_currency')).display()

# COMMAND ----------

df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
df_fact_performance2.filter(col('campaign_name')=='DE_M&Ms_Social Hub P9 dark post_0922').groupBy('country_id','campaign_name')\
                                              .agg(sum('actual_spend_local_currency').alias('actual_spend_local_currency'),\
                                                  sum('actual_spend_dollars').alias('actual_spend_dollars')).display()
                                         

# COMMAND ----------

df_planned_spend.filter(col('campaign_name')=='DE_M&Ms_Social Hub P9 dark post_0922').groupBy('country_id','plan_line_id')\
                                              .agg(sum('planned_budget_local_currency').alias('planned_budget_local_currency'),\
                                                  sum('planned_budget_dollars').alias('planned_budget_dollars')).display()

# COMMAND ----------

df_planned_spend2 = df_planned_spend.join(df_dim_product,  'product_id','left')
df_planned_spend2.select('plan_line_id','brand','channel','platform','campaign_name','planned_budget_local_currency').display()

# COMMAND ----------

df_planned_spend.select('plan_line_id','planned_budget_local_currency').dropDuplicates().groupBy('plan_line_id').agg(count('planned_budget_local_currency')).display()

# COMMAND ----------

df_planned_spend.filter(col('plan_line_id')=='182745').select('plan_line_id','campaign_name','planned_budget_dollars').display()

# COMMAND ----------


# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

from pyspark.sql.functions import col, date_format

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
# dim_cal = jdbc_connection('mm.vw_dim_calendar')             

# COMMAND ----------

df_planned_spend.filter(col('country_id')== 'PL').join(dim_product.select('product_id','brand'), on= 'product_id', how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').display()

# COMMAND ----------

act_df = df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year').isin(['2023','2024'])).filter(col('country_id').isin(['AE','CL','CR','EC','NZ','PE','PH','PL','SA'])).join(dim_product.select('product_id','brand'), on= 'product_id', how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').groupBy('country_id','year','gpd_campaign_id','campaign_desc','campaign_start_date','campaign_end_date','brand','platform_desc').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('actual_spend_local_currency').alias('actual_spend_local_currency'))

# planned_df = df_planned_spend.filter(col('country_id').isin(['AE','CL','CR','EC','NZ','PE','PH','PL','SA'])).join(dim_product.select('product_id','brand'), on= 'product_id', how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left')
act_df.join(df_planned_spend.select('gpd_campaign_id','planned_budget_dollars','planned_budget_local_currency'), on = ['gpd_campaign_id'], how = 'left').display()

# COMMAND ----------

# dim_product.select('brand').drop_duplicates().display()
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

df.select('product_id').dropDuplicates().display()

# COMMAND ----------

df_fact_performance.join(dim_product, on = 'product_id',how = 'left').filter(col('brand').isin(['PK','Mars Inc','Tasty Bite'])).select('product_id').distinct().display()

# COMMAND ----------

fact_creativeX = jdbc_connection('mm.vw_mw_gpd_fact_creativetest')
fact_creativeX.select('country_id').distinct().join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').display()

# COMMAND ----------

# dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
df_fact_performance.select('country_id').distinct().join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').display()

# COMMAND ----------

ih_spend = jdbc_connection('mm.vw_mw_gpd_fact_ih_spend')
ih_ingoing = jdbc_connection('mm.vw_mw_gpd_fact_ih_ingoingspend')
# dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')
# dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_calendar =jdbc_connection('mm.vw_dim_calendar') 
# df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')

dim_product_som = jdbc_connection('mm.vw_mw_gpd_dim_product_som') 


# COMMAND ----------

dim_calendar.display()

# COMMAND ----------

ih_spend.display()

# COMMAND ----------

dim_product.dis

# COMMAND ----------

# dim_creative = dim_creative.filter(col("VendorID") == "DATORAMA")
# dim_channel = dim_channel.filter(col("VendorID") == "DATORAMA")
# dim_campaign = dim_campaign.filter(col("VendorID") == "DATORAMA")
# dim_product = dim_product.filter(col("VendorID") == "DATORAMA")
# dim_strategy = dim_strategy.filter(col("VendorID") == "CREATIVEX")
# dim_mediabuy = dim_mediabuy.filter(col("VendorID") == "CREATIVEX")
# dim_site = dim_site.filter(col("VendorID") == "CREATIVEX")

# COMMAND ----------


IH_brand = ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('country_id') == 'US').filter(col('year').isin(['2023'])).filter(col('channel_mix') == 'Digital').join(dim_product.select('product_id','brand'), on = "product_id", how = 'left').groupBy('country_id','year','brand').agg( sum('cost_per_day_usd').alias('IH_actual_spend'))
# .groupBy('country_id','year','plan_desc_new','channel_mix','start_date','end_date').agg(sum('cost_per_day_lc').alias('cost_per_day_lc') , sum('cost_per_day_usd').alias('cost_per_day_usd')).display()
# 
# .groupBy('country_id','budget_year','mars_period').agg(sum('cost_per_day_usd')).display()
# .filter(col('channel_mix') == 'Digital')


MCH_brand = df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).withColumn("date_as_string", date_format(col("date"), 'yyyyMMdd')).filter(col('country_id') == "US").filter(col('year').isin(['2023'])).join(dim_product.select('product_id','brand'), on = "product_id", how = 'left').groupBy('country_id', 'year', 'brand').agg(sum('actual_spend_dollars').alias('MCH_actual_spend'))

IH_brand.join(MCH_brand, on = ['country_id','year','brand'], how = 'outer').display()
# .groupBy('country_id', 'year', 'mars_period').agg(sum('actual_spend_dollars')).display()

# COMMAND ----------

IH_brand = ih_spend.withColumn("year", expr("substr(day, 1, 4)")).withColumn("date_as_string", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == 'US').filter(col('year').isin(['2023'])).filter(col('channel_mix') == 'Digital').join(dim_product.select('product_id','brand'), on = "product_id", how = 'left').groupBy('country_id','year','brand').agg( sum('cost_per_day_usd').alias('cost_per_day_usd'))
# sum('cost_per_day_lc').alias('cost_per_day_lc') ,


MCH_brand = df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).withColumn("date_as_string", date_format(col("date"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == "IN").filter(col('year').isin(['2023'])).join(dim_product.select('product_id','brand'), on = "product_id", how = 'left').groupBy('country_id', 'year', 'brand').agg(sum('actual_spend_dollars'))

IH_brand.join(MCH_brand, on = 'brand', how = 'outer').display()


# COMMAND ----------

dim_product.display()

# COMMAND ----------

ih_spend.withColumn("date_as_string", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == 'CL').filter(col('budget_year').isin(['2023'])).filter(col('channel_mix') == 'Digital').groupBy('mars_period').agg(sum('cost_per_day_usd')).display()

# COMMAND ----------

# ih_spend.filter(col('plan_desc_new') == '2023_PR_M&Ms_Hispanic OOH').display()
ih_spend.withColumn("date_as_string", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == 'PR').filter(col('budget_year').isin(['2023'])).filter(col('channel_mix') == 'Digital').select('mars_period','plan_desc_new').groupBy('mars_period').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### MCH

# COMMAND ----------

df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).withColumn("date_as_string", date_format(col("date"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == "VN").filter(col('year').isin(['2023'])).groupBy('country_id', 'year', 'mars_period').agg(sum('actual_spend_dollars')).display()

# COMMAND ----------

# df_fact_performance.join(dim_campaign, on = 'gpd_campaign_id', how = 'left').filter(col('campaign_name') == 'co_sni_navidad-p12_mw_latam_con_brand_social').display()



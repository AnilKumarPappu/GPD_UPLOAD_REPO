# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def jdbc_connection(dbtable):
    url = "jdbc:sqlserver://xsegglobalmktgdevsynapse-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
    user_name = dbutils.secrets.get(
        scope="globalxsegsrmdatafoundationdevsecretscope", key="globalxsegsrmdatafoundationdevsynapseuserid"
    )
    password = dbutils.secrets.get(
        scope="globalxsegsrmdatafoundationdevsecretscope", key="globalxsegsrmdatafoundationdevsynapsepass"
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




# COMMAND ----------

# #Dev Tables
df_reach = jdbc_connection('mm_test.vw_mw_gpd_fact_reach')
df_fact_performance = jdbc_connection('mm_test.vw_mw_gpd_fact_performance')
df_planned_spend = jdbc_connection('mm_test.vw_mw_gpd_fact_plannedspend')
dim_campaign = jdbc_connection('mm_test.vw_mw_gpd_dim_campaign')
dim_channel = jdbc_connection('mm_test.vw_mw_gpd_dim_channel')
dim_creative = jdbc_connection('mm_test.vw_mw_gpd_dim_creative')
dim_strategy = jdbc_connection('mm_test.vw_mw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection('mm_test.vw_mw_gpd_dim_mediabuy')
dim_site = jdbc_connection('mm_test.vw_mw_gpd_dim_site')
dim_country = jdbc_connection('mm_test.vw_mw_gpd_dim_country')
dim_product = jdbc_connection('mm_test.vw_mw_gpd_dim_product')
dim_calender = jdbc_connection('mm_test.vw_dim_calendar')
ih_spend = jdbc_connection( "mm_test.vw_mw_gpd_fact_ih_spend")
ih_ingoing =jdbc_connection("mm_test.vw_mw_gpd_fact_ih_ingoingspend")
df_fact_creativetest = jdbc_connection( "mm_test.vw_mw_gpd_fact_creativetest")
df_fact_som = jdbc_connection( "mm_test.vw_mw_gpd_fact_som")


# COMMAND ----------

dim_country.display()

# COMMAND ----------

df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily reach QC

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Daily Date column values do not fall under campaign start & end dates.

# COMMAND ----------

df_reach.filter(~((col('date')>= col('campaign_platform_start_date'))   & (col('date')<= col('campaign_platform_end_date'))     )).display()



# COMMAND ----------

# MAGIC %md
# MAGIC The number of records at CampaignId X platform level are lower than number of days the campaign ran referring to the campaign start&end date-------- for the campaigns which run after 15th of July 2024

# COMMAND ----------

from pyspark.sql import functions as F
days_campaign_run = df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('date')>= '2024-07-15').withColumn('days_campaign_run', F.datediff(F.col("campaign_platform_end_date"), F.col("campaign_platform_start_date") + 3)).select('campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','days_campaign_run')

rowsForDate =  df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('date')>= '2024-07-15').groupBy('campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date').agg(count('date'))

days_campaign_run.join(rowsForDate , on = ['campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date'], how = 'left').filter(col('days_campaign_run') != col('count(date)')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC In-platform reach is not coming as a cumulative increase at the CampaignId X platform level.

# COMMAND ----------

reach_15 = df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('date')>= '2024-07-15')


from pyspark.sql import Window
import pyspark.sql.functions as F

# Assuming you have a DataFrame named df with columns: col1, col2, col3, and date

# Create a window partitioned by col1 and col2, ordered by date
window_spec = Window.partitionBy('campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date').orderBy('date')

# Add a lagged column to get the previous value of col3 within the window
df_with_lag = reach_15.withColumn('prev_in_platform_reach', F.lag('in_platform_reach').over(window_spec))

# Add a column that checks if the current col3 is greater than or equal to the previous col3
df_with_check = df_with_lag.withColumn(
    'is_increasing', 
    F.when(
        F.col('prev_in_platform_reach').isNull(), 
        True  # If prev_col3 is null, it's the first row, so treat as valid
    ).otherwise(
        F.col('in_platform_reach') >= F.col('prev_in_platform_reach')
    )
)

# Filter the rows where col3 is not increasing
non_increasing_rows = df_with_check.filter(F.col('is_increasing') == False)

# Show the result
df_with_check.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_reach.join(dim_campaign, on = 'gpd_campaign_id', how = 'left').join(dim_channel, on = 'channel_id',how = 'left').filter(col('campaign_advertiser_code').isin(['971401413',
'847103741',
'847044639',
'971442944',
'847123566',
'1093818701',
'971445536',
'846955423',
'847039345',
'978019360',
'978157332',
'971442155',
'846956872'])).select('campaign_advertiser_code','platform_desc').display()

# COMMAND ----------

df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').display()

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

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list("globalxsegsrmdatafoundationdevsecretscope")

# COMMAND ----------

dbutils.fs.ls('abfss://output@marsanalyticsdevadls01.dfs.core.windows.net/GLOBAL_XSEG_SRM_DATA_FOUNDATION/TEST/DATA_ACCESS/MW_GPD')

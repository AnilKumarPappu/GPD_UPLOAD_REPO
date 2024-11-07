# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np

# COMMAND ----------

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

df_reach = jdbc_connection_prod('mm.vw_gpd_fact_reach')
df_fact_performance = jdbc_connection_prod('mm.vw_gpd_fact_performance')
df_planned_spend = jdbc_connection_prod('mm.vw_gpd_fact_plannedspend')
dim_campaign = jdbc_connection_prod('mm.vw_gpd_dim_campaign')
dim_channel = jdbc_connection_prod('mm.vw_gpd_dim_channel')
dim_creative = jdbc_connection_prod('mm.vw_gpd_dim_creative')
dim_strategy = jdbc_connection_prod('mm.vw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection_prod('mm.vw_gpd_dim_mediabuy')
dim_site = jdbc_connection_prod('mm.vw_gpd_dim_site')
dim_country = jdbc_connection_prod('mm.vw_gmd_dim_country')
dim_product = jdbc_connection_prod('mm.vw_gmd_dim_product')

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='EG').filter(col('date').isin('20230618',
'20230617',
'20230616',
'20230615',
'20230614',
'20230613',
'20230612',
'20230611',
'20230610',
'20230609',
'20230608',
'20230607',
'20230606',
'20230605',
'20230604',
'20230603')).display()

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='SA').filter(col('date').isin('20220604',
'20220605',
'20220606',
'20220607',
'20220608',
'20220609',
'20220610',
'20220611',
'20220612',
'20220613',
'20220614',
'20220615',
'20220616',
'20220617',
'20220618',
'20220619',
'20220620',
'20220621',
'20220622',
'20220623',
'20220624',
'20220625',
'20220626',
'20220627',
'20220628',
'20220629',
'20220630',
'20220701',
'20220702',
'20220703',
'20220704',
'20220705',
'20220706',
'20220707',
'20220708',
'20220709',
'20220710',
'20220711',
'20220712',
'20220713',
'20220714',
'20220715',
'20220716',
'20220717',
'20220814',
'20220911',
'20221009',
'20221106',
'20230101',
'20230102',
'20230130',
'20230131',
'20230201',
'20230202',
'20230203',
'20230204',
'20230205',
'20230226',
'20230618',
'20230910',
'20230911',
'20230912',
'20230913'
)).display()

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='US').filter(col('date').isin('20230624',
'20230917',
'20230319',
'20230321',
'20230427',
'20230323',
'20230428',
'20230325',
'20230423',
'20230920',
'20230918',
'20230519',
'20230921',
'20230515',
'20230424',
'20230922',
'20230320',
'20230324',
'20230618',
'20230520',
'20230426',
'20230514',
'20230621',
'20230623',
'20230919',
'20230517',
'20230923',
'20230425',
'20230429',
'20230516',
'20230619',
'20230622',
'20230322',
'20230620',
'20230518')).display()

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='EG').filter(col('date').isin('20231002',
'20231001',
'20230930',
'20230929',
'20230928',
'20230927',
'20230913',
'20230912',
'20230911',
'20230910',
'20230909',
'20230908',
'20230907',
'20230906',
'20230813',
'20230812',
'20230811',
'20230810',
'20230809',
'20230808',
'20230807',
'20230806',
'20230805',
'20230804',
'20230803',
'20230802',
'20230801',
'20230731',
'20230730',
'20230729',
'20230728',
'20230727',
'20230726',
'20230725',
'20230724',
'20230723',
'20230722',
'20230721',
'20230720',
'20230719',
'20230718',
'20230717',
'20230716',
'20230618',
'20230617',
'20230616',
'20230615',
'20230614',
'20230613',
'20230612',
'20230611',
'20230610',
'20230609',
'20230608',
'20230607',
'20230606',
'20230605',
'20230604',
'20230603',
'20230602',
'20230601',
'20230531',
'20230530',
'20230529',
'20230528',
'20230527',
'20230526',
'20230525',
'20230524',
'20230523',
'20230522',
'20230521',
'20230520',
'20230519',
'20230518',
'20230517',
'20230516',
'20230515',
'20230514',
'20230513',
'20230512',
'20230511',
'20230510',
'20230509',
'20230508',
'20230507',
'20230506',
'20230505',
'20230504',
'20230503',
'20230502',
'20230501',
'20230430',
'20230429',
'20230428',
'20230427',
'20230426',
'20230425',
'20230424',
'20230423',
'20230226',
'20230208',
'20230207',
'20230206',
'20230205',
'20230204',
'20230203',
'20230202',
'20230201',
'20230131',
'20230130',
'20230129',
'20230128',
'20230127',
'20230126',
'20230125',
'20230124',
'20230109',
'20230108',
'20230107',
'20230106',
'20230105',
'20230104',
'20230103',
'20230102',
'20230101',
'20221204',
'20221113',
'20221112',
'20221111',
'20221110',
'20221109',
'20221108',
'20221107',
'20221106',
'20221009',
'20221008',
'20221007',
'20220911',
'20220910',
'20220909',
'20220908',
'20220907',
'20220906',
'20220905',
'20220904',
'20220903',
'20220902',
'20220901',
'20220831',
'20220830',
'20220829',
'20220828',
'20220827',
'20220826',
'20220825',
'20220824',
'20220823',
'20220822',
'20220821',
'20220820',
'20220819',
'20220818',
'20220817',
'20220816',
'20220815',
'20220814',
'20220813')).display()

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='CA').filter(col('date').isin('20230108')).display()

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='AE').filter(col('date').isin('20221106',
'20230203',
'20230101',
'20230201',
'20230910',
'20230131',
'20230205',
'20230913',
'20230912',
'20230226',
'20221204',
'20230204',
'20220911',
'20230130',
'20221009',
'20230911',
'20230202',
'20220814',
'20230102')).display()

# COMMAND ----------



# COMMAND ----------

prod_dim_calendar = jdbc_connection_prod('mm.vw_dim_calendar') 

# COMMAND ----------

df_fact_performance.filter(col('country_id')=='SA').groupBy('country_id').agg(min('date'),max('date')).display()

# COMMAND ----------

# SA
mini= '20220531'
maxi = '20231008'
prod_dim_calendar.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance.filter(col('country_id')=='SA').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar.calendar_id == df_fact_performance.date)).display()


# COMMAND ----------

# US
mini= '20230101'
maxi = '20231010'
prod_dim_calendar.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance.filter(col('country_id')=='US').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar.calendar_id == df_fact_performance.date)).display()


# COMMAND ----------

# EG
mini= '20220718'
maxi = '20231007'
prod_dim_calendar.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance.filter(col('country_id')=='EG').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar.calendar_id == df_fact_performance.date)).display()


# COMMAND ----------

# CA
mini= '20230101'
maxi = '20231010'
prod_dim_calendar.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance.filter(col('country_id')=='CA').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar.calendar_id == df_fact_performance.date)).display()


# COMMAND ----------

# AE
mini= '20220718'
maxi = '20231007'
prod_dim_calendar.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance.filter(col('country_id')=='AE').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar.calendar_id == df_fact_performance.date)).display()


# COMMAND ----------

df_fact_performance.filter(col('country_id').isin('CA','EG','AE','US').groupBy('country_id').agg(min('date'),max('date')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### synapse
# MAGIC

# COMMAND ----------

def jdbc_connection_synapse(dbtable):
    url = "jdbc:sqlserver://globalxsegsrmdatafoundationprodsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
    user_name = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databricksusername"
    )
    password = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databrickspassword"
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

df_reach_synapse = jdbc_connection_synapse('mm.vw_gpd_fact_reach')
df_fact_performance_synapse = jdbc_connection_synapse('mm.vw_gpd_fact_performance')
df_planned_spend_synapse = jdbc_connection_synapse('mm.vw_gpd_fact_plannedspend')
dim_campaign_synapse = jdbc_connection_synapse('mm.vw_gpd_dim_campaign')
dim_channel_synapse = jdbc_connection_synapse('mm.vw_gpd_dim_channel')
dim_creative_synapse = jdbc_connection_synapse('mm.vw_gpd_dim_creative')
dim_strategy_synapse = jdbc_connection_synapse('mm.vw_gpd_dim_strategy')
dim_mediabuy_synapse = jdbc_connection_synapse('mm.vw_gpd_dim_mediabuy')
dim_site_synapse = jdbc_connection_synapse('mm.vw_gpd_dim_site')
dim_country_synapse = jdbc_connection_synapse('mm.vw_gmd_dim_country')
dim_product_synapse = jdbc_connection_synapse('mm.vw_gmd_dim_product')

# COMMAND ----------

prod_dim_calendar_synapse = jdbc_connection_synapse('mm.vw_dim_calendar') 

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='CA').groupBy('country_id').agg(min('date'),max('date')).display()

# COMMAND ----------

# SA
mini= '20220531'
maxi = '20231008'
prod_dim_calendar_synapse.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance_synapse.filter(col('country_id')=='SA').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar_synapse.calendar_id == df_fact_performance_synapse.date)).display()


# COMMAND ----------

# US
mini= '20230101'
maxi = '20231010'
prod_dim_calendar_synapse.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance_synapse.filter(col('country_id')=='US').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar_synapse.calendar_id == df_fact_performance_synapse.date)).display()


# COMMAND ----------

# EG
mini= '20220718'
maxi = '20231007'
prod_dim_calendar_synapse.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance_synapse.filter(col('country_id')=='EG').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar_synapse.calendar_id == df_fact_performance_synapse.date)).display()


# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='EG').filter(col('date').isin('20230618',
'20230617',
'20230616',
'20230615',
'20230614',
'20230613',
'20230612',
'20230611',
'20230610',
'20230609',
'20230608',
'20230607',
'20230606',
'20230605',
'20230604',
'20230603')).display()

# COMMAND ----------

# CA
mini= '20230101'
maxi = '20231010'
prod_dim_calendar_synapse.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance_synapse.filter(col('country_id')=='CA').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar_synapse.calendar_id == df_fact_performance_synapse.date)).display()


# COMMAND ----------

# AE
mini= '20220718'
maxi = '20231007'
prod_dim_calendar_synapse.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).select('calendar_id').join(df_fact_performance_synapse.filter(col('country_id')=='AE').select('country_id','date').dropDuplicates(), how= 'left', on = (prod_dim_calendar_synapse.calendar_id == df_fact_performance_synapse.date)).display()


# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id').isin('SA','CA','EG','AE','US')).groupBy('country_id').agg(min('date'),max('date')).display()

# COMMAND ----------

dim_country_synapse.filter(col('countrycode').isin('EGY','ARE','SAU','USA','CAN')).display()
dim_country_synapse.display()

# COMMAND ----------

prod_dim_calendar_synapse.filter(col('cal_year').isin('2022','2023')).display()

# COMMAND ----------

mini= '20220531'
maxi = '20231008'
prod_dim_calendar_synapse.filter((col('calendar_id')>=mini) & (col('calendar_id')<= maxi) ).display()
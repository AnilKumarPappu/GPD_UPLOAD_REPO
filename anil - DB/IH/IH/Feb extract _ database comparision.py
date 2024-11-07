# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark as ps
import numpy as np
import pandas as pd

# COMMAND ----------

# def jdbc_connection_dev(dbtable):
#     url = 'jdbc:sqlserver://marsanalyticsdevsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationdevsqldb'
#     user_name =dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'username2')
#     password = dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'password2')
#     df = spark.read \
#     .format('jdbc') \
#     .option('url',url) \
#     .option('dbtable',dbtable) \
#     .option('user',user_name) \
#     .option('password',password) \
#     .option('authentication','ActiveDirectoryPassword') \
#     .load()
#     return df
def jdbc_connection_prod(dbtable):
    url = 'jdbc:sqlserver://marsanalyticsprodsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationprodsqldb'
    user_name =dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databricksusername')
    password = dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databrickspassword')
    df = spark.read \
    .format('jdbc') \
    .option('url',url) \
    .option('dbtable',dbtable) \
    .option('user',user_name) \
    .option('password',password) \
    .option('authentication','ActiveDirectoryPassword') \
    .load()
    return df


# COMMAND ----------

# dev_ih_spend = jdbc_connection_dev('mm_test.vw_gmd_fact_ih_spend')
# dev_ih_ingoing = jdbc_connection_dev('mm_test.vw_gmd_fact_ih_ingoingspend')
# dev_dim_country = jdbc_connection_dev('mm_test.vw_gmd_dim_country')
# dev_dim_product = jdbc_connection_dev('mm_test.vw_gmd_dim_product')
# dev_dim_calendar = jdbc_connection_dev('mm_test.vw_dim_calendar')
prod_ih_spend_3 = jdbc_connection_prod('mm.vw_gpd_fact_ih_spend')
prod_ih_ingoing_3 = jdbc_connection_prod('mm.vw_gpd_fact_ih_ingoingspend')
prod_dim_country_3 = jdbc_connection_prod('mm.vw_gpd_dim_country')
prod_dim_product_3 = jdbc_connection_prod('mm.vw_gpd_dim_product')
prod_dim_calendar_3 = jdbc_connection_prod('mm.vw_dim_calendar')  ## check the connection name

# COMMAND ----------

prod_ih_ingoing_3.groupBy('country_id',
 'product_id',
 'budget_year',
 'plan_desc',
 'media_type',
 'start_date',
 'end_date').agg(sum('ingoing_spend_per_day_lc').alias('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd').alias('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## at granular level
# MAGIC

# COMMAND ----------

prod_ih_ingoing_3.join(prod_dim_country_3,on='country_id',how='left').join(prod_dim_product_3,on= 'product_id',how= 'left').filter(col('budget_year').isin('2019','2020','2021','2022','2023')).groupBy('country_desc',
 'product',
 'budget_year',
 'plan_desc',
 'media_type',
 'start_date',
 'end_date').agg(sum('ingoing_spend_per_day_lc').alias('ingoing_spend_lc'),sum('ingoing_spend_per_day_usd').alias('ingoing_spend_usd')).display()

# COMMAND ----------

prod_ih_ingoing_3.join(prod_dim_country_3,on='country_id',how='left').join(prod_dim_product_3,on= 'product_id',how= 'left').filter((col('budget_year')=='2022') & (col('country_desc')=='UNITED KINGDOM') & (col('plan_desc') == '2022 United Kingdom Wrigley Skittles Skittles')).select('budget_year','plan_desc','media_type','start_date','end_date','country_desc','product','ingoing_spend_per_day_lc','ingoing_spend_per_day_usd').groupBy('budget_year','plan_desc','media_type','start_date','end_date','country_desc','product').agg(sum('ingoing_spend_per_day_lc').alias('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd').alias('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

.filter(col('budget_year').isin('2019','2020','2021','2022'))

# COMMAND ----------

prod_ih_ingoing_3.join(prod_dim_country_3,on='country_id',how='left').join(prod_dim_product_3,on= 'product_id',how= 'left').filter(col('budget_year').isin('2019','2020','2021','2022')).groupBy('country_desc','budget_year').agg(sum('ingoing_spend_per_day_lc').alias('ingoing_spend_lc'),sum('ingoing_spend_per_day_usd').alias('ingoing_spend_usd')).display()
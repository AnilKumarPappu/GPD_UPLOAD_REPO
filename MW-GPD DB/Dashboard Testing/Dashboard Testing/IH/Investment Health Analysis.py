# Databricks notebook source
# MAGIC %md
# MAGIC ## Loading Files

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark as ps
import numpy as np
import pandas as pd

# COMMAND ----------

def jdbc_connection_dev(dbtable):
    url = 'jdbc:sqlserver://marsanalyticsdevsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationdevsqldb'
    user_name =dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'username2')
    password = dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'password2')
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

dev_ih_spend = jdbc_connection_dev('mm_test.vw_gmd_fact_ih_spend')
dev_ih_ingoing = jdbc_connection_dev('mm_test.vw_gmd_fact_ih_ingoingspend')
dev_dim_country = jdbc_connection_dev('mm_test.vw_gmd_dim_country')
dev_dim_product = jdbc_connection_dev('mm_test.vw_gmd_dim_product')
dev_dim_calendar = jdbc_connection_dev('mm_test.vw_dim_calendar')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Viewing Data

# COMMAND ----------

dev_ih_spend.display()

# COMMAND ----------

dev_ih_ingoing.display()

# COMMAND ----------

dev_dim_country.display()

# COMMAND ----------

dev_dim_product.display()

# COMMAND ----------

dev_dim_calendar.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Basic Data Checks

# COMMAND ----------

#1
dev_ih_spend.select(col('country_id')).distinct().display()
dev_ih_ingoing.select(col('country_id')).distinct().display()

# COMMAND ----------

#2
dev_ih_spend.select(max(col('day'))).display()
dev_ih_ingoing.select(max(col('version_date'))).display()
dev_ih_ingoing.select(max(col('day'))).display()

# COMMAND ----------

#dev_ih_spend.filter(col('country_id') == 'CA').select(max(col('day'))).display()


# COMMAND ----------

#dev_ih_spend.count() #162518 on 26-04 #164902 on 27-04 #201595 on 28-04 #578806 on 02-05 #700010 on 10-05
#dev_ih_ingoing.count() #417 on 26-04  #465 on 27-04 #877 on 28-04 #1656967 on 02-05 #2101402 on 09-05

# COMMAND ----------

#3
dev_ih_ingoing.select("country_id","budget_year").groupby("country_id").agg(max(col("budget_year"))).display()
dev_ih_spend.select("country_id","budget_year").groupby("country_id").agg(max(col("budget_year"))).display()

# COMMAND ----------

dev_ih_spend.schema

# COMMAND ----------

df_spend = dev_ih_spend
df_spend = df_spend.withColumn("cost_usd",col("cost_per_day_usd").cast('double'))
df_spend = df_spend.withColumn("cost_lc",col("cost_per_day_lc").cast('double'))
df_spend = df_spend.withColumn("ingoing_usd",col("ingoing_spend_per_day_usd").cast('double'))
df_spend = df_spend.withColumn("ingoing_lc",col("ingoing_spend_per_day_lc").cast('double'))
#df_spend.select('budget_year','country_id','cost_usd','ingoing_usd').groupby('budget_year','country_id').sum('cost_usd','ingoing_usd').display()
df_spend.select('budget_year','cost_lc').groupby('budget_year').sum('cost_lc').display()

# COMMAND ----------

dev_ih_spend.select('budget_year','cost_per_day_lc').groupby('budget_year').agg(sum('cost_per_day_lc')).display()

# COMMAND ----------

dev_dim_product.filter(col('product_id').contains('US')).display()

# COMMAND ----------

##Diagnosing LC issue
lc_sample = dev_ih_spend.filter("country_id in ('US','GB','DE','MX','AU')")
lc_sample.select('budget_year','cost_per_day_lc').groupby('budget_year').agg(sum('cost_per_day_lc')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##IH Analysis

# COMMAND ----------

dev_ih_spend.filter(col("ingoing_spend_per_day_lc").isNull()).select('country_id','ingoing_spend_per_day_lc').groupby('country_id').count().display()
dev_ih_spend.groupby('country_id').count().display()

# COMMAND ----------

dev_ih_spend.filter(col("ingoing_spend_per_day_usd").isNull()).select('country_id','ingoing_spend_per_day_usd').groupby('country_id').count().display()

# COMMAND ----------

dev_ih_spend.filter(col("cost_per_day_lc").isNull()).select('country_id','cost_per_day_lc').groupby('country_id').count().display()

# COMMAND ----------

dev_ih_spend.filter(col("cost_per_day_usd").isNull()).select('country_id','cost_per_day_usd').groupby('country_id').count().display()

# COMMAND ----------

#dev_ih_spend.select('country_id','budget_year','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('country_id','budget_year').sum('cost_per_day_usd','ingoing_spend_per_day_usd').display()
dev_ih_spend.select('country_id','budget_year','cost_per_day_lc','ingoing_spend_per_day_lc').groupby('country_id','budget_year').sum('cost_per_day_lc','ingoing_spend_per_day_lc').display()

# COMMAND ----------

dev_ih_spend.select('country_id','budget_year','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('country_id','budget_year').sum('cost_per_day_usd','ingoing_spend_per_day_usd').display()

# COMMAND ----------

dev_spend_prod = dev_ih_spend.join(dev_dim_product, on = 'product_id', how = 'inner')
dev_spend_prod.limit(10).display()

# COMMAND ----------

#dev_spend_prod.count() #578806
#dev_spend_prod = dev_spend_prod.withColumn("cost_per_day_usd_2",col("cost_per_day_usd").cast('double'))
#dev_spend_prod.select('strategic_cell','cost_per_day_usd').groupby('strategic_cell').sum('cost_per_day_usd').display()

dev_spend_prod = dev_ih_spend.join(dev_dim_product, on = 'product_id', how = 'inner')
dev_spend_prod.select('country_id','brand','portfolio','strategic_cell').distinct().display()

# COMMAND ----------

dev_spend_prod.select('brand','portfolio').filter(col('portfolio') == 'miscellaneous').distinct().display()

# COMMAND ----------

dev_dim_product.filter(col('product_id').contains('Synthesis')).display()

# COMMAND ----------

dev_spend_country = dev_ih_spend.join(dev_dim_country,on = 'country_id',how = 'inner')
dev_spend_country.select('country_id','region','marketregioncode').distinct().display()

# COMMAND ----------

dev_dim_country.filter(col('marketregioncode') == 'EMEA').select('country_id').display()
#dev_dim_country.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batchwise Analysis

# COMMAND ----------

#ih_spend_sample = dev_ih_spend.filter("country_id in ('NL','PL','FR','AE','SA','EG','KE')")
#ih_spend_sample = dev_ih_spend.filter("country_id in ('GB','US','AU','MX','DE')")
#ih_spend_sample = dev_ih_spend.filter("country_id in ('AU','MX','DE','PL','FR','AE','SA','KE')")
ih_spend_sample = dev_ih_spend.filter("country_id in ('MY', 'IN', 'HK', 'VN', 'KR', 'ID', 'NZ', 'PH', 'SG', 'TH', 'TW', 'JP')")
#ih_spend_sample = dev_ih_spend.filter("country_id in ('GB','DE','PL','FR','AE','SA','KE','MX','AU','IN','TW','
ih_spend_sample.limit(10).display()

# COMMAND ----------

#ih_ingoing_sample = dev_ih_ingoing.filter("country_id in ('GB','US','AU','MX','DE')")
ih_ingoing_sample = dev_ih_ingoing.filter("country_id in ('NL','PL','FR','AE','SA','EG','KE')")
#ih_ingoing_sample = dev_ih_ingoing.filter("country_id in ('MY', 'IN', 'HK', 'VN', 'KR', 'ID', 'NZ', 'PH', 'SG', 'TH', 'TW', 'JP')")
ih_ingoing_sample.limit(10).display()

# COMMAND ----------

ih_ingoing_sample.select("country_id","budget_year").groupby("country_id").agg(max(col("budget_year"))).display()
ih_spend_sample.select("country_id","budget_year").groupby("country_id").agg(max(col("budget_year"))).display()

# COMMAND ----------

ih_spend_sample.select('country_id').distinct().display()
ih_ingoing_sample.select('country_id').distinct().display()


# COMMAND ----------

ih_spend_sample.select('channel_mix','media_type').distinct().display()

# COMMAND ----------

##important
#ih_spend_sample.filter(col('start_date') > col('end_date')).display() #No cases where start date is greater than end date
#ih_spend_sample.filter(col('start_date') > col('day')).display() #No cases where start date is greater than day
#ih_spend_sample.filter(col('end_date') < col('day')).display() #No cases where day is greater than end date


# COMMAND ----------

ih_spend_sample.join(dev_dim_product, on = 'product_id', how = 'inner').select('plan_name','budget_year').distinct().display()

#Issue: Does Plan Name has to adhere to a standard convention?

# COMMAND ----------

ih_spend_sample.select('country_id','plan_name').distinct().display()
#NZ does not follow naming convention at all.

# COMMAND ----------

#ih_ingoing_sample.count() #726362 for 5 major countries #316949 for EMEA #149202 for Batch 3
ih_spend_sample.count() #227435 for 5 major countries #98513 for EMEA #59605 for Batch 3

# COMMAND ----------

ih_spend_sample.filter(col('cost_per_day_usd') != col('ingoing_spend_per_day_usd')).display()
#Cost and Ingoing are different only for 2023 for 5 major countries
#For EMEA, we are seeing different Cost and Ingoing values for Egypt (2020), Saudi Arabia (2022) and rest countries (2023)
#For Batch 3, we are seeing different Cost and Ingoing values for Philippines (2015), India (2022) and rest countries (2023)

# COMMAND ----------

temp = ih_spend_sample.select(col('plan_name').alias('plan_name_spend'))
ih_ingoing_sample.select('plan_name').distinct().join(temp.distinct(), ih_ingoing_sample.plan_name == temp.plan_name_spend, how = 'outer').display()
#temp.display()

#Issue: Significant number of plan_names in ingoing_spend but not in spend table in 5 major countries
#Issue: Significant number of plan_names in ingoing_spend but not in spend table for EMEA. However, no plan_name in Spend which isn't there in Ingoing.

# COMMAND ----------

ih_spend_sample.filter(col('plan_name') == '2023 Starburst Approved P4 MFA 4.5.23').display()

# COMMAND ----------

ih_spend_sample.filter((col('cost_per_day_usd').isNull()) & (col('cost_per_day_lc').isNotNull())).display()
ih_spend_sample.filter((col('cost_per_day_usd') == 0) & (col('cost_per_day_lc') != 0)).display()


# COMMAND ----------

ih_spend_sample.select('country_id','budget_year').groupby('country_id','budget_year').count().display()

# COMMAND ----------

ih_spend_sample.filter((col('cost_per_day_lc').isNull()) & (col('cost_per_day_usd').isNotNull())).display()
ih_spend_sample.filter((col('cost_per_day_lc') == 0) & (col('cost_per_day_usd') != 0)).display()
#ih_spend_sample.filter((col('cost_per_day_lc').isNull()) & (col('cost_per_day_usd').isNotNull())).select('country_id','budget_year').groupby('country_id','budget_year').count().display()

# COMMAND ----------

ih_spend_sample.filter((col('ingoing_spend_per_day_usd').isNull()) & (col('ingoing_spend_per_day_lc').isNotNull())).display()
ih_spend_sample.filter((col('ingoing_spend_per_day_usd') == 0) & (col('ingoing_spend_per_day_lc') != 0)).display()


# COMMAND ----------

ih_spend_sample.filter((col('ingoing_spend_per_day_lc').isNull()) & (col('ingoing_spend_per_day_usd').isNotNull())).display()
ih_spend_sample.filter((col('ingoing_spend_per_day_lc') == 0) & (col('ingoing_spend_per_day_usd') != 0)).display()

##RESUME VALIDATION FROM HERE

# COMMAND ----------

ih_spend_sample.filter(col('country_id') == 'US').count()

# COMMAND ----------

ih_spend_sample.filter((col('cost_per_day_lc') != 0) & (col('ingoing_spend_per_day_lc') == 0)).select('country_id','budget_year','ingoing_spend_per_day_lc').groupby('country_id','budget_year').count().display()

ih_spend_sample.filter((col('cost_per_day_usd') != 0) & (col('ingoing_spend_per_day_usd') == 0) & (col('budget_year') == 2023)).select('country_id','ingoing_spend_per_day_usd').groupby('country_id').count().display()


#20651


# COMMAND ----------

ih_spend_sample.select('country_id','budget_year','ingoing_spend_per_day_lc').groupBy('country_id','budget_year').count().display()

# COMMAND ----------

dev_ih_spend.filter(col('country_id') == 'US').select('country_id','budget_year','ingoing_spend_per_day_usd').groupby('country_id','budget_year').sum('ingoing_spend_per_day_usd').display()

# COMMAND ----------

ih_spend_sample.select('budget_year','country_id','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('budget_year','country_id').sum('cost_per_day_usd','ingoing_spend_per_day_usd').display()

# COMMAND ----------

dev_dim_product.filter(col('portfolio') == 'miscellaneous').select('brand').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard View - Build

# COMMAND ----------

df_spend = ih_spend_sample.join(dev_dim_product, on = 'product_id', how = 'inner')
#df_spend = df_spend.withColumn("cost_usd",col("cost_per_day_usd").cast('double'))
#df_spend = df_spend.withColumn("cost_lc",col("cost_per_day_lc").cast('double'))
#df_spend = df_spend.withColumn("ingoing_usd",col("ingoing_spend_per_day_usd").cast('double'))
#df_spend = df_spend.withColumn("ingoing_lc",col("ingoing_spend_per_day_lc").cast('double'))
#df_spend = df_spend.select('country_id','region','budget_year','channel_mix','media','media_type','brand','portfolio','segment','strategic_cell','cost_usd','cost_lc','ingoing_usd','ingoing_lc')
df_spend = df_spend.select('country_id','region','budget_year','channel_mix','media','media_type','brand','portfolio','segment','strategic_cell','cost_per_day_usd','cost_per_day_lc','ingoing_spend_per_day_usd','ingoing_spend_per_day_lc')


# COMMAND ----------

df_spend.display()

# COMMAND ----------

#Budget Year vs Cost, Ingoing
#df_spend.select('budget_year','cost_per_day_usd','ingoing_spend_per_day_usd').groupBy('budget_year').sum('cost_per_day_usd','ingoing_spend_per_day_usd').display()
#df_spend.select('budget_year','cost_usd','ingoing_usd').groupBy('budget_year').sum('cost_usd','ingoing_usd').display()
#df_spend.select('budget_year','cost_per_day_lc','ingoing_spend_per_day_lc').groupBy('budget_year').sum('cost_per_day_lc','ingoing_spend_per_day_lc').display()
#df_spend.select('budget_year','country_id','cost_per_day_usd','cost_per_day_lc', 'ingoing_spend_per_day_lc', 'ingoing_spend_per_day_usd').groupBy('budget_year','country_id').sum('cost_per_day_usd','cost_per_day_lc', 'ingoing_spend_per_day_lc', 'ingoing_spend_per_day_usd').display()
dev_ih_spend.filter(col('country_id') == 'US').select('budget_year','cost_per_day_usd','cost_per_day_lc', 'ingoing_spend_per_day_lc', 'ingoing_spend_per_day_usd').groupBy('budget_year').sum('cost_per_day_usd','cost_per_day_lc', 'ingoing_spend_per_day_lc', 'ingoing_spend_per_day_usd').display()



# Issue: Why cost and ingoing exactly same for each year? #Update: Already notified
# Issue: Cost and Ingoing LC exactly same for all years except 2022, 2023


# COMMAND ----------

df_spend_2 = ih_spend_sample
df_spend_2 = df_spend_2.withColumn("cost_usd",col("cost_per_day_usd").cast('double'))
df_spend_2 = df_spend_2.withColumn("cost_lc",col("cost_per_day_lc").cast('double'))
df_spend_2 = df_spend_2.withColumn("ingoing_usd",col("ingoing_spend_per_day_usd").cast('double'))
df_spend_2 = df_spend_2.withColumn("ingoing_lc",col("ingoing_spend_per_day_lc").cast('double'))
df_spend_2 = df_spend_2.select('country_id','region','budget_year','channel_mix','media','media_type','cost_usd','cost_lc','ingoing_usd','ingoing_lc')

# COMMAND ----------

df_spend.filter(col('cost_usd') != col('ingoing_usd')).display()

#Cost and Ingoing only different for 2023

# COMMAND ----------

df_spend.select('budget_year','channel_mix','cost_per_day_usd','ingoing_spend_per_day_usd').groupBy('budget_year','channel_mix').sum('cost_per_day_usd','ingoing_spend_per_day_usd').display()

# COMMAND ----------

df_spend.select('brand','portfolio','strategic_cell').distinct().display()

#Issue: Portfolio, Strategic_Cell still getting mapped to 'miscellenous'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Issues

# COMMAND ----------

#1 
ih_ingoing_sample.select('plan_name').distinct().join(temp.distinct(), ih_ingoing_sample.plan_name == temp.plan_name_spend, how = 'outer').display()

# COMMAND ----------

#2
df_spend.select('budget_year','cost_usd','ingoing_usd').groupBy('budget_year').sum('cost_usd','ingoing_usd').display()

# COMMAND ----------

#3
dev_spend_country.select('country_id','region','marketregioncode').distinct().display()

# COMMAND ----------

#4
ih_spend_sample.filter((col('cost_per_day_usd').isNull()) & (col('cost_per_day_lc').isNotNull())).display()
#count: 18457

# COMMAND ----------

#5
ih_spend_sample.filter(col('plan_name').isin(['2023 Starburst Approved P4 MFA 4.5.23','2023 Skittles Approved P4 MFA 4.5.23','2023 Orbit Approved Flowchart P4 4.6.23','2023 Extra Approved Flowchart P4 4.6.23','2023 5 Gum Approved Flowchart P4 4.6.23'])).display()

# COMMAND ----------


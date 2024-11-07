# Databricks notebook source
# MAGIC %md
# MAGIC ## Importing libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark as ps
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load tables

# COMMAND ----------

# MAGIC %md
# MAGIC --> Since the tables are in different RG, we are using JDBC connection to retrieve the tables.

# COMMAND ----------

#Synapse - prod
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
# #Synapse -Dev
# def jdbc_connection(dbtable):
#     url = "jdbc:sqlserver://globalxsegsrmdatafoundationdevsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
#     user_name = dbutils.secrets.get(
#         scope="mwoddasandbox1005devsecretscope", key="databricksusername"
#     )
#     password = dbutils.secrets.get(
#         scope="mwoddasandbox1005devsecretscope", key="databrickspassword"
#     )
#     df = (
#         spark.read.format("jdbc")
#         .option("url", url)
#         .option("dbtable", dbtable)
#         .option("user", user_name)
#         .option("password", password)
#         .option("authentication", "ActiveDirectoryPassword")
#         .load()
#     )
#     return df


# COMMAND ----------

ih_spend = jdbc_connection('mm.vw_mw_gpd_fact_ih_spend')
ih_ingoing = jdbc_connection('mm.vw_mw_gpd_fact_ih_ingoingspend')
dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')
dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_calendar =jdbc_connection('mm.vw_dim_calendar') 
df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')


# COMMAND ----------

ih_spend.select('country_id').distinct().join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').display()

# COMMAND ----------

# ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('country_id') == 'GB').filter(col('year') == '2022').groupBy('channel_mix','plan_desc_new','start_date','end_date').agg(sum('ingoing_spend_per_day_usd')).select().display()
# ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('country_id') == 'GB').filter(col('year') == '2022').groupBy('channel_mix','plan_desc_new','start_date','end_date').agg(sum('ingoing_spend_per_day_usd')).display()


# ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('plan_desc_new') == '2022_GB_M&Ms_Online Video').display()
# ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('plan_desc_new') == '2022_GB_M&Ms_Online Video').display()

# ih_ingoing.select('version_date').distinct().display()
ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') >= '2019').filter(col('year')<= '2023').groupBy('year').agg(sum('ingoing_spend_per_day_usd'),sum('ingoing_spend_per_day_lc')).display()

ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') >= '2019').filter(col('year')<= '2023').groupBy('year', 'version_date').agg(sum('ingoing_spend_per_day_usd'),sum('ingoing_spend_per_day_lc')).display()


# COMMAND ----------

## unique plans in spend table
ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').select('plan_desc_new','start_date','end_date').dropDuplicates().count()

# COMMAND ----------

ih_spend.filter(col('budget_year') != '2024').filter(col('ingoing_spend_per_day_lc').isNull()).display()

# COMMAND ----------

ih_ingoing.filter(col('plan_desc_new') == '2023_GB_Celebrations_Audio Streaming').display()

# COMMAND ----------

ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').select('version_date','country_id', 'plan_desc_new','start_date','end_date').dropDuplicates().filter(col('version_date') == '2024-02-19').filter(col('country_id').isin(['MY', 'UA', 'BR'])).groupBy('version_date').count().display()

# COMMAND ----------

ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('budget_year') == '2024').select('version_date', 'plan_desc_new','start_date','end_date').dropDuplicates().groupBy('version_date').count().display()

# COMMAND ----------

## plan count on both versions

ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').select('version_date', 'plan_desc_new','start_date','end_date').dropDuplicates().groupBy('version_date').count().display()

# ### country count on bith versions
ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').select('version_date', 'country_id').dropDuplicates().groupBy('version_date').count().display()

lat = ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').select('version_date', 'country_id').dropDuplicates().filter(col('version_date')== '2024-02-19').select('country_id').distinct()
pre = ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').select('version_date', 'country_id').dropDuplicates().filter(col('version_date')== '2023-02-28').select('country_id').distinct()

set_column1 = set(lat.rdd.map(lambda x: x[0]).collect())
set_column2 = set(pre.rdd.map(lambda x: x[0]).collect())

# Find values in column1 that are not in column2
uncommon_values_column1 = set_column1 - set_column2
print(uncommon_values_column1)

ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('year') == '2023').groupBy('country_id','version_date').agg(sum('ingoing_spend_per_day_usd'),sum('ingoing_spend_per_day_lc')).display()

# COMMAND ----------

# ih_spend.withColumn("year", expr("substr(day, 1, 4)")).groupBy('country_id','year').agg(sum('ingoing_spend_per_day_usd')).display()
# ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).groupBy('country_id','year').agg(sum('ingoing_spend_per_day_usd')).display()

# ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('country_id') == 'GB').filter(col('year') == '2023').groupBy('channel_mix','plan_desc_new','start_date','end_date').agg(sum('ingoing_spend_per_day_usd')).select().display()
# ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('country_id') == 'GB').filter(col('year') == '2023').groupBy('channel_mix','plan_desc_new','start_date','end_date').agg(sum('ingoing_spend_per_day_usd')).display()

ih_spend.withColumn("year", expr("substr(day, 1, 4)")).filter(col('plan_desc_new') == '2023_GB_Maltesers_Paid Social Video').display()
ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).filter(col('plan_desc_new') == '2023_GB_Maltesers_Paid Social Video').display()



# COMMAND ----------

# ih_spend.withColumn("year", expr("substr(day, 1, 4)")).join(dim_product.select('product_id','brand'), on = 'product_id', how = 'left').groupBy('year', 'country_id', 'channel_mix', 'brand').agg(sum('cost_per_day_usd'), sum('cost_per_day_lc')).display()
# ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).join(dim_product.select('product_id','brand'), on = 'product_id', how = 'left').groupBy('year', 'country_id', 'channel_mix', 'brand').agg(sum('ingoing_spend_per_day_usd'),sum('ingoing_spend_per_day_lc')).display()

ih_spend.withColumn("year", expr("substr(day, 1, 4)")).withColumn("date", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date'), on = 'date', how = 'left').groupBy('year', 'mars_period').agg(sum('cost_per_day_usd'), sum('cost_per_day_lc')).display()
ih_ingoing.withColumn("year", expr("substr(day, 1, 4)")).withColumn("date", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date'), on = 'date', how = 'left').groupBy('year', 'mars_period').agg(sum('ingoing_spend_per_day_usd'),sum('ingoing_spend_per_day_lc')).display()

# COMMAND ----------

ih_spend.withColumn("date_as_string", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == 'CL').filter(col('budget_year').isin(['2023'])).filter(col('channel_mix') != 'Digital').groupBy('country_id','budget_year','mars_period').agg(sum('cost_per_day_usd')).display()

# COMMAND ----------

from pyspark.sql.functions import col, date_format
ih_spend.withColumn("date_as_string", date_format(col("day"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == 'CL').filter(col('budget_year').isin(['2023'])).groupBy('country_id','budget_year','mars_period','plan_desc_new','channel_mix','start_date','end_date').agg(sum('cost_per_day_lc').alias('cost_per_day_lc') , sum('cost_per_day_usd').alias('cost_per_day_usd')).display()
# .groupBy('country_id','budget_year','mars_period').agg(sum('cost_per_day_usd')).display()
# .filter(col('channel_mix') != 'Digital')

# COMMAND ----------

ih_spend.filter(col('budget_year').isin(['2023','2024'])).filter(col('country_id').isin(['KE',
'VN',
'FR',
'NL',
'PE',
'SA',
'NZ',
'AU',
'GB',
'CO',
'IN',
'PR',
'CL',
'PL',
'BR',
'HK',
'EC',
'CA',
'DE',
'MX',
'TW',
'CR',
'AE',
'KR',
'PH',
'US',
'TH',
'PA',
'MY',
'EG'])).filter(col('channel_mix') == 'Digital').groupBy('country_id','budget_year').agg(sum('cost_per_day_usd')).display()

# COMMAND ----------

ih_spend.filter()

# COMMAND ----------

# MAGIC %md
# MAGIC -->> Applying fileter on syntesis data and for budget_year >=2019 (5 Years)

# COMMAND ----------

dim_product = dim_product.filter(col('vendor_id')=='SYNTHESIS')
ih_spend = ih_spend.filter(col('budget_year') >= 2019)
ih_ingoing = ih_ingoing.filter(col('budget_year') >= 2019)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Data type should be correct for all tables

# COMMAND ----------


ih_spend. printSchema()

# COMMAND ----------

ih_ingoing.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Cost to Client uniformaly divided on daily basis

# COMMAND ----------

# Cost to client / number of days = cost per day (LC & USD)

ih_spend.withColumn('days count', datediff(col('end_date'), col('start_date'))+1).\
    withColumn('calculated_cost_per_day_lc', 
               col('cost_to_client_lc')/col('days count')).\
    withColumn('lc_ratio', round(col('cost_per_day_lc'),2)/
           round(col('calculated_cost_per_day_lc'),2)).\
               filter(round(col('lc_ratio'),2) != 1.00).display()
    
ih_spend.withColumn('days count', datediff(col('end_date'), col('start_date'))+1).\
    withColumn('calculated_cost_per_day_usd', 
               col('cost_to_client_usd')/col('days count')).\
    withColumn('usd_ratio', round(col('cost_per_day_usd'),2)/
           round(col('calculated_cost_per_day_usd'),2)).\
               filter(round(col('usd_ratio'),2) != 1.00).display()
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.Ingoing Spend uniformaly divided on daily basis

# COMMAND ----------

# ingoing spend / number of days = ingoing spend per day (LC & USD)

ih_ingoing.withColumn('days count', datediff(col('end_date'), col('start_date'))+1).\
    withColumn('calculated_spend_per_day_lc', 
               col('ingoing_spend_lc')/col('days count')).\
    withColumn('lc_ratio', round(col('ingoing_spend_per_day_lc'),2)/
           round(col('calculated_spend_per_day_lc'),2)).\
               filter(round(col('lc_ratio'),2) != 1.00).display()
    
ih_ingoing.withColumn('days count', datediff(col('end_date'), col('start_date'))+1).\
    withColumn('calculated_spend_per_day_usd', 
               col('ingoing_spend_usd')/col('days count')).\
    withColumn('usd_ratio', round(col('ingoing_spend_per_day_usd'),2)/
           round(col('calculated_spend_per_day_usd'),2)).\
               filter(round(col('usd_ratio'),2) != 1.00).display()
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. No duplicate rows in any table

# COMMAND ----------

print("Duplicates in ih_spend   :-->", ih_spend.count()-ih_spend.dropDuplicates().count())
print("Duplicates in ih_ingoing :-->", ih_ingoing.count()-ih_ingoing.dropDuplicates().count())
print("Duplicates in dim_product:-->", dim_product.count()-dim_product.dropDuplicates().count())
print("Duplicates in dim_country:-->", dim_country.count()-dim_country.dropDuplicates().count())



# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Access Layer table schemas must match the ODDA-approved schema shared with DDAS
# MAGIC ----->>> Can be done Manually.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. All product_id in IH Spend should be present in dim_product

# COMMAND ----------

# fact_ih_spend and fact_ih_ingoing table

ih_spend.join(dim_product, on='product_id', how='left').filter(col('brand').isNull()).display()
ih_ingoing.join(dim_product, on='product_id', how='left').filter(col('brand').isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Ingoing LC should not be 0 if Ingoing USD present & vice versa

# COMMAND ----------

ih_ingoing.filter(col('ingoing_spend_usd')==0).filter(col('ingoing_spend_lc') != 0).display()
ih_ingoing.filter(col('ingoing_spend_lc')==0).filter(col('ingoing_spend_usd') != 0).display()

ih_ingoing.filter(col('ingoing_spend_per_day_usd')==0).filter(col('ingoing_spend_per_day_lc') != 0).display()
ih_ingoing.filter(col('ingoing_spend_per_day_lc')==0).filter(col('ingoing_spend_per_day_usd') != 0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Cost LC should not be 0 if Cost USD present & vice versa

# COMMAND ----------

ih_spend.filter((col('cost_to_client_lc') == 0) & (col('cost_to_client_usd')!=0)).display()
ih_spend.filter((col('cost_to_client_usd') == 0) & (col('cost_to_client_lc')!=0)).display()

ih_spend.filter((col('cost_per_day_lc') == 0) & (col('cost_per_day_usd')!=0)).display()
ih_spend.filter((col('cost_per_day_usd') == 0) & (col('cost_per_day_lc')!=0)).display()

ih_spend.filter((col('ingoing_spend_per_day_lc') == 0) & (col('ingoing_spend_per_day_usd')!=0)).display()
ih_spend.filter((col('ingoing_spend_per_day_usd') == 0) & (col('ingoing_spend_per_day_lc')!=0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9. All dim tables should not have null values

# COMMAND ----------


dim_product.select([count(when(isnull(c), c)).alias(c) for c in dim_product.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10. All fact table ids should be present in dim tables

# COMMAND ----------

# fact_ih_spend & fact_ih_ingoing table

#dim_product table
ih_spend.join(dim_product, on='product_id', how='left').filter(col('brand').isNull()).display()
ih_ingoing.join(dim_product, on='product_id', how='left').filter(col('brand').isNull()).display()

#dim_country table
ih_spend.select(col('country_id')).dropDuplicates().join(dim_country, 
                                                         on='country_id', 
                                                         how='left').filter(
                                                             dim_country.country_id.isNull()).display()
ih_ingoing.select(col('country_id')).dropDuplicates().join(dim_country, 
                                                           on='country_id', 
                                                           how='left').filter(
                                                               dim_country.country_id.isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11. For countries having buying currency as USD, Cost LC & Cost USD values should be same for budget_year >= 2023
# MAGIC ---->>>> "AZ", "CO", "EC", "GE", "IQ", "OM", "PE", "PR", "AE", "UY", "US", "SA" countries have same spend (LC & USD)

# COMMAND ----------


# Checking for fact_ih_spend table

countries = ["AZ", "CO", "EC", "GE", "IQ", "OM", "PE", "PR", "AE", "UY", "US", "SA"]
df1 = ih_spend.groupBy( 'country_id', 
                 'budget_year').agg(sum('cost_to_client_lc'),
                                            sum('cost_to_client_usd'),
                                            sum('cost_per_day_lc'),
                                            sum('cost_per_day_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd'))
                            

df1 = df1.filter(col('country_id').isin(countries)).filter(col('budget_year')>=2023)
df1 = df1.withColumn('ratio_cost_to_client', col('sum(cost_to_client_lc)')/ col('sum(cost_to_client_usd)'))
df1 = df1.withColumn('ratio_cost_per_day', col('sum(cost_per_day_lc)')/ col('sum(cost_per_day_usd)'))
df1 = df1.withColumn('ratio_ingoing_spend_per_day', col('sum(ingoing_spend_per_day_lc)')/ col('sum(ingoing_spend_per_day_usd)'))

df1.filter(col('ratio_cost_per_day') != 1.00).display()                                                                           

# COMMAND ----------


# Checking for fact_ih_ingoing table

df2 = ih_ingoing.groupBy('country_id', 
                   'budget_year').agg(      sum('ingoing_spend_lc'),
                                            sum('ingoing_spend_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd'))



                                                        
df2 = df2.filter(col('country_id').isin(countries)).filter(col('budget_year')>=2023)
df2 = df2.withColumn('ratio_ingoing_spend', col('sum(ingoing_spend_lc)')/ col('sum(ingoing_spend_usd)'))
df2 = df2.withColumn('ratio_ingoing_spend_per_day', col('sum(ingoing_spend_per_day_lc)')/ 
                                                    col('sum(ingoing_spend_per_day_usd)'))

df2.filter(col('ratio_ingoing_spend') != 1.00).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 12. Ingoing Spend and Actual Spend should be the same at plan name level for 2019-2023

# COMMAND ----------

df_1 = df_fact_performance.withColumn("budget_year", year(df_fact_performance["date"]))
df_1 = df_1.groupBy('country_id', 'budget_year').agg(sum('actual_spend_dollars'), sum('actual_spend_local_currency'))
df_2 = ih_spend.filter(col('channel_mix') == 'Digital').groupBy('country_id', 'budget_year').agg(sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd'))

df = df_1.join(df_2, on=['country_id', 'budget_year'], how = 'left')
df = df.withColumn("actual_spend : ingoing_spend (LC)", col('sum(actual_spend_local_currency)')/col('sum(ingoing_spend_per_day_lc)'))

df = df_1.join(df_2, on=['country_id', 'budget_year'], how = 'left')
df = df.withColumn("actual_spend : ingoing_spend (USD)", col('sum(actual_spend_dollars)')/col('sum(ingoing_spend_per_day_usd)'))

df = df.withColumn("actual_spend : ingoing_spend (LC)", col('sum(actual_spend_local_currency)')/col('sum(ingoing_spend_per_day_lc)'))

df.filter(col('budget_year')==2024).display()

# COMMAND ----------

df_1 = df_fact_performance.withColumn("budget_year", year(df_fact_performance["date"]))
df_1 = df_1.groupBy('country_id', 'budget_year').agg(sum('actual_spend_dollars'), sum('actual_spend_local_currency'))
df_2 = ih_spend.filter(col('channel_mix') == 'Digital').groupBy('country_id', 'budget_year').agg(sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd'))

df = df_1.join(df_2, on=['country_id', 'budget_year'], how = 'left')
df = df.withColumn("actual_spend : ingoing_spend (LC)", col('sum(actual_spend_local_currency)')/col('sum(ingoing_spend_per_day_lc)'))

df = df_1.join(df_2, on=['country_id', 'budget_year'], how = 'left')
df = df.withColumn("actual_spend : ingoing_spend (USD)", col('sum(actual_spend_dollars)')/col('sum(ingoing_spend_per_day_usd)'))

df = df.withColumn("actual_spend : ingoing_spend (LC)", col('sum(actual_spend_local_currency)')/col('sum(ingoing_spend_per_day_lc)'))

df.filter(col('budget_year')==2024).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 13. plan_desc_new is combination of Budget Year _ CountryID _ Product _ Media-type

# COMMAND ----------


df = ih_spend.select(
                            'product_id', 'country_id', 
                            'budget_year', 'media_type', 'plan_desc_new').join(dim_product.select(
                                                                'product_id', 'product'
                                                                ), on ='product_id', how='left')
 
df = df.withColumn('created_plan_name', concat_ws('_', col('budget_year'), col('country_id'), col('product'), col('media_type')))

df.select('created_plan_name', 'plan_desc_new').dropDuplicates().groupBy('created_plan_name').agg(count('plan_desc_new').alias('plan_desc_new_count')).filter(col('plan_desc_new_count')>1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 14. Currency conversion ratio (Country X Year Level) (Local/USD value should be same for spend & spend_per_day)

# COMMAND ----------


df_ing = ih_ingoing.filter(col('budget_year')>=2019).groupBy(    'country_id', 'budget_year',
                            ).agg(
                                            sum('ingoing_spend_lc'),
                                            sum('ingoing_spend_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd'))

df_ing = df_ing.withColumn('ingoing_spend_(lc/usd)_ratio', col('sum(ingoing_spend_lc)')/col('sum(ingoing_spend_usd)'))
df_ing = df_ing.withColumn('ingoing_spend_per_day_(lc/usd)_ratio', col('sum(ingoing_spend_per_day_lc)')/col('sum(ingoing_spend_per_day_usd)'))

df_ing = df_ing.withColumn('ratio', col('ingoing_spend_(lc/usd)_ratio')/col('ingoing_spend_per_day_(lc/usd)_ratio')).display()

# COMMAND ----------


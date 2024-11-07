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
    url = "jdbc:sqlserver://xsegglobalmktgprodsynapse-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
    user_name = dbutils.secrets.get(
        scope="globalxsegsrmdatafoundationprodsecretscope", key="globalxsegsrmdatafoundationprodsynapseuserid"
    )
    password = dbutils.secrets.get(
        scope="globalxsegsrmdatafoundationprodsecretscope", key="globalxsegsrmdatafoundationprodsynapsepass"
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

ih_spend = jdbc_connection('mm.vw_mw_gpd_fact_ih_spend')
ih_ingoing = jdbc_connection('mm.vw_mw_gpd_fact_ih_ingoingspend')
dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')
dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_calendar =jdbc_connection('mm.vw_dim_calendar') 
df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')
som_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_som')


# COMMAND ----------

ih_spend.filter(col('plan_desc_new')=='2024_AT_Ice Cream_Paid Social Video').filter(col('budget_year')=='2024').display()
ih_ingoing.filter(col('plan_desc_new')=='2024_AT_Ice Cream_Paid Social Video').filter(col('budget_year')=='2024').display()

# COMMAND ----------

ih_spend.groupBy('country_id', 'budget_year').agg(sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd')).display()
ih_ingoing.groupBy('country_id', 'budget_year').agg(sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd')).display()


# COMMAND ----------

ih_spend.select('country_id').dropDuplicates().display()

# COMMAND ----------

ih_ingoing.select('country_id').dropDuplicates().display()

# COMMAND ----------

dim_product.display()

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

# MAGIC %md
# MAGIC #### 15. Missing country on budget Year level 
# MAGIC

# COMMAND ----------


df_1 = ih_ingoing.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2019",), ("2020",), ("2021",), ("2022",), ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")


df_2 = ih_ingoing.withColumn('Year', year('day'))
df_2 = df_2.groupBy('country_id', 'Year').agg(count('day'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------

ih_spend.join(dim_product, on ='product_id', how= 'left').groupBy(ih_spend.country_id).agg(
                                                                sum('cost_to_client_lc'),
                                                                sum('cost_to_client_usd'),
                                                                sum('cost_per_day_lc'),
                                                                sum('cost_per_day_usd'),
                                                                sum('ingoing_spend_per_day_lc'),
                                                                sum('ingoing_spend_per_day_usd'),
                                                                min('day'), max('day'), 
                                                                count('day')).display()
                                                                                                                                                                                                                                                  


# COMMAND ----------

ih_spend.join(dim_product, on ='product_id', how= 'left').groupBy(ih_spend.country_id, 'budget_year', 'brand').agg(
                                                                sum('cost_to_client_lc'),
                                                                sum('cost_to_client_usd'),
                                                                sum('cost_per_day_lc'),
                                                                sum('cost_per_day_usd'),
                                                                sum('ingoing_spend_per_day_lc'),
                                                                sum('ingoing_spend_per_day_usd'),
                                                                min('day'), max('day'), 
                                                                count('day')).display()
                                                                                                                                                                                                                                                  


# COMMAND ----------

ih_ingoing.join(dim_product, on ='product_id', how= 'left').groupBy(ih_ingoing.country_id).agg(
                                                                sum('ingoing_spend_lc'),
                                                                sum('ingoing_spend_usd'),
                                                                sum('ingoing_spend_per_day_lc'),
                                                                sum('ingoing_spend_per_day_usd'),
                                                                min('day'), max('day'),
                                                                count('day')).display()
                                                                                                                                                                                                                                                  


# COMMAND ----------

ih_ingoing.join(dim_product, on ='product_id', how= 'left').groupBy(ih_ingoing.country_id, 'budget_year', 'brand').agg(
                                                                sum('ingoing_spend_lc'),
                                                                sum('ingoing_spend_usd'),
                                                                sum('ingoing_spend_per_day_lc'),
                                                                sum('ingoing_spend_per_day_usd'),
                                                                min('day'), max('day'),
                                                                count('day')).display()
                                                                                                                                                                                                                                                  


# COMMAND ----------

som_fact_performance.display()

# COMMAND ----------



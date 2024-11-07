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
df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')

dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_calendar =jdbc_connection('mm.vw_dim_calendar') 
dim_channel = jdbc_connection('mm.vw_mw_gpd_dim_channel')
som_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_som')
som_product = jdbc_connection('mm.vw_mw_gpd_dim_product_som')


# COMMAND ----------

dim_product.select( 'country_id',
                    'product',
                    'brand',
                    'portfolio',
                    'segment').dropDuplicates().display()

# COMMAND ----------

dim_country.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Filter synthesis data from 2019 or after

# COMMAND ----------


ih_spend = ih_spend.filter(col('budget_year') >= 2019)
ih_ingoing = ih_ingoing.filter(col('budget_year') >= 2019)
dim_product = dim_product.filter(col('vendor_id')=='SYNTHESIS')

# COMMAND ----------

# MAGIC %md
# MAGIC A

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. In Portfolio, Cell and Brands there should be no repetition of similar values. 

# COMMAND ----------

dim_product.select(
                    'brand',
                    'portfolio',
                    'strategic_cell').dropDuplicates().groupBy(['brand', 
                                                                'portfolio',
                                                                ]).agg(count('strategic_cell')).filter(
                                                                            col('count(strategic_cell)')>1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### B. All prod_id in IH Spend should be present in dim_product

# COMMAND ----------

ih_spend.select('product_id').dropDuplicates().join(dim_product.select('product_id', 'portfolio'), 
                                                    on = 'product_id',
                                                    how = 'left').filter(col('portfolio').isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### C. Ingoing Spend Table should always have data from Feb (latest year).

# COMMAND ----------

year_checking_for = 2024

df_1 = ih_ingoing.groupBy( 'country_id',
                    'budget_year').agg(sum('ingoing_spend_lc'),
                                       sum('ingoing_spend_usd'),
                                       sum('ingoing_spend_per_day_lc'),
                                       sum('ingoing_spend_per_day_usd')).filter(col('budget_year')==year_checking_for)

df_2 = ih_ingoing.select('country_id').dropDuplicates()                           

df_2.join(df_1, on='country_id', how='left').filter(col('budget_year').isNull()).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### D. Ingoing LC should not be 0 if Ingoing USD present & vice versa

# COMMAND ----------

ih_ingoing.filter(col('ingoing_spend_usd')==0).filter(col('ingoing_spend_lc') != 0).display()
ih_ingoing.filter(col('ingoing_spend_lc')==0).filter(col('ingoing_spend_usd') != 0).display()

ih_ingoing.filter(col('ingoing_spend_per_day_usd')==0).filter(col('ingoing_spend_per_day_lc') != 0).display()
ih_ingoing.filter(col('ingoing_spend_per_day_lc')==0).filter(col('ingoing_spend_per_day_usd') != 0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### E. Cost LC should not be 0 if Cost USD present & vice versa

# COMMAND ----------

ih_spend.filter((col('cost_to_client_lc') == 0) & (col('cost_to_client_usd')!=0)).display()
ih_spend.filter((col('cost_to_client_usd') == 0) & (col('cost_to_client_lc')!=0)).display()

ih_spend.filter((col('cost_per_day_lc') == 0) & (col('cost_per_day_usd')!=0)).display()
ih_spend.filter((col('cost_per_day_usd') == 0) & (col('cost_per_day_lc')!=0)).display()

ih_spend.filter((col('ingoing_spend_per_day_lc') == 0) & (col('ingoing_spend_per_day_usd')!=0)).display()
ih_spend.filter((col('ingoing_spend_per_day_usd') == 0) & (col('ingoing_spend_per_day_lc')!=0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### F. Country names in Synthesis and dim_country should match

# COMMAND ----------

df_1 = dim_country.select('country_id', lit('availble_in_country_table'))
df_2 = ih_ingoing.select('country_id', lit('available_in_ingoing_table')).dropDuplicates()
df_3 = ih_spend.select('country_id', lit('available_in_spend_table')).dropDuplicates()
print('# in dim_country:- ', df_1.count(), '  ::# in ingoing:-', df_2.count(), '  ::# in spend:-', df_3.count())
df_1.join(df_2, on='country_id', how='left').join(df_3, on='country_id', how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### G. Ingoing Spend and Actual Spend should be the same at plan name level for 2019-2023

# COMMAND ----------

df_1 = df_fact_performance.withColumn("budget_year", year(df_fact_performance["date"]))
df_1 = df_1.groupBy('country_id', 'budget_year').agg(sum('actual_spend_dollars'), sum('actual_spend_local_currency'))
df_2 = ih_spend.groupBy('country_id', 'budget_year').agg(sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd'))

df = df_1.join(df_2, on=['country_id', 'budget_year'], how = 'left')
df = df.withColumn("actual_spend : ingoing_spend (LC)", col('sum(actual_spend_local_currency)')/col('sum(ingoing_spend_per_day_lc)'))

df = df_1.join(df_2, on=['country_id', 'budget_year'], how = 'left')
df = df.withColumn("actual_spend : ingoing_spend (USD)", col('sum(actual_spend_dollars)')/col('sum(ingoing_spend_per_day_usd)'))

df = df.withColumn("actual_spend : ingoing_spend (LC)", col('sum(actual_spend_local_currency)')/col('sum(ingoing_spend_per_day_lc)'))

df.filter(col('budget_year')<2024).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Extra Checks

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. plan_desc_new is combination of Budget Year _ CountryID _ Product _ Media-type

# COMMAND ----------


df = ih_spend.select(
                            'product_id', 'country_id', 
                            'budget_year', 'media_type', 'plan_desc_new'
                            
).join(dim_product.select(
    'product_id', 'product'
), on ='product_id', how='left')
 
df = df.withColumn('created_plan_name', concat_ws('_', col('budget_year'), col('country_id'), col('product'), col('media_type')))

df.select('created_plan_name', 'plan_desc_new').dropDuplicates().groupBy('created_plan_name').agg(count('plan_desc_new').alias('plan_desc_new_count')).filter(col('plan_desc_new_count')>1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Ingoing table has same ingoing_spend_usd and ingoing_spend_per_day_usd - Issue Solved

# COMMAND ----------



ih_ingoing.groupBy(       'country_id', 'budget_year',
                            'plan_desc_new',
                            'start_date',
                            'end_date').agg(sum('ingoing_spend_lc'),
                                        sum('ingoing_spend_usd'),
                                        sum('ingoing_spend_per_day_lc'),
                                        sum('ingoing_spend_per_day_usd')).filter(
                            col('sum(ingoing_spend_per_day_usd)')==('sum(ingoing_spend_per_day_lc)')).display()

                            



# COMMAND ----------



ih_ingoing.groupBy(       'country_id', 'budget_year',
                            'plan_desc_new',
                            'start_date',
                            'end_date').agg(sum('ingoing_spend_lc'),
                                        sum('ingoing_spend_usd'),
                                        sum('ingoing_spend_per_day_lc'),
                                        sum('ingoing_spend_per_day_usd')).filter(
                            col('sum(ingoing_spend_usd)')==('sum(ingoing_spend_lc)')).display()

                            



# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3. Negative value in ingoing spend local column

# COMMAND ----------

ih_ingoing.filter(col('ingoing_spend_per_day_lc') < 0).display()
ih_ingoing.filter(col('ingoing_spend_lc') < 0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Cost per day = 0, But cost to client is not 0

# COMMAND ----------

ih_spend.filter((col('cost_per_day_lc') == 0) & (col('cost_to_client_lc')!=0)).display()
ih_spend.filter((col('cost_per_day_usd') == 0) & (col('cost_to_client_usd')!=0)).display()

ih_spend.filter((col('cost_to_client_lc') == 0) & (col('cost_per_day_lc')!=0)).display()
ih_spend.filter((col('cost_to_client_usd') == 0) & (col('cost_per_day_usd')!=0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Check for spikes in spend
# MAGIC "AZ", "CO", "EC", "GE", "IQ", "OM", "PE", "PR", "AE", "UY", "US", "SA" countries have same spend (LC & USD)

# COMMAND ----------


df1 = ih_spend.groupBy( 'country_id', 
                 'budget_year').agg(sum('cost_to_client_lc'),
                                            sum('cost_to_client_usd'),
                                            sum('cost_per_day_lc'),
                                            sum('cost_per_day_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd'))
                            


df2 = ih_ingoing.groupBy('country_id', 
                   'budget_year').agg(      sum('ingoing_spend_lc'),
                                            sum('ingoing_spend_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd'))

                            

countries = ["AZ", "CO", "EC", "GE", "IQ", "OM", "PE", "PR", "AE", "UY", "US", "SA"]
df1.filter(col('country_id').isin(countries)).filter(col('budget_year')>=2023).display()
df2.filter(col('country_id').isin(countries)).filter(col('budget_year')>=2023).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Currency conversion ratio (Country X Year Level)

# COMMAND ----------


df_ing = ih_ingoing.filter(col('budget_year')>=2019).groupBy(    'country_id', 'budget_year',
                            ).agg(
                                            sum('ingoing_spend_lc'),
                                            sum('ingoing_spend_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd'))

df_ing = df_ing.withColumn('ingoing_spend_(lc/usd)_ratio', col('sum(ingoing_spend_lc)')/col('sum(ingoing_spend_usd)'))
df_ing = df_ing.withColumn('ingoing_spend_per_day_(lc/usd)_ratio', col('sum(ingoing_spend_per_day_lc)')/col('sum(ingoing_spend_per_day_usd)'))

df_ing = df_ing.withColumn('ratio', col('ingoing_spend_(lc/usd)_ratio')/col('ingoing_spend_per_day_(lc/usd)_ratio'))

# COMMAND ----------

df_ing.filter(col('budget_year') >= 2019).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Ingoing data match with Spend data
# MAGIC

# COMMAND ----------

df1 = ih_spend.groupBy('country_id', 'budget_year').agg(
                                                            
                    sum('ingoing_spend_per_day_lc').alias('ingoing_spend_per_day_lc-spend table'),
                    sum('ingoing_spend_per_day_usd').alias('ingoing_spend_per_day_usd-spend table')
)

df2 = ih_ingoing.groupBy('country_id', 'budget_year').agg(
                                                           
                    sum('ingoing_spend_per_day_lc').alias('ingoing_spend_per_day_lc-ingoing table'),
                    sum('ingoing_spend_per_day_usd').alias('ingoing_spend_per_day_usd-ingoing table')
)


df2 = df2.join(df1, on=['country_id', 'budget_year'], how='inner')
df2 = df2.withColumn('Ratio LC', col('ingoing_spend_per_day_lc-ingoing table')/col('ingoing_spend_per_day_lc-spend table'))
df2 = df2.withColumn('Ratio USD', 
                     col('ingoing_spend_per_day_usd-ingoing table')
                     /col('ingoing_spend_per_day_usd-spend table'))
df2.filter(col('budget_year')>=2019).display()



# COMMAND ----------

ih_spend.groupBy('country_id', 'budget_year',).agg( sum('cost_to_client_lc'),
                                                    sum('cost_to_client_usd'),
                                                    sum('cost_per_day_lc'),
                                                    sum('cost_per_day_usd'),
                                                    sum('ingoing_spend_per_day_lc'),
                                                    sum('ingoing_spend_per_day_usd')).display()


ih_ingoing.groupBy('country_id', 'budget_year').agg(
                                                           
                    sum('ingoing_spend_per_day_lc').alias('ingoing_spend_per_day_lc-ingoing table'),
                    sum('ingoing_spend_per_day_usd').alias('ingoing_spend_per_day_usd-ingoing table')
)

# COMMAND ----------

dim_product.select('segment').dropDuplicates().display()
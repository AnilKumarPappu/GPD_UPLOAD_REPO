# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark as ps
import numpy as np
import pandas as pd

# COMMAND ----------

#username and password to - databricksusername , databrickspassword
# SQL -DEV
# def jdbc_connection(dbtable):
#     url = 'jdbc:sqlserver://marsanalyticsdevsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationdevsqldb'
#     user_name =dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databricksusername')
#     password = dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databrickspassword')
#     df = spark.read \
#     .format('com.microsoft.sqlserver.jdbc.spark') \
#     .option('url',url) \
#     .option('dbtable',dbtable) \
#     .option('user',user_name) \
#     .option('password',password) \
#     .option('authentication','ActiveDirectoryPassword') \
#     .load()
#     return df


# SQL- PROD
def jdbc_connection(dbtable):
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

#Synapse - prod
def jdbc_connection(dbtable):
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

# df_fact_ih_spend = jdbc_connection('mm.vw_gmd_fact_ih_spend')
# df_fact_ih_ingoing_spend = jdbc_connection('mm.vw_gmd_fact_ih_ingoingspend')

# df_fact_ih_spend_3 = jdbc_connection('mm.vw_gpd_fact_ih_spend')
# df_fact_ih_ingoing_spend_3 = jdbc_connection('mm.vw_gpd_fact_ih_ingoingspend')

# #mm_test.vw_gmd_fact_ih_spend
# mm_test.vw_gmd_fact_ih_ingoingspend

# COMMAND ----------

ih_spend_2 = jdbc_connection('mm.vw_gmd_fact_ih_spend')
ih_ingoing_2 = jdbc_connection('mm.vw_gmd_fact_ih_ingoingspend')
dim_product_2 = jdbc_connection('mm.vw_gmd_dim_product')


ih_spend_3 = jdbc_connection('mm.vw_gpd_fact_ih_spend')
ih_ingoing_3 = jdbc_connection('mm.vw_gpd_fact_ih_ingoingspend')
dim_product_3 = jdbc_connection('mm.vw_gpd_dim_product')

dim_calendar = jdbc_connection('mm.vw_dim_calendar')
dim_country = jdbc_connection('mm.vw_gpd_dim_country')

# COMMAND ----------

dim_country.display()

# COMMAND ----------

ih_spend_2 = ih_spend_2.filter(col('budget_year') >= 2019)
ih_ingoing_2 = ih_ingoing_2.filter(col('budget_year') >= 2019)
ih_spend_3 = ih_spend_3.filter(col('budget_year') >= 2019)
ih_ingoing_3 = ih_ingoing_3.filter(col('budget_year') >= 2019)

# COMMAND ----------


path = "october_file_summary.csv"
ih_spend_3 = pd.read_csv(path)
ih_spend_3.reset_index()


# COMMAND ----------

# ih_spend_3.columns
ih_spend_3 = ih_spend_3.drop('Unnamed: 0', axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 to 1 mapping on budget_year_country_id_product_media-type to plan_desc_new

# COMMAND ----------


df = ih_spend_3.select(
                            'product_id', 'country_id', 
                            'budget_year', 'media_type', 'plan_desc_new'
                            
).join(dim_product_3.select(
    'product_id', 'product'
), on ='product_id', how='left')
 


# COMMAND ----------


df = df.withColumn('created_plan_name', concat_ws('_', col('budget_year'), col('country_id'), col('product'), col('media_type')))



# COMMAND ----------

df.select('created_plan_name', 'plan_desc_new').dropDuplicates().groupBy('created_plan_name').agg(count('plan_desc_new').alias('plan_desc_new_count')).filter(col('plan_desc_new_count')>1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingoing table has same ingoing_spend_usd and ingoing_spend_per_day_usd - Issue Solved

# COMMAND ----------

#ih_ingoing_3 = ih_ingoing_3.select().join(dim_product_3, on ='product_id', how='left')


ih_ingoing_3.groupBy(       'country_id', 'budget_year',
                            'plan_desc_new',
                            'start_date',
                            'end_date').agg(sum('ingoing_spend_lc'),
                                        sum('ingoing_spend_usd'),
                                        sum('ingoing_spend_per_day_lc'),
                                        sum('ingoing_spend_per_day_usd')).filter(
                            col('sum(ingoing_spend_per_day_usd)')==('sum(ingoing_spend_per_day_lc)')).display()

                            



# COMMAND ----------

# MAGIC %md
# MAGIC #### Negative value in ingoing spend local column

# COMMAND ----------

ih_ingoing_3.filter(col('ingoing_spend_per_day_lc') < 0).display()
ih_ingoing_3.filter(col('ingoing_spend_lc') < 0).display()

# COMMAND ----------


ih_ingoing_3.filter(col('plan_desc_new') == '2019_US_Twix_TV').display()

# COMMAND ----------

ih_ingoing_3.filter(col('plan_desc_new') == '2019_US_Twix_TV').groupBy(
                            'plan_desc_new',
                            'start_date',
                            'end_date').agg(sum('ingoing_spend_lc'),
                                            sum('ingoing_spend_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cost per day = 0, But cost to client is not 0

# COMMAND ----------

ih_spend_3.columns

# COMMAND ----------

ih_spend_3[(ih_spend_3['Cost to Client per day']==0) & (ih_spend_3['Cost to Client']!=0)]




# COMMAND ----------

ih_spend_3[(ih_spend_3['Cost to Client USD per day']==0) & (ih_spend_3['Cost to Client USD']!=0)]


# COMMAND ----------


ih_spend_3.filter(col('plan_desc_new') == '2023_PE_M&Ms_Online Video').display()


# COMMAND ----------

#ih_ingoing_3 = ih_ingoing_3.select().join(dim_product_3, on ='product_id', how='left')

ih_spend_3.groupBy(         'country_id', 'budget_year',
                            'plan_desc_new',
                            'start_date',
                            'end_date').agg(sum('cost_to_client_lc'),
                                            sum('cost_to_client_usd'),
                                            sum('cost_per_day_lc'),
                                            sum('cost_per_day_usd'),
                                            sum('ingoing_spend_per_day_lc'),
                                            sum('ingoing_spend_per_day_usd')).display()

                            

 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Currency conversion ratio

# COMMAND ----------


# df_ing = ih_ingoing_3.filter(col('budget_year')>=2019).groupBy(    'country_id', 'budget_year',
#                             ).agg(
#                                             sum('ingoing_spend_lc'),
#                                             sum('ingoing_spend_usd'),
#                                             # sum('ingoing_spend_per_day_lc'),
#                                             sum('ingoing_spend_per_day_usd'))

# COMMAND ----------

# df_ing = df_ing.withColumn('ingoing_spend_(lc/usd)_ratio', col('sum(ingoing_spend_lc)')/col('sum(ingoing_spend_usd)'))
# df_ing = df_ing.withColumn('ingoing_spend_per_day_(lc/usd)_ratio', col('sum(ingoing_spend_per_day_lc)')/col('sum(ingoing_spend_per_day_usd)'))



# COMMAND ----------

df_ing.filter(col('budget_year') >= 2019).display()

# COMMAND ----------

ih_spend_3.columns

# COMMAND ----------


# df = ih_spend_3.filter(col('budget_year')>=2019).groupBy(    'country_id', 'budget_year',
#                             ).agg(sum('cost_to_client_lc'),
#                                             sum('cost_to_client_usd'),
#                                             sum('cost_per_day_lc'),
#                                             sum('cost_per_day_usd'),
#                                             sum('ingoing_spend_per_day_lc'),
#                                             sum('ingoing_spend_per_day_usd'))


grouped_df = ih_spend_3.groupby(['Country', 'Budget Year']).agg({
    # 'Cost to Client': 'sum',
    # 'Cost to Client USD': 'sum',
    'Cost to Client per day': 'sum',
    'Cost to Client USD per day': 'sum',
    # 'ingoing_spend_per_day_lc': 'sum',
    # 'ingoing_spend_per_day_usd': 'sum'
}).reset_index()


# COMMAND ----------

grouped_df.columns

# COMMAND ----------

grouped_df['cost_to_client(lc/usd)_ratio'] = grouped_df['Cost to Client'] / grouped_df['Cost to Client USD']
grouped_df['cost_per_day(lc/usd)_ratio'] = grouped_df['Cost to Client per day'] / grouped_df['Cost to Client USD per day']
# df = df.withColumn('ingoing_spend_per_day(lc/usd)_ratio', col('sum(ingoing_spend_per_day_lc)')/col('sum(ingoing_spend_per_day_usd)'))


# COMMAND ----------

grouped_df_19 = grouped_df[grouped_df['Budget Year'] >= 2019]

# COMMAND ----------

grouped_df_19.to_csv('curr_conv.csv')

# COMMAND ----------

ih_spend_3[(ih_spend_3['Budget Year']== 2023) & (ih_spend_3['Country']=='India')]

# COMMAND ----------



# COMMAND ----------

df.filter(col('budget_year') >= 2019).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ----------->>>>> Ingoing data match with Spend data
# MAGIC

# COMMAND ----------

df_fact_ih_spend.groupBy('country_id', 'budget_year').agg(
                                                            sum('cost_to_client_lc'),
                                                            sum('cost_to_client_usd'),
                                                            sum('cost_per_day_lc'),
                                                            sum('cost_per_day_usd'),
                                                            sum('ingoing_spend_per_day_lc'),
                                                            sum('ingoing_spend_per_day_usd')
).display()

df_fact_ih_ingoing_spend.groupBy('country_id', 'budget_year').agg(
                                                            sum('ingoing_spend_lc'),
                                                            sum('ingoing_spend_usd'),
                                                            sum('ingoing_spend_per_day_lc'),
                                                            sum('ingoing_spend_per_day_usd')
).display()




# COMMAND ----------


df_fact_ih_spend_3.groupBy('country_id', 'budget_year').agg(
                                                            sum('cost_to_client_lc'),
                                                            sum('cost_to_client_usd'),
                                                            sum('cost_per_day_lc'),
                                                            sum('cost_per_day_usd'),
                                                            sum('ingoing_spend_per_day_lc'),
                                                            sum('ingoing_spend_per_day_usd')
).display()


df_fact_ih_ingoing_spend_3.groupBy('country_id', 'budget_year').agg(
                                                            sum('ingoing_spend_lc'),
                                                            sum('ingoing_spend_usd'),
                                                            sum('ingoing_spend_per_day_lc'),
                                                            sum('ingoing_spend_per_day_usd')
).display()



# COMMAND ----------

df_planned_spend = jdbc_connection('mm.vw_gmd_fact_plannedspend')
df_fact_performance = jdbc_connection('mm.vw_gmd_fact_performance')
df_dim_country = jdbc_connection('mm.vw_gmd_dim_country')
df_dim_product = jdbc_connection('mm.vw_gmd_dim_product')
df_dim_calendar = jdbc_connection('mm.vw_dim_calendar')
df_reach = jdbc_connection('mm.vw_gmd_fact_reach')
df_dim_campaign = jdbc_connection('mm.vw_gmd_dim_campaign')

# COMMAND ----------

df_dim_country.select('countrydesc', 'country_id').dropDuplicates().display()

# COMMAND ----------

df_dim_product.select('segment').dropDuplicates().display()

# COMMAND ----------

df_dim_country.select('countrydesc', 'country_id').dropDuplicates().display()

# COMMAND ----------

df_fact_ih_spend.display()

# COMMAND ----------

df_fact_ih_spend.select('country_id').dropDuplicates().display()

# COMMAND ----------


access_layer_country  = df_fact_ih_spend.filter(col('start_date') > 20190101).select('country_id').dropDuplicates()

# COMMAND ----------



access_layer_country.select('country_id').display()

# COMMAND ----------

df_fact_ih_spend.filter(col('plan_name') == '2020 Austria Chocolate Snickers Snickers').filter(col('media_type')=='Online Video').display()

# COMMAND ----------

df_fact_ih_spend.join(df_dim_product, on ='product_id', how = 'left').filter(col('start_date')>'20190101').filter(col('segment')=='MW').select('country_id', 'budget_year', 'brand','plan_name', 'media', 'media_type','start_date','end_date', 'cost_to_client_lc','cost_to_client_usd',  'ingoing_spend_per_day_lc', 'ingoing_spend_per_day_usd').distinct().groupby('country_id', 'budget_year', 'brand','plan_name', 'media', 'media_type', 'start_date', 'end_date').agg(sum('cost_to_client_lc'),sum('cost_to_client_usd'), sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

df_fact_ih_spend.join(df_dim_product, on ='product_id', how = 'left').filter((col('start_date')>'20190101') & (col('start_date')<'20230701')).filter(col('segment')=='MW').select('country_id', 'budget_year', 'brand','plan_name', 'media', 'media_type','start_date','end_date', 'cost_to_client_lc','cost_to_client_usd',  'ingoing_spend_per_day_lc', 'ingoing_spend_per_day_usd').distinct().groupby('country_id', 'budget_year', 'brand','plan_name').agg(sum('cost_to_client_lc'),sum('cost_to_client_usd'), sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

df_reach = jdbc_connection('mm.vw_gpd_fact_reach')


# COMMAND ----------

df_reach.display()

# COMMAND ----------

df_reach.groupBy('country_id', 'campaign_desc', 'campaign_platform_start_date','campaign_platform_end_date').agg(sum('in_platform_reach')).display()

# COMMAND ----------


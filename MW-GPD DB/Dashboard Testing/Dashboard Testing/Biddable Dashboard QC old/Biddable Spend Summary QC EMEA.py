# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.column import *

# COMMAND ----------

# url = 'jdbc:sqlserver://marsanalyticsdevsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationdevsqldb'
# dbtable = 'mm_test.vw_gmd_fact_performance'

# COMMAND ----------

def jdbc_connection(dbtable):
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

df_planned_spend = jdbc_connection('mm_test.vw_gmd_fact_plannedspend')
df_performance = jdbc_connection('mm_test.vw_gmd_fact_performance')
df_dim_country = jdbc_connection('mm_test.vw_gmd_dim_country')
df_dim_product = jdbc_connection('mm_test.vw_gmd_dim_product')
df_dim_calendar = jdbc_connection('mm_test.vw_dim_calendar')
df_reach = jdbc_connection('mm_test.vw_gmd_fact_reach')
df_dim_campaign = jdbc_connection('mm_test.vw_gmd_dim_campaign')

# COMMAND ----------

df_performance.select('country_id','channel', 'platform').dropDuplicates().display()

# COMMAND ----------

df_planned_spend = df_planned_spend.join(df_dim_product, on = 'product_id')
df_performance = df_performance.join(df_dim_product, on = 'product_id')

# COMMAND ----------

df_planned_spend.select('plan_line_id', 'planned_budget_dollars').dropDuplicates().agg(sum('planned_budget_dollars')).display()

# COMMAND ----------

df_performance.select('country_id').distinct().display()

# COMMAND ----------

df_performance.filter(~((col('country_id')!= 'IN') & (col('country_id')!= 'AU'))).agg(sum('actual_spend_local_currency'), sum('actual_spend_dollars')).display()

# COMMAND ----------

df_planned_spend.select('country_id',
 'plan_line_id','planned_budget_dollars',
 'planned_budget_local_currency').dropDuplicates().groupBy('country_id').agg(sum('planned_budget_dollars'), sum('planned_budget_local_currency')).display()

# COMMAND ----------

df_performance.groupBy('country_id').agg(sum('actual_spend_local_currency'), sum('actual_spend_dollars')).display()

# COMMAND ----------

df_performance.groupBy('country_id', 'channel').agg(sum('actual_spend_local_currency'), sum('actual_spend_dollars'), min('date')).display()

# COMMAND ----------

df_performance.filter(col('date')>= 20220323).groupBy('country_id', 'channel').agg(sum('actual_spend_local_currency'), sum('actual_spend_dollars'), min('date')).display()

# COMMAND ----------

col1 = ['country_id','product_id','channel','platform','campaign_name','actual_spend_local_currency']
df_actual_spend1 = df_actual_spend.select(col1)
df_actual_spend2 = df_actual_spend1.join(df_dim_product, on = 'product_id', how = 'left').drop('product_id')
df_actual_spend3 = df_actual_spend2.fillna(0, subset='actual_spend_local_currency')
df_actual_spend3 = df_actual_spend3.filter(col('platform') != 'Campaign Manager')

# COMMAND ----------

df_actual_spend4 = df_actual_spend3.groupBy('country_id','brand','channel','platform','campaign_name')\
                    .agg(sum('actual_spend_local_currency').alias('actual_spend_gbp'))

# COMMAND ----------

df_actual_spend4.display()

# COMMAND ----------

df_planned_spend = df_planned_spend.select('plan_line_id','planned_budget_local_currency').dropDuplicates().select(sum('planned_budget_local_currency'))

# COMMAND ----------

df_planned_spend.display()

# COMMAND ----------

df_actual_spend5 = df_actual_spend4.join(df_planned_spend1, on=['country_id','channel','platform','campaign_name'], how = 'left')\
.select('country_id','brand','channel','platform','campaign_name','plan_line_id','actual_spend_gbp','planned_budget_local_currency').dropDuplicates()\
.groupBy('country_id','brand','channel','platform','campaign_name').agg(max('actual_spend_gbp').alias('actual_gbp'),sum('planned_budget_local_currency').alias('planned_gbp'))
df_actual_spend5.display()

# COMMAND ----------

df_actual_spend5.groupBy('platform').sum('actual_gbp','planned_gbp').display()

# COMMAND ----------

df_actual_spend5.groupBy('brand').sum('actual_gbp','planned_gbp').display()

# COMMAND ----------

df_actual_spend5.groupBy('channel').sum('actual_gbp','planned_gbp').display()

# COMMAND ----------

df_actual_spend5.groupBy('country_id').sum('actual_gbp','planned_gbp').display()

# COMMAND ----------



# COMMAND ----------

df_actual_spend5.filter(col('campaign_name')=='GB_Airwaves_W&S Top feed P11_1122').display()

# COMMAND ----------


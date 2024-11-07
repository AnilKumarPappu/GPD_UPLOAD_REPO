# Databricks notebook source
# MAGIC %md
# MAGIC ##Loading Tables

# COMMAND ----------

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

dev_ih_spend = jdbc_connection_prod('mm.vw_gpd_fact_ih_spend')
dev_ih_ingoing = jdbc_connection_prod('mm.vw_gpd_fact_ih_ingoingspend')
dev_dim_country = jdbc_connection_prod('mm.vw_gpd_dim_country')
dev_dim_product = jdbc_connection_prod('mm.vw_gpd_dim_product')
dev_dim_calendar = jdbc_connection_prod('mm.vw_dim_calendar')

# COMMAND ----------

dev_ih_spend = jdbc_connection_dev('mm_test.vw_gmd_fact_ih_spend')
dev_ih_ingoing = jdbc_connection_dev('mm_test.vw_gmd_fact_ih_ingoingspend')
dev_dim_country = jdbc_connection_dev('mm_test.vw_gmd_dim_country')
dev_dim_product = jdbc_connection_dev('mm_test.vw_gmd_dim_product')
dev_dim_calendar = jdbc_connection_dev('mm_test.vw_dim_calendar')
dev_ih_spend = jdbc_connection_prod('mm.vw_gmd_fact_ih_spend')
dev_ih_ingoing = jdbc_connection_prod('mm.vw_gmd_fact_ih_ingoingspend')
dev_dim_country = jdbc_connection_prod('mm.vw_gmd_dim_country')
dev_dim_product = jdbc_connection_prod('mm.vw_gmd_dim_product')
dev_dim_calendar = jdbc_connection_prod('mm.vw_dim_calendar')

# COMMAND ----------

dev_dim_country.display()

# COMMAND ----------

dev_ih_spend.filter(col("country_id")=="SA").groupby('country_id','budget_year').agg(sum('cost_per_day_lc'), sum('cost_per_day_usd')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vieweing Data

# COMMAND ----------

dev_ih_spend.limit(10).display()

# COMMAND ----------

dev_ih_ingoing.limit(10).display()

# COMMAND ----------

#dev_ih_spend.filter(col('budget_year') > '2018').filter("country_id in ('AU','GB','DE','MX','IN','FR','CA','CN','JP','NZ')").select('country_id','budget_year','ingoing_spend_per_day_lc','ingoing_spend_per_day_usd','cost_per_day_lc','cost_per_day_usd').groupby('country_id','budget_year').sum('ingoing_spend_per_day_lc','ingoing_spend_per_day_usd','cost_per_day_lc','cost_per_day_usd').display()

# COMMAND ----------

#dev_ih_spend.filter(col('budget_year') > '2018').filter("country_id not in ('UZ','US')").select('country_id','budget_year','plan_name', 'media_type','cost_per_day_usd','ingoing_spend_per_day_usd', 'cost_per_day_lc','ingoing_spend_per_day_lc').groupby('country_id','budget_year','plan_name', 'media_type').agg(sum('cost_per_day_usd').alias('cost_usd'),sum('ingoing_spend_per_day_usd').alias('ingoing_usd'), sum('cost_per_day_lc').alias('cost_lc'), sum('ingoing_spend_per_day_lc').alias('ingoing_lc')).display()

# COMMAND ----------

dev_dim_product.display()

# COMMAND ----------

dev_dim_country = jdbc_connection_prod('mm.vw_gmd_dim_country')
dev_dim_country.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Checks

# COMMAND ----------

#1: Only numerical values in Budget Year Column
dev_ih_spend.select('budget_year').distinct().display()

# COMMAND ----------

#2: Country should be mapped correctly to Region according to upload app
dev_ih_spend.select('country_id','region').distinct().display()

# COMMAND ----------

#3: Cost to Client and Ingoing Spend should be in Decimal format only. "Char" and "Varchar" would break the dashboard refresh
dev_ih_spend.schema

# COMMAND ----------

ih_spend_sample = dev_ih_spend.filter("country_id not in ('UZ','IQ','JO','MA')").filter(col('budget_year') > 2018)
ih_ingoing_sample = dev_ih_ingoing.filter("country_id not in ('US','UZ','IQ','JO','MA')").filter(col('budget_year') > 2018)

# COMMAND ----------

#4: Each brand should be mapped to correct Cell and Portfolio (no miscellenous values)
#18: All Cell/Portfolio (as per Uptimal mapping) should be present in IH Spend
dev_spend_prod = ih_spend_sample.join(dev_dim_product, on = 'product_id', how = 'inner')
dev_spend_prod.select('country_id','brand','portfolio','strategic_cell').distinct().display()

# COMMAND ----------

#dev_spend_prod.filter(col('brand') == 'PK').select('budget_year','country_id','brand','cost_per_day_lc').groupby('budget_year','brand','country_id').sum('cost_per_day_lc').display()

# COMMAND ----------

#5: In Portfolio, Cell and Brands there should be no repetition of similar values. Ex. Bite Size and Bitesize
dev_spend_prod.select('portfolio').distinct().show()
dev_spend_prod.select('strategic_cell').distinct().show()
dev_spend_prod.select('brand').distinct().display()

# COMMAND ----------

#7: Cost to Client uniformaly divided on daily basis
ih_spend_sample.filter(col('start_date') > col('day')).display()
ih_spend_sample.filter(col('end_date') < col('day')).display() 

# COMMAND ----------

#8: Ingoing Spend uniformaly divided on daily basis
ih_ingoing_sample.filter(col('start_date') > col('day')).display()
ih_ingoing_sample.filter(col('end_date') < col('day')).display() 

# COMMAND ----------

#9: No duplicate rows in Spend Table
#ih_spend_sample.count()#984999 #935542
ih_spend_sample.distinct().count() #same

# COMMAND ----------

#10: No duplicate rows in dim_product
#dev_dim_product.count() #714 #741
print(dev_dim_product.count(),dev_dim_product.distinct().count()) #714

# COMMAND ----------

#11: No duplicate rows in dim_country
#dev_dim_country.count() #86 #85
print(dev_dim_country.count(), dev_dim_country.distinct().count()) #86

# COMMAND ----------

#12: Day column should have consistent date format(No string data) Any issue here would be break dashboard and access layer
ih_spend_sample.select('day').distinct().display()

# COMMAND ----------

#14: All prod_id in IH Spend should be present in dim_product
spend = ih_spend_sample.select(col('product_id').alias('prod_spend'))
dev_dim_product.select('product_id').distinct().join(spend.distinct(), dev_dim_product.product_id == spend.prod_spend, how = 'outer').display()


# COMMAND ----------

#16: Check if all countries have data till latest year.
#19: Ingoing Spend data should be present for last 5 years.
#20: Actual Spend data should be present for last 5 years.
ih_spend_sample.select('country_id','budget_year').groupby('country_id').agg(max('budget_year').alias('max_year'),min('budget_year').alias('min_year')).display()
#ih_ingoing_sample.select('country_id','budget_year').groupby('country_id').agg(max('budget_year').alias('max_year'),min('budget_year').alias('min_year')).display()

# COMMAND ----------

dev_ih_spend.select('country_id','start_date').groupby('country_id').agg(max('start_date').alias('max_date')).display()


# COMMAND ----------

#17: Total country count > 75
ih_spend_sample.select('country_id').distinct().display() #69
#dev_ih_ingoing.select('country_id').distinct().count() #69

# COMMAND ----------

#21: Ingoing LC should not be 0 if Ingoing USD present & vice versa
ih_spend_sample.filter((col('ingoing_spend_per_day_usd').isNull()) & (col('ingoing_spend_per_day_lc').isNotNull())).display()
ih_spend_sample.filter((col('ingoing_spend_per_day_usd') == 0) & (col('ingoing_spend_per_day_lc') != 0)).display()
ih_spend_sample.filter((col('ingoing_spend_per_day_lc').isNull()) & (col('ingoing_spend_per_day_usd').isNotNull())).display()
ih_spend_sample.filter((col('ingoing_spend_per_day_lc') == 0) & (col('ingoing_spend_per_day_usd') != 0)).display()

# COMMAND ----------

#22: Cost LC should not be 0 if Cost USD present & vice versa
ih_spend_sample.filter((col('cost_per_day_usd').isNull()) & (col('cost_per_day_lc').isNotNull())).display()
ih_spend_sample.filter((col('cost_per_day_usd') == 0) & (col('cost_per_day_lc') != 0)).display()
ih_spend_sample.filter((col('cost_per_day_lc').isNull()) & (col('cost_per_day_usd').isNotNull())).display()
ih_spend_sample.filter((col('cost_per_day_lc') == 0) & (col('cost_per_day_usd') != 0)).display()


# COMMAND ----------

#23: Plan name in spend table but not in ingoing
temp = ih_spend_sample.filter(col('country_id') != 'US').select(col('plan_name').alias('plan_name_spend'))
ih_ingoing_sample.select('plan_name').distinct().join(temp.distinct(), ih_ingoing_sample.plan_name == temp.plan_name_spend, how = 'outer').display()

# COMMAND ----------

#24: Channel Mix and Media Type don't have null values.
ih_spend_sample.select('channel_mix','media_type').distinct().display() #63 rows

# COMMAND ----------

#25: Variance within threshold b/w actual and ingoing spend (30-120%)
ih_spend_sample.select('country_id','budget_year','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('country_id','budget_year').agg(sum('cost_per_day_usd').alias('cost'),sum('ingoing_spend_per_day_usd').alias('spend')).withColumn('variance',((col('cost') - col('spend'))/col('spend'))*100).display()

# COMMAND ----------

#26
temp_c = dev_ih_spend.select(col('country_id').alias('country_id_synth'))
dev_dim_country.select('country_id','countrydesc').distinct().join(temp_c.distinct(), dev_dim_country.country_id == temp_c.country_id_synth, how = 'outer').display()

# COMMAND ----------

dev_ih_spend.select('country_id','budget_year','cost_per_day_usd','cost_per_day_lc','ingoing_spend_per_day_usd','ingoing_spend_per_day_lc').groupBy('country_id','budget_year').agg(sum('cost_per_day_usd').alias('cost_per_day_usd'),sum('cost_per_day_lc').alias('cost_per_day_lc'), sum('ingoing_spend_per_day_usd').alias('ingoing_spend_per_day_usd'), sum('ingoing_spend_per_day_lc').alias('ingoing_spend_per_day_lc')).withColumn('ingoing_ratio', col('ingoing_spend_per_day_lc')/col('ingoing_spend_per_day_usd')).withColumn('spend_ratio', col('cost_per_day_lc')/col('cost_per_day_usd')).withColumn('actual_ratio', col('ingoing_ratio') / col('spend_ratio')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Data Operations

# COMMAND ----------

ih_spend_sample.select('country_id','budget_year','cost_per_day_lc','cost_per_day_usd').groupby('country_id','budget_year').agg(sum('cost_per_day_lc'),sum('cost_per_day_usd')).display()

# COMMAND ----------

dev_spend_prod.select('country_id','portfolio','cost_per_day_lc','cost_per_day_usd').groupby('country_id','portfolio').agg(sum('cost_per_day_lc'),sum('cost_per_day_usd')).display()

# COMMAND ----------

dev_spend_prod.select('country_id','channel_mix','cost_per_day_lc','cost_per_day_usd').groupby('country_id','channel_mix').agg(sum('cost_per_day_lc'),sum('cost_per_day_usd')).display()

# COMMAND ----------

dev_ih_spend.filter(col('budget_year') > '2018').groupby('country_id','budget_year').agg(sum('ingoing_spend_per_day_lc'), sum('ingoing_spend_per_day_usd'), sum('cost_per_day_lc'), sum('cost_per_day_usd')).display()

# COMMAND ----------

ih_spend_sample.select('country_id','budget_year','ingoing_spend_per_day_lc','ingoing_spend_per_day_usd').groupby('country_id','budget_year').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).filter(col('sum(ingoing_spend_per_day_usd)') == 0).display()

# COMMAND ----------

dev_ih_spend.filter(col('budget_year') > '2018').select('country_id','budget_year','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('country_id','budget_year').agg(sum('cost_per_day_usd').alias('cost'),sum('ingoing_spend_per_day_usd').alias('spend')).withColumn('variance',((col('cost') - col('spend'))/col('spend'))*100).display()

# COMMAND ----------

ih_spend_sample.select('country_id','budget_year','cost_per_day_lc','cost_per_day_usd').groupby('country_id','budget_year').sum('cost_per_day_lc','cost_per_day_usd').display()

# COMMAND ----------

ih_spend_sample.select('country_id','budget_year','ingoing_spend_per_day_lc','ingoing_spend_per_day_usd').filter((col('ingoing_spend_per_day_usd') == 0) & (col('ingoing_spend_per_day_lc') != 0)).groupby('country_id','budget_year').sum('ingoing_spend_per_day_lc','ingoing_spend_per_day_usd').display()

# COMMAND ----------

dev_ih_spend.filter(col('country_id') != 'UZ').select('budget_year','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('budget_year').agg(sum('cost_per_day_usd'),sum('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

#dev_ih_spend.filter(col('plan_name') == 'Charlie Test copy Approvals').display()

# COMMAND ----------

#16-B
#20-B
dev_ih_spend.select('country_id','budget_year').groupby('country_id').agg(max('budget_year').alias('max_year'),min('budget_year').alias('min_year')).display()

# COMMAND ----------

#21-B: Ingoing LC should not be 0 if Ingoing USD present & vice versa
dev_ih_spend.filter((col('ingoing_spend_per_day_usd').isNull()) & (col('ingoing_spend_per_day_lc').isNotNull())).display()
dev_ih_spend.filter((col('ingoing_spend_per_day_usd') == 0) & (col('ingoing_spend_per_day_lc') != 0)).display()
dev_ih_spend.filter((col('ingoing_spend_per_day_lc').isNull()) & (col('ingoing_spend_per_day_usd').isNotNull())).display()
dev_ih_spend.filter((col('ingoing_spend_per_day_lc') == 0) & (col('ingoing_spend_per_day_usd') != 0)).display()

# COMMAND ----------

#22-B: Cost LC should not be 0 if Cost USD present & vice versa
dev_ih_spend.filter((col('cost_per_day_usd').isNull()) & (col('cost_per_day_lc').isNotNull())).display()
dev_ih_spend.filter((col('cost_per_day_usd') == 0) & (col('cost_per_day_lc') != 0)).display()
dev_ih_spend.filter((col('cost_per_day_lc').isNull()) & (col('cost_per_day_usd').isNotNull())).display()
dev_ih_spend.filter((col('cost_per_day_lc') == 0) & (col('cost_per_day_usd') != 0)).display()

# COMMAND ----------

#23-B: Plan name in spend table but not in ingoing
temp_b = dev_ih_spend.select(col('plan_name').alias('plan_name_spend'))
temp_c = dev_ih_ingoing.select('plan_name').distinct().join(temp_b.distinct(), dev_ih_ingoing.plan_name == temp_b.plan_name_spend, how = 'outer')

# COMMAND ----------

temp_c.filter(col('plan_name').isNull()).select('plan_name_spend').join(dev_ih_spend, temp_c.plan_name_spend == dev_ih_spend.plan_name, how = 'inner').select('country_id','region','budget_year','plan_name','channel_mix','media','media_type','start_date','end_date', 'cost_to_client_lc','cost_to_client_usd').filter(col('budget_year') > '2018').dropDuplicates().display()

# COMMAND ----------

dev_ih_spend.select('country_id').distinct().join(dev_dim_country.select('country_id'),on='country_id',how = 'outer').display()

# COMMAND ----------

dev_spend_prod = dev_ih_spend.join(dev_dim_product, on = 'product_id', how = 'inner')
dev_spend_prod.select('country_id','brand','portfolio','strategic_cell').distinct().display()

# COMMAND ----------

dev_ih_spend.select('country_id','region').distinct().display()

# COMMAND ----------

ih_spend_sample = dev_ih_spend.filter("country_id not in ('US','UZ','IQ','JO','MA')").filter(col('budget_year') > 2018)

# COMMAND ----------

dev_spend_prod = ih_spend_sample.join(dev_dim_product, on = 'product_id', how = 'inner')
prod_details = dev_spend_prod.select('country_id','brand','product').distinct()

# COMMAND ----------

prod_details.filter(col('brand') != col('product')).display()

# COMMAND ----------

prod_details.select('product').distinct().count(), prod_details.select('brand').distinct().count()

# COMMAND ----------

prod_details.display()

# COMMAND ----------

ih_spend_sample.filter(col('budget_year') < '2023').select('country_id','budget_year','plan_name','start_date','end_date','cost_per_day_usd','ingoing_spend_per_day_usd').groupby('country_id','budget_year','plan_name','start_date','end_date').agg(sum('cost_per_day_usd').alias('cost_usd'),sum('ingoing_spend_per_day_usd').alias('ingoing_usd')).filter(col('ingoing_usd') != col('cost_usd')).display()

# COMMAND ----------

ih_spend_sample.filter(col('budget_year') < '2023').select('country_id','budget_year','plan_name','start_date','end_date','cost_per_day_lc','ingoing_spend_per_day_lc').groupby('country_id','budget_year','plan_name','start_date','end_date').agg(sum('cost_per_day_lc').alias('cost_lc'),sum('ingoing_spend_per_day_lc').alias('ingoing_lc')).filter(col('ingoing_lc') != col('cost_lc')).display()

# COMMAND ----------

ih_spend_sample.filter(col('budget_year') < '2023').filter(col('ingoing_spend_per_day_lc') != col('cost_per_day_lc')).display()

# COMMAND ----------

dev_ih_spend.filter(col('country_id') == 'US').filter(col('start_date') >= 20230501).select('plan_name').distinct().display()

# COMMAND ----------

dev_ih_ingoing.filter(col('country_id') == 'US').filter(col('budget_year') == 2023).select('plan_name').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## New Plan Name Test

# COMMAND ----------

spend_new = dev_ih_spend.filter("country_id in ('US','GB')").filter(col('budget_year') > 2018)
ingoing_new = dev_ih_ingoing.filter("country_id in ('US','GB')").filter(col('budget_year') > 2018)
#spend_new.display()

# COMMAND ----------

spend_new = spend_new.join(dev_dim_product, on = 'product_id', how = 'inner')
#spend_new = spend_new.withColumn("new_plan_spend", concat_ws(' ', col('budget_year'), col('country_id'), col('product'), col('media_type')))
spend_new = spend_new.withColumn("new_plan_spend", concat_ws(' ', col('budget_year'), col('country_id'), col('product'), col('media_type'), col('start_date'),col('end_date')))
#spend_new.display()

# COMMAND ----------

#spend_new.select('plan_name').distinct().count() #116
#spend_new.select('new_plan_spend').distinct().count() #753
#spend_new.select('cost_to_client_lc').distinct().count() #3358
#spend_new.select('new_plan_spend','start_date','end_date').distinct().count() #4185

# COMMAND ----------

#spend_new = spend_new.select('country_id','budget_year','region','new_plan_spend','brand','product','channel_mix','media','media_type','cost_to_client_lc','cost_to_client_usd').distinct()
#spend_new = spend_new.groupby('country_id','budget_year','region','new_plan_spend','brand','product','channel_mix','media','media_type').agg(sum('cost_to_client_lc').alias('cost_to_client_lc'),sum('cost_to_client_usd').alias('cost_to_client_usd'))
spend_new = spend_new.select('country_id','budget_year','region','new_plan_spend','brand','product','channel_mix','media','media_type','start_date','end_date','cost_to_client_lc','cost_to_client_usd').distinct()
spend_new = spend_new.groupby('country_id','budget_year','region','new_plan_spend','brand','product','channel_mix','media','media_type','start_date','end_date').agg(sum('cost_to_client_lc').alias('cost_to_client_lc'),sum('cost_to_client_usd').alias('cost_to_client_usd'))
#spend_new.display()

# COMMAND ----------

ingoing_new = ingoing_new.join(dev_dim_product, on = 'product_id', how = 'inner')
#ingoing_new = ingoing_new.withColumn("new_plan_ingoing", concat_ws(' ', col('budget_year'), col('country_id'), col('product'), col('media_type')))
#ingoing_new = ingoing_new.select('country_id','budget_year','region','new_plan_ingoing','brand','product','channel_mix','media','media_type','ingoing_spend_lc','ingoing_spend_usd').distinct()
ingoing_new = ingoing_new.withColumn("new_plan_ingoing", concat_ws(' ', col('budget_year'), col('country_id'), col('product'), col('media_type'),col('start_date'),col('end_date')))
ingoing_new = ingoing_new.select('country_id','budget_year','region','new_plan_ingoing','brand','product','channel_mix','media','media_type','start_date','end_date','ingoing_spend_lc','ingoing_spend_usd').distinct()
#ingoing_new.display()

# COMMAND ----------

df = spend_new.join(ingoing_new.groupby('new_plan_ingoing').agg(sum('ingoing_spend_lc').alias('ingoing_spend_lc'),sum('ingoing_spend_usd').alias('ingoing_spend_usd')), spend_new.new_plan_spend == ingoing_new.new_plan_ingoing, how = 'inner')
#df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.distinct().groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd'),sum('cost_to_client_lc'),sum('cost_to_client_usd')).display()

# COMMAND ----------

spend_new.groupby('country_id','budget_year').agg(sum('cost_to_client_lc'),sum('cost_to_client_usd')).display()

# COMMAND ----------

ingoing_new.groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()
ingoing_new.drop_duplicates().groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()


# COMMAND ----------

ingoing_new.select('country_id','budget_year','media_type','start_date','end_date','ingoing_spend_lc').drop_duplicates().groupby('country_id','budget_year').agg(sum('ingoing_spend_lc')).display()

# COMMAND ----------

dev_ih_ingoing.display()

# COMMAND ----------

dev_ih_ingoing.groupby('country_id','budget_year').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

dev_ih_ingoing.select('country_id','budget_year','product_id','plan_name','channel_mix','media','media_type','start_date','end_date','ingoing_spend_lc','ingoing_spend_usd').distinct().groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()

# COMMAND ----------

dev_ih_ingoing.filter(col('country_id') == 'GB').filter(col('budget_year') == '2023').select('country_id','budget_year',
                                                                                             'plan_name','channel_mix','media','media_type','start_date','end_date','ingoing_spend_lc','ingoing_spend_usd').distinct().groupby('plan_name').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()

# COMMAND ----------

dev_ih_spend.filter(col('country_id') == 'GB').filter(col('budget_year') == '2023').groupby('plan_name').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd'), sum('cost_per_day_lc'), sum('cost_per_day_usd')).display()

# COMMAND ----------

dev_ih_ingoing.select('country_id','budget_year','product_id','version_date','plan_name','media','media_type','start_date','end_date','ingoing_spend_lc','ingoing_spend_usd').distinct().groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()

# COMMAND ----------

ingoing_new.filter(col('country_id') == 'PL').filter(col('budget_year') == '2022').groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()

# COMMAND ----------

dev_ih_ingoing.filter(col('country_id') == 'PL').filter(col('budget_year') == '2022').groupby('country_id','budget_year','plan_name').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()
dev_ih_spend.filter(col('country_id') == 'PL').filter(col('budget_year') == '2022').groupby('country_id','budget_year','plan_name').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

dev_ih_ingoing.filter(col('country_id') == 'MX').filter(col('budget_year') == '2022').groupby('country_id','budget_year','plan_name').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()
dev_ih_spend.filter(col('country_id') == 'MX').filter(col('budget_year') == '2022').groupby('country_id','budget_year','plan_name').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()

# COMMAND ----------

dev_ih_ingoing.filter(col('country_id') == 'MX').filter(col('budget_year') == '2022').groupby('country_id','budget_year').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()
dev_ih_spend.filter(col('country_id') == 'MX').filter(col('budget_year') == '2022').groupby('country_id','budget_year').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()
ingoing_new.filter(col('country_id') == 'MX').filter(col('budget_year') == '2022').groupby('country_id','budget_year').agg(sum('ingoing_spend_lc'),sum('ingoing_spend_usd')).display()

# COMMAND ----------

dev_ih_spend.filter(col('plan_name') == '2023 United Kingdom Chocolate Twix Twix').display()
dev_ih_ingoing.filter(col('plan_name') == '2023 United Kingdom Chocolate Twix Twix').display()


# COMMAND ----------

##Conclusion: Wrong USD values in ingoing_spend_per_day_USD in IH_Ingoing_Table due to wrong conversion logic/rates (probably)

# COMMAND ----------

dev_ih_spend.filter(col('plan_name') == '2023 United Kingdom Chocolate Twix Twix').filter(col('day') == '20230129').filter(col('channel_mix') == 'Digital').filter(col('media') == 'Video').groupby('media_type').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()
dev_ih_ingoing.filter(col('plan_name') == '2023 United Kingdom Chocolate Twix Twix').filter(col('day') == '20230129').filter(col('channel_mix') == 'Digital').filter(col('media') == 'Video').groupby('media_type').agg(sum('ingoing_spend_per_day_lc'),sum('ingoing_spend_per_day_usd')).display()


# COMMAND ----------

#To check
#1. Can we use Year instead of start and end dates?
#2. Is there any flight that is identical but with diff. start and end dates?


# COMMAND ----------

dev_ih_spend.filter(col('plan_name') == '2022 Australia Wrigley Extra Extra Gum').select('plan_name','media','media_type','start_date','end_date','cost_to_client_lc','cost_to_client_usd').distinct().display()

# COMMAND ----------

dev_ih_spend.join(dev_dim_product, on ='product_id', how = 'left').filter(col('start_date')>'20190101').filter(col('segment')=='MW').select('country_id', 'budget_year', 'brand','plan_name', 'media', 'media_type','start_date','end_date', 'cost_to_client_lc','cost_to_client_usd').distinct().groupby('country_id', 'budget_year', 'brand','plan_name', 'media', 'media_type').agg(min('start_date'),max('end_date'),sum('cost_to_client_lc'),sum('cost_to_client_usd')).display()

# COMMAND ----------


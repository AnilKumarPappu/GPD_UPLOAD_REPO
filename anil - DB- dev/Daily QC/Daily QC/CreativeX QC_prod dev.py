# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
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

# #Dev Tables

dim_campaign = jdbc_connection('mm_test.vw_mw_gpd_dim_campaign')
dim_channel = jdbc_connection('mm_test.vw_mw_gpd_dim_channel')
dim_creative = jdbc_connection('mm_test.vw_mw_gpd_dim_creative')
dim_strategy = jdbc_connection('mm_test.vw_mw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection('mm_test.vw_mw_gpd_dim_mediabuy')
dim_site = jdbc_connection('mm_test.vw_mw_gpd_dim_site')
dim_country = jdbc_connection('mm_test.vw_mw_gpd_dim_country')
dim_product = jdbc_connection('mm_test.vw_mw_gpd_dim_product')
fact_creativeX = jdbc_connection( "mm_test.vw_mw_gpd_fact_creativetest")



# COMMAND ----------

dim_creative = dim_creative.filter(col("VendorID") == "CREATIVEX")
dim_channel = dim_channel.filter(col("VendorID") == "CREATIVEX")
dim_campaign = dim_campaign.filter(col("VendorID") == "CREATIVEX")
dim_product = dim_product.filter(col("VendorID") == "CREATIVEX")
# dim_strategy = dim_strategy.filter(col("VendorID") == "CREATIVEX")
# dim_mediabuy = dim_mediabuy.filter(col("VendorID") == "CREATIVEX")
# dim_site = dim_site.filter(col("VendorID") == "CREATIVEX")

# COMMAND ----------


print('fact_creativeX--->', fact_creativeX.count())
print('dim_campaign--->', dim_campaign.count())
print('dim_channel--->', dim_channel.count())
print('dim_strategy--->',dim_strategy.count())
print('dim_mediabuy--->', dim_mediabuy.count())
print('dim_site--->', dim_site.count())
print('dim_creative--->' ,dim_creative.count())
print('dim_country--->',dim_country.count())
print('dim_product--->',dim_product.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if the primary keys are valid in dim tables & their count
# MAGIC --> Since the tables are following star schema, it's important to check if the primary keys are holding correctly.

# COMMAND ----------

dim_product.groupBy("product_id").count().filter(col("count") > 1).join(
    dim_product, "product_id"
).display()
dim_campaign.groupBy("gpd_campaign_id").count().filter(col("count") > 1).join(
    dim_campaign, "gpd_campaign_id"
).display()
dim_creative.groupBy("creative_id").count().filter(col("count") > 1).join(
    dim_creative, "creative_id"
).display()
dim_channel.groupBy("channel_id").count().filter(col("count") > 1).join(
    dim_channel, "channel_id"
).display()
dim_mediabuy.groupBy("media_buy_id").count().filter(col("count") > 1).join(
    dim_mediabuy, "media_buy_id"
).display()
dim_site.groupBy("site_id").count().filter(col("count") > 1).join(
    dim_site, "site_id"
).display()
dim_strategy.groupBy("strategy_id").count().filter(col("count") > 1).join(
    dim_strategy, "strategy_id"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking granularity of fact_creativeX

# COMMAND ----------

fact_creativeX.groupBy("creative_id","guideline_id").count().filter(col("count")>1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### #No of countries in creative X

# COMMAND ----------

fact_creativeX.select("country_id").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Latest data available according to country

# COMMAND ----------

fact_creativeX.groupBy("country_id").agg(
    min("date_id"), max("date_id"), count("date_id")
).display()

# COMMAND ----------


df_1 = fact_creativeX.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")


df_2 = fact_creativeX.withColumn('Year', year('date_id'))
df_2 = df_2.groupBy('country_id', 'Year').agg(count('date_id'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### There should not be any null values in all Tables

# COMMAND ----------

# checcking Null values
dim_creative.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_creative.columns]).display()
dim_channel.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_channel.columns]).display()
dim_campaign.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_campaign.columns]).display()
dim_product.select([count(when(col(c).isNull(), c)).alias(c) for c in dim_product.columns]).display()


# COMMAND ----------

# checcking NA values
dim_creative.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_creative.columns]).display()
dim_channel.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_channel.columns]).display()
dim_campaign.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_campaign.columns]).display()
dim_product.select([count(when(col(c)== "NA", c)).alias(c) for c in dim_product.columns]).display()


# COMMAND ----------

dim_campaign.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### New primary key logic (VendorID + Previous PK) should stand true.

# COMMAND ----------

dim_channel.withColumn("new_id", concat(col("vendor_id"),lit("_"), col("country_id"),lit("_"), col("channel_code")) ).filter(col("new_id") != col("channel_id")).display()


# COMMAND ----------

dim_campaign.withColumn("new_id", concat(col("vendor_id"),lit("_"), col("country_id"),lit("_"), col("gpd_campaign_code")) ).filter(col("new_id") != col("gpd_campaign_id")).display()

# COMMAND ----------

dim_creative.withColumn("new_id", concat(col("vendor_id"),lit("_"), col("country_id"),lit("_"), col("creative_code")) ).filter(col("new_id") != col("creative_id")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Guideline value should be between 0 and 1

# COMMAND ----------

fact_creativeX.filter(~((col('guideline_value') <= 1) &(col("guideline_value") >= 0))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### creative_quality_score value should be between 0 and 1

# COMMAND ----------

fact_creativeX.filter(~((col("creative_quality_score") <= 1) &(col("creative_quality_score") >= 0))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### creative_quality_tier should be one of those values

# COMMAND ----------

# dim_creative.select("creative_quality_tier").distinct().display()
dim_creative.filter(~col("creative_quality_tier").isin(['Excellent','On Track','Needs Work','NA'])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check whether all Id s in fact table are in corresponding dim table

# COMMAND ----------

fact_creativeX.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').select("gpd_campaign_id").distinct().display()
fact_creativeX.join(dim_product, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
fact_creativeX.join(dim_channel, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()
fact_creativeX.join(dim_creative, on = "creative_id", how = 'left_anti').select("creative_id").distinct().display()


# COMMAND ----------

# fact_creativeX.filter(col('product_id')== 'CREATIVEX_US_MW_57').display()
dim_product.filter(col('product_id')== 'CREATIVEX_US_MW_57').display()
# dim_product.count()

# COMMAND ----------

# fact_creativeX.filter(col('product_id').isin(['CREATIVEX_DE_MW_55','CREATIVEX_US_MW_55'])).select(col('gpd_campaign_id')).distinct().display()
dim_campaign.filter(col('gpd_campaign_id').isin(['CREATIVEX_DE_379',
'CREATIVEX_DE_383',
'CREATIVEX_US_1231',
'CREATIVEX_US_1098',
'CREATIVEX_DE_542',
'CREATIVEX_DE_545',
'CREATIVEX_DE_382',
'CREATIVEX_DE_378',
'CREATIVEX_DE_541',
'CREATIVEX_DE_540',
'CREATIVEX_US_1232',
'CREATIVEX_US_1097',
'CREATIVEX_DE_543',
'CREATIVEX_DE_544',
'CREATIVEX_DE_381',
'CREATIVEX_DE_380'])).display()
# fact_creativeX.filter(col('product_id').isin(['CREATIVEX_DE_MW_55','CREATIVEX_US_MW_55'])).display()



# COMMAND ----------

# dim_product.filter(col("product_id")=="CREATIVEX_GB_MW_49").display()
# pro = dim_product.select(col("product_id")).distinct()
# fact = fact_creativeX.select(col("product_id")).distinct()
# fact.subtract(pro).display()

# COMMAND ----------


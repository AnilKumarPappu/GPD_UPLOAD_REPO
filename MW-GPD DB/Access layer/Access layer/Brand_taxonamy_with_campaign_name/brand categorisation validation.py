# Databricks notebook source
# MAGIC %md
# MAGIC ## Importing libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load tables

# COMMAND ----------

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

df_reach = jdbc_connection('mm.vw_mw_gpd_fact_reach')
df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')
df_planned_spend = jdbc_connection('mm.vw_mw_gpd_fact_plannedspend')
dim_campaign = jdbc_connection('mm.vw_mw_gpd_dim_campaign')
dim_channel = jdbc_connection('mm.vw_mw_gpd_dim_channel')
dim_creative = jdbc_connection('mm.vw_mw_gpd_dim_creative')
dim_strategy = jdbc_connection('mm.vw_mw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection('mm.vw_mw_gpd_dim_mediabuy')
dim_site = jdbc_connection('mm.vw_mw_gpd_dim_site')
dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')

# COMMAND ----------

dim_creative.display()

# COMMAND ----------

dim_campaign.join(df_fact_performance.withColumn("year", substring("date", 1, 4)).filter(col("year").isin(['2024'])), on = "gpd_campaign_id", how = "inner").display()

# COMMAND ----------

df_fact_performance.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Brand categorisation main code

# COMMAND ----------

# Fact_performance
df_fact_performance_23_24 = df_fact_performance.withColumn("year", substring("date", 1, 4)).filter(col("year").isin(['2023','2024']))

df_fact_performance_23_24.join(dim_country.select("country_id","marketregion_code"), on = "country_id", how = "left")\
.join(dim_product.select("product_id","brand","product"), on = "product_id", how = "left")\
.join(dim_channel.select("channel_id","channel_desc","platform_desc"), on = "channel_id", how = "left")\
.join(dim_campaign.select("gpd_campaign_id","gpd_campaign_code","campaign_subproduct","media_channel",'naming_convention_flag'), on = "gpd_campaign_id" , how = "inner")\
.join(dim_creative.select("creative_id","creative_subproduct"), on = "creative_id", how= "left").select("year","marketregion_code","country_id","product","brand","channel_desc","platform_desc","gpd_campaign_id","gpd_campaign_code","campaign_desc","creative_subproduct","campaign_start_date","campaign_end_date","campaign_subproduct","media_channel",'naming_convention_flag' ).dropDuplicates().join(df_reach.select('gpd_campaign_id','in_platform_reach'), on = 'gpd_campaign_id', how = 'left').display()
# .join(dim_campaign.select("gpd_campaign_id","gpd_campaign_code"), on = "gpd_campaign_id" , how = "inner")
# nl_mal_core-p2_mw_eur_cho_brand_social_awareness_bid-nadr_0224_#160392

# COMMAND ----------

# Reach
df_reach.join(dim_country.select("country_id","marketregion_code"), on = "country_id", how = "left").join(dim_product.select("product_id","brand"), on = "product_id", how = "left").join(dim_channel.select("channel_id","channel_desc","platform_desc"), on = "channel_id", how = "inner").groupBy("marketregion_code","country_id","brand","channel_desc","platform_desc","campaign_desc", "campaign_platform_start_date","campaign_platform_end_date").agg(sum("in_platform_reach").alias("in_platform_reach")).display()
# .withColumn("campaign_status", when(col("campaign_platform_end_date").cast("int") >= 20240214, "Ongoing").otherwise("Finished") )

# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

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

dim_country.display()

# COMMAND ----------



# COMMAND ----------

curr_conv_values = spark.table('contry_currency_conversion')

# COMMAND ----------

curr_conv_values= curr_conv_values.withColumnRenamed("Countries\xa0▾", "country_desc")
curr_conv_values = curr_conv_values.withColumn("country_desc", upper(curr_conv_values["country_desc"]))

# COMMAND ----------

df_planned_spend1 = (
    df_planned_spend.select(
        "country_id",
        "plan_line_id",
        "planned_budget_dollars",
        "planned_budget_local_currency",
    )
    .dropDuplicates()
    .groupBy("country_id")
    .agg(
        sum("planned_budget_local_currency").alias("planned_budget_local_currency"),
        sum("planned_budget_dollars").alias("planned_budget_dollars"),
    )
    .withColumn(
        "planned_ratio",
        col("planned_budget_local_currency") / col("planned_budget_dollars"),
    )
)

df_fact_performance1 = (
    df_fact_performance.groupBy("country_id")
    .agg(
        sum("actual_spend_local_currency").alias("actual_spends_local_currency"),
        sum("actual_spend_dollars").alias("actual_spends_dollars"),
    )
    .withColumn(
        "actual_ratio",
        col("actual_spends_local_currency") / col("actual_spends_dollars"),
    )
)

ratio = df_fact_performance1.join(df_planned_spend1, on = "country_id", how = 'left').withColumn(
    "ratio", col("planned_ratio") / col("actual_ratio")
)

# COMMAND ----------

ratio = ratio.join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left')

# COMMAND ----------

ratio.join(curr_conv_values, on = 'country_desc', how = 'left').select('country_desc','actual_ratio','planned_ratio','Latest value','ratio').display()

# COMMAND ----------

df_planned_spend.join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('planned_budget_dollars') == col('planned_budget_local_currency')).groupBy('country_desc').count().display()
df_planned_spend.join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').groupBy('country_desc').count().display()
# country_list = df_planned_spend.join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('planned_budget_dollars') == col('planned_budget_local_currency')).select('country_desc').distinct()


# COMMAND ----------

actual_spend_campaign_level = df_fact_performance.groupBy('gpd_campaign_id').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'), sum('actual_spend_local_currency').alias('actual_spend_local_currency'))

# COMMAND ----------

df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'POLAND').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'ECUADOR').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'EGYPT').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'CHILE').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'PHILIPPINES').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'PERU').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'SAUDI ARABIA').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'UNITED ARAB EMIRATES').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'COSTA RICA').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()
df_planned_spend.select('country_id','gpd_campaign_id','campaign_desc','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency').join(dim_country.select('country_id','country_desc'), on = 'country_id', how = 'left').filter(col('country_desc') == 'NEW ZEALAND').drop_duplicates().join(actual_spend_campaign_level, on = 'gpd_campaign_id', how = 'left').display()

# .filter(col('planned_budget_dollars') == col('planned_budget_local_currency')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPID count
# MAGIC

# COMMAND ----------

# Get creative name and media buy name of distinct campaigns
# year 2023 and 2024


df = df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year').isin(['2023','2024'])).select(
    "country_id", "gpd_campaign_id", "creative_id", "media_buy_id"
).distinct()
df = (
    df.join(
        dim_campaign.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
    )
    .join(dim_creative.withColumnRenamed("country_id", "country_id2"), "creative_id")
    .join(dim_mediabuy.withColumnRenamed("country_id", "country_id3"), "media_buy_id")
    .select("country_id", "campaign_desc", "creative_desc", "media_buy_desc")
    .distinct()
)

# Extract OPID values from 'creative_name' and 'media_buy_name' columns
df1 = df.withColumn(
    "OPID1",
    when(
        regexp_extract("creative_desc", "(?:.*OPID-)([0-9]+)", 1) != "",
        regexp_extract("creative_desc", "(?:.*OPID-)([0-9]+)", 1),
    ).otherwise(None),
)
df1 = df1.withColumn(
    "OPID2",
    when(
        regexp_extract("media_buy_desc", "(?:.*OPID-)([0-9]+)", 1) != "",
        regexp_extract("media_buy_desc", "(?:.*OPID-)([0-9]+)", 1),
    ).otherwise(None),
)
df1 = df1.withColumn(
    "OPID3",
    when(
        regexp_extract("creative_desc", "(?:.*opid-)([0-9]+)", 1) != "",
        regexp_extract("creative_desc", "(?:.*opid-)([0-9]+)", 1),
    ).otherwise(None),
)
df1 = df1.withColumn(
    "OPID4",
    when(
        regexp_extract("media_buy_desc", "(?:.*opid-)([0-9]+)", 1) != "",
        regexp_extract("media_buy_desc", "(?:.*opid-)([0-9]+)", 1),
    ).otherwise(None),
)

# Consolidate OPID values into a single column 'OPID'
df1 = df1.withColumn("OPID", coalesce("OPID1", "OPID2", "OPID3", "OPID4"))

# Drop unnecessary columns
df1 = df1.drop("creative_name", "media_buy_name", "OPID1", "OPID2", "OPID3", "OPID4")

# If OPID row count for a campaign is same as null count for that camapign from above table, it means that it has no OPID.
no_opid_campaigns = (
    df1.groupBy("country_id", "campaign_desc")
    .agg(
        count("campaign_desc").alias("count_records"),
        sum(when(col("OPID").isNull(), 1).otherwise(0)).alias("count_null_values"),
    )
    .filter(col("count_records") == col("count_null_values"))
    .select("country_id", "campaign_desc")
)
####
opid_campaigns = (
    df1.groupBy("country_id", "campaign_desc")
    .agg(
        count("campaign_desc").alias("count_records"),
        sum(when(col("OPID").isNull(), 1).otherwise(0)).alias("count_null_values"),
    )
    .filter(col("count_records") != col("count_null_values"))
    .select("country_id", "campaign_desc")
)

print(
    f"Total campaign count: {df.select('country_id','campaign_desc').distinct().count()}"
)
print(f"No OPID campaign count: {no_opid_campaigns.count()}")

# Final campaigns with no OPID along with their creative and media buy name
df.join(no_opid_campaigns, ["country_id", "campaign_desc"]).display()

# COMMAND ----------

fact_agg_actual_spend = df_fact_performance.select('gpd_campaign_id','campaign_desc','channel_id','product_id','date','actual_spend_dollars').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id', how = 'left').join(dim_campaign.select('gpd_campaign_id','gpd_campaign_code'), on = 'gpd_campaign_id', how = 'left').groupBy('gpd_campaign_id','campaign_desc','platform_desc','brand').agg(sum('actual_spend_dollars'))
# .join(dim_campaign.select('gpd_campaign_id','campaign_desc'), on = 'gpd_campaign_id', how = 'inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Campaigns with no OPID 

# COMMAND ----------

no_opid_campaigns.filter(~col('country_id').isin(['US','CA'])).drop_duplicates().join(fact_agg_actual_spend, on = 'campaign_desc', how = 'left').drop_duplicates().display()
# no_opid_campaigns.filter(~col('country_id').isin(['US','CA'])).drop_duplicates().display()
# .groupBy('country_id','platform_desc').agg(count('campaign_desc'))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Campaigns with OPID but dont have data in planned spend

# COMMAND ----------

# opid_campaigns.join(df_planned_spend, on = 'campaign_desc', how = 'left_anti').dropDuplicates().display()
opid_campaigns.join(df_planned_spend, on = 'campaign_desc', how = 'left_anti').dropDuplicates().join(fact_agg_actual_spend, on = 'campaign_desc', how = 'left').dropDuplicates().display()

# COMMAND ----------

df_fact_performance.filter(col('campaign_desc') == "Mars_M&M'S_Evergreen_Conversions_D2C_RT_ASM 2022").display()
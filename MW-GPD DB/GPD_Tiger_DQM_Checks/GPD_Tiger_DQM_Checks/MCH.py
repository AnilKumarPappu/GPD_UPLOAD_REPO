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

# MAGIC %md
# MAGIC --> Since the tables are in different RG, we are using JDBC connection to retrieve the tables.

# COMMAND ----------

#Prod Connection
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


# Dev Connection

# def jdbc_connection(dbtable):
#     url = "jdbc:sqlserver://globalxsegsrmdatafoundationdevsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
#     user_name = dbutils.secrets.get(
#         scope="mwoddasandbox1005devsecretscope", key="databricksusername"
#     )
#     password = dbutils.secrets.get(
#         scope="mwoddasandbox1005devsecretscope", key="databrickspassword2"
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

#Dev Tables
# df_reach = jdbc_connection('mm_test.vw_mw_gpd_fact_reach')
# df_fact_performance = jdbc_connection('mm_test.vw_mw_gpd_fact_performance')
# df_planned_spend = jdbc_connection('mm_test.vw_mw_gpd_fact_plannedspend')
# dim_campaign = jdbc_connection('mm_test.vw_mw_gpd_dim_campaign')
# dim_channel = jdbc_connection('mm_test.vw_mw_gpd_dim_channel')
# dim_creative = jdbc_connection('mm_test.vw_mw_gpd_dim_creative')
# dim_strategy = jdbc_connection('mm_test.vw_mw_gpd_dim_strategy')
# dim_mediabuy = jdbc_connection('mm_test.vw_mw_gpd_dim_mediabuy')
# dim_site = jdbc_connection('mm_test.vw_mw_gpd_dim_site')
# dim_country = jdbc_connection('mm_test.vw_mw_gpd_dim_country')
# dim_product = jdbc_connection('mm_test.vw_mw_gpd_dim_product')

# COMMAND ----------

#Prod Tables
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

# MAGIC %md
# MAGIC Preliminary Checks

# COMMAND ----------

# MAGIC %md
# MAGIC 1. There should not be any empty tables 

# COMMAND ----------


print('df_reach--->', df_reach.count())
print('df_planned_spend--->', df_planned_spend.count())
print('dim_campaign--->', dim_campaign.count())
print('dim_channel--->', dim_channel.count())
print('dim_strategy--->',dim_strategy.count())
print('dim_mediabuy--->', dim_mediabuy.count())
print('dim_site--->', dim_site.count())
print('df_fact_performance--->', df_fact_performance.count())
print('dim_creative--->' ,dim_creative.count())
print('dim_country--->',dim_country.count())
print('dim_product--->',dim_product.count())

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE--> Filtering countries as per our requirements. Skip if all countries are needed

# COMMAND ----------

countries = []

skip_countries = [

]

if countries:
    df_reach = df_reach.filter(col("country_id").isin(countries))
    df_fact_performance = df_fact_performance.filter(col("country_id").isin(countries))
    df_planned_spend = df_planned_spend.filter(col("country_id").isin(countries))
    dim_campaign = dim_campaign.filter(col("country_id").isin(countries))
    dim_channel = dim_channel.filter(col("country_id").isin(countries))
    dim_creative = dim_creative.filter(col("country_id").isin(countries))
    dim_strategy = dim_strategy.filter(col("country_id").isin(countries))
    dim_mediabuy = dim_mediabuy.filter(col("country_id").isin(countries))
    dim_site = dim_site.filter(col("country_id").isin(countries))

if skip_countries:
    df_reach = df_reach.filter(~col("country_id").isin(countries))
    df_fact_performance = df_fact_performance.filter(~col("country_id").isin(countries))
    df_planned_spend = df_planned_spend.filter(~col("country_id").isin(countries))
    dim_campaign = dim_campaign.filter(~col("country_id").isin(countries))
    dim_channel = dim_channel.filter(~col("country_id").isin(countries))
    dim_creative = dim_creative.filter(~col("country_id").isin(countries))
    dim_strategy = dim_strategy.filter(~col("country_id").isin(countries))
    dim_mediabuy = dim_mediabuy.filter(~col("country_id").isin(countries))
    dim_site = dim_site.filter(~col("country_id").isin(countries))

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Check if the primary keys are valid in dim tables & their count
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
dim_mediabuy.groupBy("media_buy_id").count().filter(col("count") > 1).join(
    dim_mediabuy, "media_buy_id"
).display()
dim_channel.groupBy("channel_id").count().filter(col("count") > 1).join(
    dim_channel, "channel_id"
).display()
dim_site.groupBy("site_id").count().filter(col("count") > 1).join(
    dim_site, "site_id"
).display()
dim_strategy.groupBy("strategy_id").count().filter(col("count") > 1).join(
    dim_strategy, "strategy_id"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard Breaking Issues

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. Granularity of Source Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Datorama Data
# MAGIC ### There shouldn't be Multiple Campaign name for One gpd_campaign_id in  (fact_perf + dim_campaign tables)

# COMMAND ----------

df_fact_performance.select('gpd_campaign_id', 'campaign_desc').dropDuplicates().groupBy('gpd_campaign_id').agg(count('campaign_desc')).filter(col('count(campaign_desc)') > 1 ).display()

dim_campaign.groupBy('gpd_campaign_id').count().filter(col("count") > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Olive Data
# MAGIC ### There shouldn't be Multiple budgets for same plan line ID
# MAGIC --> As seen from analysis, the planned budget should be unique to a plan line ID. In case of multiple occurences, the logic doesn't holds true and the dashboard breaks.

# COMMAND ----------

# budget in dollars
df_planned_spend.select("plan_line_id", "planned_budget_dollars").distinct().groupBy(
    "plan_line_id"
).count().filter(col("count") > 1).display()

# budget in local currency
df_planned_spend.select("plan_line_id", "planned_budget_local_currency").distinct().groupBy(
    "plan_line_id"
).count().filter(col("count") > 1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Reach Data
# MAGIC Reach should be unique at campaign_desc X Platform level

# COMMAND ----------



df_reach.join(dim_channel, on='channel_id',
               how='left').select('gpd_campaign_id', 
                                  'platform_desc',
                                  'in_platform_reach').dropDuplicates().groupBy(
                                                'gpd_campaign_id',
                                                'platform_desc').agg(count('in_platform_reach')).filter(col('count(in_platform_reach)')>1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ###B. There shouldn't be any null values in any fact tables & dim tables

# COMMAND ----------

df_fact_performance.select(
    [count(when(isnull(c), c)).alias(c) for c in df_fact_performance.columns]
).display()

df_planned_spend.select(
    [count(when(isnull(c), c)).alias(c) for c in df_planned_spend.columns]
).display()

df_reach.select([count(when(isnull(c), c)).alias(c) for c in df_reach.columns]).display()


dim_campaign.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_campaign.columns]
).display()

dim_product.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_product.columns]
).display()


dim_mediabuy.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_mediabuy.columns]
).display()

dim_creative.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_creative.columns]
).display()


dim_site.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_site.columns]
).display()

dim_channel.select(
    [count(when(isnull(c), c)).alias(c) for c in dim_channel.columns]
).display()





# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### B1. There should not be any duplicate rows in Table

# COMMAND ----------

print(f"{df_fact_performance.count() - df_fact_performance.dropDuplicates().count()} duplicates in df_fact_performance")

print(f"{df_planned_spend.count() - df_planned_spend.dropDuplicates().count()} duplicates in df_planned_spend")

print(f"{df_reach.count() - df_reach.dropDuplicates().count()} duplicates in df_reach")

print(f"{dim_country.count() - dim_country.dropDuplicates().count()} duplicates in dim_country")

print(f"{dim_product.count() - dim_product.dropDuplicates().count()} duplicates in dim_product")

print(f"{dim_campaign.count() - dim_campaign.dropDuplicates().count()} duplicates in dim_campaign")

print(f"{dim_channel.count() - dim_channel.dropDuplicates().count()} duplicates in dim_channel")

print(f"{dim_creative.count() - dim_creative.dropDuplicates().count()} duplicates in dim_creative")

print(f"{dim_mediabuy.count() - dim_mediabuy.dropDuplicates().count()} duplicates in dim_mediabuy")

print(f"{dim_site.count() - dim_site.dropDuplicates().count()} duplicates in dim_site")

print(f"{dim_strategy.count() - dim_strategy.dropDuplicates().count()} duplicates in dim_strategy")

# COMMAND ----------

# MAGIC %md
# MAGIC #### C. Data for fact_pefromace table should be available in all dim_tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. fact_performance & dim_product
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_product, on ='product_id'
                         , how='left').filter(dim_product.country_id.isNull()).select(
                                                df_fact_performance.country_id,
                                                df_fact_performance.product_id,
                                                'gpd_campaign_id',
                                                'campaign_desc',
                                                'product',
                                                'brand',
                                                'campaign_start_date',
                                                'campaign_end_date'
                         ).dropDuplicates()

print("Number of missing product_id in dim_product Table:-  ", 
      df.select('product_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC 2. fact_performance & dim_channel
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_channel, on ='channel_id'
                                             , how='left').filter(col('channel_desc').isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.channel_id,
                                                                'gpd_campaign_id',
                                                                'campaign_desc',
                                                                'channel_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing Channel_id in dim_channel Table:-  ", 
      df.select('channel_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC  3. fact_performance & dim_creative
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_creative, on ='creative_id',
                              how='left').filter(dim_creative.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.creative_id,
                                                                'creative_desc',
                                                                'campaign_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing creative_id in dim_creative Table:-  ", 
      df.select('creative_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC  4. fact_performance & dim_mediabuy
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_mediabuy, on ='media_buy_id',
                              how='left').filter(dim_mediabuy.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.media_buy_id,
                                                                'media_buy_desc',
                                                                'campaign_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing mediabuy_id in dim_mediabuy Table:-  ", 
      df.select('media_buy_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC  5. fact_performance & dim_campaign
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_campaign, on ='gpd_campaign_id',
                              how='left').filter(dim_campaign.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.gpd_campaign_id,
                                                                df_fact_performance.campaign_desc,
                                                                dim_campaign.campaign_desc,
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing gpd_campaign_id in dim_campaign Table:-  ", 
      df.select('gpd_campaign_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC  6. fact_performance & dim_site
# MAGIC

# COMMAND ----------

df = df_fact_performance.join(dim_site, on ='site_id',
                              how='left').filter(dim_site.country_id.isNull()).select(
                                                                df_fact_performance.country_id,
                                                                df_fact_performance.site_id,
                                                                df_fact_performance.campaign_desc,
                                                                'site_desc',
                                                                'campaign_start_date',
                                                                'campaign_end_date'
                                        ).dropDuplicates()

print("Number of missing site_id in dim_site Table:-  ", 
      df.select('site_id').dropDuplicates().count())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. For all campaigns, campaign_platform start and end dates must fall b/w campaign start and end date of each campaign<br>
# MAGIC --> Asserting the start and end date of a campaign/platform with min and max date of data availability.

# COMMAND ----------

# Campaign Platform Start and end date check
df_camp_plat_dates = df_fact_performance.groupBy(
    "country_id", "gpd_campaign_id", "channel_id"
).agg(
    min("date").alias("camp_plat_start_date"), max("date").alias("camp_plat_end_date")
)
df_camp_plat_dates = df_camp_plat_dates.join(
    df_fact_performance.select(
        "country_id",
        "gpd_campaign_id",
        "channel_id",
        "campaign_platform_start_date",
        "campaign_platform_end_date",
    ),
    ["country_id", "gpd_campaign_id", "channel_id"],
)

df_camp_plat_dates.join(
    dim_campaign.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
).join(dim_channel.withColumnRenamed("country_id", "country_id2"), "channel_id").filter(
    (col("camp_plat_start_date") != col("campaign_platform_start_date"))
    & (col("camp_plat_end_date") != col("campaign_platform_end_date"))
).select(
    "country_id",
    "gpd_campaign_id",
    "channel_id",
    "channel_desc",
    "platform_desc",
    "campaign_desc",
    "campaign_platform_start_date",
    "campaign_platform_end_date",
    "camp_plat_start_date",
    "camp_plat_end_date",
).display()

# COMMAND ----------

# Campaign Start and end date check
df_camp_plat_dates = (
    df_fact_performance.groupBy("country_id", "gpd_campaign_id")
    .agg(min("date").alias("camp_start_date"), max("date").alias("camp_end_date"))
    .distinct()
)
df_camp_plat_dates = df_camp_plat_dates.join(
    df_fact_performance.select(
        "country_id", "gpd_campaign_id", "campaign_start_date", "campaign_end_date"
    ).distinct(),
    ["country_id", "gpd_campaign_id"],
)

df_camp_plat_dates.join(
    dim_campaign.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
).filter(
    (col("camp_start_date") != col("campaign_start_date"))
    & (col("camp_end_date") != col("campaign_end_date"))
).select(
    "country_id",
    "gpd_campaign_id",
    "campaign_desc",
    "campaign_start_date",
    "campaign_end_date",
    "camp_start_date",
    "camp_end_date",
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. The Planned Spend LC/USD ratio should be similar to Actual Spend LC/USD ratio, to show consistent currency conversion
# MAGIC --> The USD vs Local ratio should be somewhat similar among actual and planned spend. Since the data is only a couple years old, the currency shouldn't appreciate or depreciate to a huge extent compared to USD. THe ratio should make sense with current conversion ratio found on Google.

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

df_fact_performance1.join(df_planned_spend1, "country_id").withColumn(
    "ratio", col("planned_ratio") / col("actual_ratio")
).display()

# COMMAND ----------

dim_product.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### * Data-type should be correct for all table

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 3. Countries should be consistent in all fact tables
# MAGIC --> It is expected that the data for countries should be consistent among all tables. If not, it should be raised to the source.

# COMMAND ----------

df1 = (
    df_fact_performance.select("country_id")
    .distinct()
    .withColumn("index", monotonically_increasing_id())
    .withColumnRenamed("country_id", "performance")
)
df2 = (
    df_planned_spend.select("country_id")
    .distinct()
    .withColumn("index", monotonically_increasing_id())
    .withColumnRenamed("country_id", "planned_spend")
)
df3 = (
    df_reach.select("country_id")
    .distinct()
    .withColumn("index", monotonically_increasing_id())
    .withColumnRenamed("country_id", "reach")
)
df1.join(df2, "index", "outer").join(df3, "index", "outer").drop("index").display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Reach frequency in data should match with calculated frequency
# MAGIC --> Frequency = Total number of impressions / Total reach <br>
# MAGIC     Impressions are aggregated on platform level from fact_performance which are the divided by the in_platform_reach. If the outcome is not same as in the frequency column, there is an error in data transformation.

# COMMAND ----------

df_fact_performance1 = df_fact_performance.join(
    dim_channel.withColumnRenamed("country_id", "country_id1"), "channel_id"
).filter(col("platform_desc") != "Campaign Manager")

df_reach1 = df_reach.join(
    df_fact_performance1.groupBy(["country_id", "gpd_campaign_id", "channel_id"]).agg(
        sum("impressions").alias("impressions")
    ),
    ["country_id", "gpd_campaign_id", "channel_id"],
)

df_reach1 = (
    df_reach1.withColumn(
        "calculated_freq", col("impressions") / col("in_platform_reach")
    )
    .withColumn("calculated_freq", round(col("calculated_freq"), 2))
    .withColumn("frequency", round(col("frequency"), 2))
    .filter(col("frequency") != col("calculated_freq"))
    .withColumn("diff", col("calculated_freq") - col("frequency"))
)

df_reach1.join(
    dim_channel.withColumnRenamed("country_id", "country_id1"), "channel_id"
).select(
    "country_id",
    "gpd_campaign_id",
    "channel_id",
    "channel_desc",
    "platform_desc",
    "campaign_desc",
    "frequency",
    "calculated_freq",
    "diff",
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 5. There should not be a High Variance b/w Actual and Planned Spend for countries
# MAGIC --> The planned amount and actual spend is expected to be in similar range. Anything above 110% and below 30% is an outlier.

# COMMAND ----------

x = (
    df_planned_spend.select(
        "country_id",
        "channel_id",
        "gpd_campaign_id",
        "plan_line_id",
        "planned_budget_dollars",
        "planned_budget_local_currency",
    )
    .dropDuplicates()
    .groupBy("country_id", "channel_id", "gpd_campaign_id")
    .agg(
        sum("planned_budget_dollars").alias("planned_budget_dollars"),
        sum("planned_budget_local_currency").alias("planned_budget_local_currency"),
    )
)

y = df_fact_performance.groupBy("country_id", "channel_id", "gpd_campaign_id").agg(
    sum("actual_spend_dollars").alias("actual_spend_dollars"),
    sum("actual_spend_local_currency").alias("actual_spend_local_currency"),
)

df = (
    x.join(y, on=["country_id", "channel_id", "gpd_campaign_id"])
    .withColumn(
        "variance_usd",
        (col("actual_spend_dollars") / col("planned_budget_dollars")) * 100,
    )
    .withColumn(
        "variance_local",
        (col("actual_spend_local_currency") / col("planned_budget_local_currency"))
        * 100,
    )
)

z = df.filter((col("variance_local") > 110) | (col("variance_local") < 30))
print(f"{z.count()}/{df.count()} = {z.count()/df.count()}")
z.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Spend shouldn't be 0 at year level
# MAGIC --> A sense check to make sure the planned spend and actual spend shouldn't be 0 at year level for any country.

# COMMAND ----------

df1 = df_planned_spend.groupBy(
    "country_id", col("plan_start_date").substr(1, 4).alias("year")
    ).agg(
    sum("planned_budget_dollars").alias("planned_budget_dollars"),
    sum("planned_budget_local_currency").alias("planned_budget_local_currency"),
    )

df2 = df_fact_performance.groupBy("country_id", col("date").substr(1, 4).alias("year")).agg(
    sum("actual_spend_dollars").alias("actual_spend_dollars"),
    sum("actual_spend_local_currency").alias("actual_spend_local_currency"),
    )

df2.join(df1, on =['country_id', 'year'], how = 'left').display()

# COMMAND ----------

df_fact_performance.groupBy("country_id", col("date").substr(1, 4).alias("year")).agg(
    sum("actual_spend_dollars").alias("actual_spend_dollars"),
    sum("actual_spend_local_currency").alias("actual_spend_local_currency"),
).display()

# COMMAND ----------

df_planned_spend.groupBy(
    "country_id", col("plan_start_date").substr(1, 4).alias("year")
).agg(
    sum("planned_budget_dollars").alias("planned_budget_dollars"),
    sum("planned_budget_local_currency").alias("planned_budget_local_currency"),
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 7.Brand level Categorization for campaign_desc Naming convensin taxonomy Checks.

# COMMAND ----------

# Fact_performance
df_fact_performance_23_24 = df_fact_performance.withColumn("year", substring("date", 1, 4)).filter(col("year").isin(['2023','2024']))


# COMMAND ----------


df_fact_performance_23_24.join(dim_country.select("country_id",
                            "marketregion_code"), 
                               on = "country_id", 
                               how = "left").join(dim_product.select("product_id",
                                                "brand","product"), 
                                                  on = "product_id", 
                                                  how = "left").join(
                                                      dim_channel.select(
                                                          "channel_id","channel_desc","platform_desc"),
                                                         on = "channel_id",
                                                         how = "left").join(
                                                             dim_campaign.select(
                                                                 "gpd_campaign_id","gpd_campaign_code","campaign_subproduct","media_channel",'naming_convention_flag'), 
                                                             on = "gpd_campaign_id" ,
                                                             how = "inner").join(dim_creative.select("creative_id",
                                                            "creative_subproduct"), 
                                                            on = "creative_id", 
                                                            how= "left").select(
                                                                "year","marketregion_code","country_id","product","brand","channel_desc","platform_desc","gpd_campaign_id","gpd_campaign_code","campaign_desc","creative_subproduct","campaign_start_date","campaign_end_date","campaign_subproduct","media_channel",'naming_convention_flag' 
                                                                ).dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
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
# MAGIC ### Load tables

# COMMAND ----------

# MAGIC %md
# MAGIC --> Since the tables are in different RG, we are using JDBC connection to retrieve the tables.

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

#Prod Tables
df_reach = jdbc_connection('mm.vw_mw_gpd_fact_reach') #datorama reach
df_fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance') # Dataroma
df_planned_spend = jdbc_connection('mm.vw_mw_gpd_fact_plannedspend')  # Olive
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

# MAGIC %md
# MAGIC ### Preliminary Checks

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. There should not be any empty tables 

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
# MAGIC #### 2. Check if the primary keys are valid in dim tables & their count
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
# MAGIC #####1. Olive Data
# MAGIC There shouldn't be Multiple budgets for same plan line ID
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
# MAGIC ##### 2. Datorama Data
# MAGIC There shouldn't be Multiple Campaign name for One gpd_campaign_id in  (fact_perf + dim_campaign tables)

# COMMAND ----------

df_fact_performance.select('gpd_campaign_id', 'campaign_desc').dropDuplicates().groupBy('gpd_campaign_id').agg(count('campaign_desc')).filter(col('count(campaign_desc)') > 1 ).display()

dim_campaign.groupBy('gpd_campaign_id').count().filter(col("count") > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Reach Data
# MAGIC Reach should be unique at campaign_desc X Platform level

# COMMAND ----------


df_reach.join(dim_channel, on='channel_id',
               how='left').select('gpd_campaign_id', 
                                  'platform_desc',
                                  'date',
                                  'in_platform_reach').dropDuplicates().groupBy(
                                                'gpd_campaign_id',
                                                'platform_desc','date').agg(count('in_platform_reach')).filter(col('count(in_platform_reach)')>1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### B. Data in fact tables should be available in all dim_tables
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Performance

# COMMAND ----------

df_fact_performance.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').select("gpd_campaign_id").distinct().display()
df_fact_performance.join(dim_product, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
df_fact_performance.join(dim_channel, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()
df_fact_performance.join(dim_creative, on = "creative_id", how = 'left_anti').select("creative_id").distinct().display()


# COMMAND ----------

# MAGIC %md
# MAGIC Planned

# COMMAND ----------

df_planned_spend.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').select("gpd_campaign_id").distinct().display()
df_planned_spend.join(dim_product, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
df_planned_spend.join(dim_channel, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Reach

# COMMAND ----------

df_reach.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').select("gpd_campaign_id").distinct().display()
df_reach.join(dim_product, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
df_reach.join(dim_channel, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()

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
# MAGIC #### C. Countries should be consistent in all fact tables
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

# count of countries in performance table should not be below 30
# count of countries in planned_spend table should not be below 28
# we can ignore JP in both above tables
# count of countries in reach table should not be below 30


# COMMAND ----------

# MAGIC %md
# MAGIC #### D. There shouldn't be any null values in any fact tables & dim tables

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
# MAGIC #### E. There should not be any duplicate rows in Table

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
# MAGIC ### Data Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Data present in Olive & Datorama should be latest.
# MAGIC --> Fact performance table refreshes daily on the asset layer. So the dates should be latest in access layer as well.

# COMMAND ----------

df_fact_performance.groupBy("country_id").agg(
    min("date"), max("date"), count("date")
).display()


df_planned_spend.groupBy("country_id").agg(
    min("campaign_start_date"), max("campaign_end_date"), count("country_id")
).display()

df_reach.groupBy("country_id").agg(
    min("campaign_platform_start_date"), max("campaign_platform_end_date"), count("country_id")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Check for missing data at country X year level

# COMMAND ----------


df_1 = df_fact_performance.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")


df_2 = df_fact_performance.withColumn('Year', year('date'))
df_2 = df_2.groupBy('country_id', 'Year').agg(count('date'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------


df_1 = df_planned_spend.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2022",), ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")


df_2 = df_planned_spend.withColumn('Year', year('campaign_start_date'))
df_2 = df_2.groupBy('country_id', 'Year').agg(count('campaign_start_date'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------


df_1 = df_reach.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2022",), ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")


df_2 = df_reach.withColumn('Year', year('campaign_platform_start_date'))
df_2 = df_2.groupBy('country_id', 'Year').agg(count('campaign_platform_start_date'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ----------> KPIs at country level

# COMMAND ----------

df_fact_performance.groupBy("country_id").agg( max("date"), sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played'),sum('views')).display()


df_planned_spend.groupBy("country_id").agg(
    max("campaign_end_date"),sum('planned_budget_dollars'),sum('planned_budget_local_currency')).display()

df_reach.groupBy("country_id").agg(
     max("campaign_platform_end_date"),sum('frequency'),sum('in_platform_reach')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reach frequency in data should match with calculated frequency
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
    "impressions",
    "in_platform_reach",
    "frequency",
    "calculated_freq",
    "diff",
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. For all campaigns, campaign_platform start and end dates must fall b/w campaign start and end date of each campaign<br>
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
# MAGIC ### 5. There should not be a High Variance b/w Actual and Planned Spend for countries
# MAGIC --> The planned amount and actual spend is expected to be in similar range. Anything above 110% and below 30% is an outlier.

# COMMAND ----------

x = (
    df_planned_spend.withColumn('Year', year('campaign_start_date')).select(
        "country_id",
        "Year",
        "channel_id",
        "gpd_campaign_id",
        "plan_line_id",
        "planned_budget_dollars",
        "planned_budget_local_currency",
    )
    .dropDuplicates()
    .groupBy("country_id", "Year", "channel_id", "gpd_campaign_id")
    .agg(
        sum("planned_budget_dollars").alias("planned_budget_dollars"),
        sum("planned_budget_local_currency").alias("planned_budget_local_currency"),
    )
)

y = df_fact_performance.withColumn('Year', year('date')).groupBy("country_id","Year", "channel_id", "gpd_campaign_id").agg(
    sum("actual_spend_dollars").alias("actual_spend_dollars"),
    sum("actual_spend_local_currency").alias("actual_spend_local_currency"),
)

df = (
    x.join(y, on=["country_id", "Year","channel_id", "gpd_campaign_id"])
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
# if the count of rows is more than 5K, then need to consider

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. The Planned Spend LC/USD ratio should be similar to Actual Spend LC/USD ratio, to show consistent currency conversion
# MAGIC --> The USD vs Local ratio should be somewhat similar among actual and planned spend. Since the data is only a couple years old, the currency shouldn't appreciate or depreciate to a huge extent compared to USD. THe ratio should make sense with current conversion ratio found on Google.

# COMMAND ----------

df_planned_spend1 = (
    df_planned_spend.withColumn('Year', year('campaign_start_date')).select(
        "country_id",
        'Year',
        "plan_line_id",
        "planned_budget_dollars",
        "planned_budget_local_currency",
    )
    .dropDuplicates()
    .groupBy("country_id", 'Year')
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
    df_fact_performance.withColumn('Year', year('date')).groupBy("country_id", "Year")
    .agg(
        sum("actual_spend_local_currency").alias("actual_spends_local_currency"),
        sum("actual_spend_dollars").alias("actual_spends_dollars"),
    )
    .withColumn(
        "actual_ratio",
        col("actual_spends_local_currency") / col("actual_spends_dollars"),
    )
)

df_fact_performance1.join(df_planned_spend1, ["country_id", "Year"]).withColumn(
    "ratio", col("planned_ratio") / col("actual_ratio")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Check null in Brand, Region

# COMMAND ----------

dim_product.select('country_id','brand').drop_duplicates().filter(col('brand').isNull()).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. No OPID campaign count
# MAGIC --> Since OPID is the joining column of a campaign to it's planned budget and we know that it is either extracted from creative or mediabuy name. So here we are trying to check the number of campaign that do not have even a single OPID.

# COMMAND ----------

# Get creative name and media buy name of distinct campaigns
df = df_fact_performance.select(
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
df.join(no_opid_campaigns, ["country_id", "campaign_desc"]).display() # default inner join

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Spend shouldn't be 0 at year level
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

# MAGIC %md
# MAGIC ### 10.Brand level Categorization for campaign_desc Naming convensin taxonomy Checks.

# COMMAND ----------


# Fact_performance
df_fact_performance_23_24 = df_fact_performance.withColumn("year", substring("date", 1, 4)).filter(col("year").isin(['2023','2024']))

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
# MAGIC ### 11(no need) . All In_platform_reach and frequency metrics available for all "reach" and "awareness" campaigns in fact_reach

# COMMAND ----------

df_reach.join(dim_campaign.select('gpd_campaign_id','media_objective'), on = 'gpd_campaign_id', how = 'left').filter(col('media_objective').isin(['reach','REACH','BRAND AWARENESS','Awareness','OUTCOME_AWARENESS','Awareness (Store Traffic)','awareness','AWARENESS','awarenesss','Reach'])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.planned spend data available for how many campaigns vs not 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Campaigns with no OPID 

# COMMAND ----------

no_opid_campaigns.display()
# no_opid_campaigns.join(df_fact_performance.select('campaign_desc','creative_id','media_buy_id'), on = 'campaign_desc', how = 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Campaigns with OPID but dont have data in planned spend

# COMMAND ----------

opid_campaigns.join(df_planned_spend, on = 'campaign_desc', how = 'left_anti').display()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 13(no need). List of video campaigns where video views are 0 

# COMMAND ----------

df_fact_performance.select('gpd_campaign_id', 'media_buy_id','video_completion_25','views').dropDuplicates().join(dim_campaign.select('gpd_campaign_id', 'campaign_desc'), on = 'gpd_campaign_id', how = 'inner').join(dim_mediabuy.filter(col('naming_convention_flag') == 0).filter(col('mediabuy_format').isin(['video','Video'])), on = 'media_buy_id', how = 'left').filter(col('views')== 0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14.Reach and frequency should be available for each row and should not have flaws

# COMMAND ----------

df_reach.filter((col('frequency')== '0.0') | (col('in_platform_reach').contains('-'))).display()
# df_reach.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. One campaign found in multiple countries

# COMMAND ----------

df1 = df_fact_performance.filter(col('date')>='2023-01-01').select('country_id', 
                           'campaign_desc').dropDuplicates().groupBy('campaign_desc').agg(count('country_id')).filter(col('count(country_id)')>1)

df2 = df_fact_performance.filter(col('date')>='2023-01-01').select('country_id', 
                           'campaign_desc').dropDuplicates()
df1.join(df2, on='campaign_desc', how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16. Wrong campaign_desc to country mapping

# COMMAND ----------

df_fact_performance.filter(col('date')>='2023-01-01').select('country_id', 
                           'campaign_desc' ,
                           upper(substring('campaign_desc', 1, 2)).alias('country_id_extract')).filter(col('country_id_extract') != col('country_id')).dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 17. To check the 2022 and 2023 data with the next historical reload 

# COMMAND ----------

df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year') == '2022').join(dim_channel.select('channel_id','platform_desc') , on = 'channel_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id', how = 'left' ).groupBy('country_id', 'year','channel_id','platform_desc','brand').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year') == '2023').join(dim_channel.select('channel_id','platform_desc') , on = 'channel_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id', how = 'left' ).groupBy('country_id', 'year','channel_id','platform_desc','brand').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()
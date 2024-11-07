# Databricks notebook source
# import time
# time.sleep(3600)

# COMMAND ----------

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

# dont rerun this. WE need to check when data went missing
#print('df_reach--->', df_reach.count())
#print('df_planned_spend--->', df_planned_spend.count())
#print('dim_campaign--->', dim_campaign.count())
#print('dim_channel--->', dim_channel.count())
#print('dim_strategy--->',dim_strategy.count())
#print('dim_mediabuy--->', dim_mediabuy.count())
#print('dim_site--->', dim_site.count())
#print('df_fact_performance--->', df_fact_performance.count())
#print('dim_creative--->' ,dim_creative.count())
#print('dim_country--->',dim_country.count())
#print('dim_product--->',dim_product.count())

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
# MAGIC --> Filtering countries as per our requirements. Skip if all countries are needed

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

# writer = pd.ExcelWriter('prod.xlsx', engine = 'xlsxwriter')
# df_reach.limit(100).toPandas().to_excel(writer, sheet_name='df_reach')
# df_fact_performance.limit(100).toPandas().to_excel(writer, sheet_name = 'df_fact_performance')
# # df_planned_spend.limit(100).toPandas().to_excel(writer, sheet_name = 'df_planned_spend')
# dim_campaign.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_campaign')
# dim_channel.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_channel')
# dim_creative.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_creative')
# dim_strategy.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_strategy')
# dim_mediabuy.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_mediabuy')
# dim_site.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_site')
# dim_country.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_country')
# dim_product.limit(100).toPandas().to_excel(writer, sheet_name = 'dim_product')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Check if the primary keys are valid in dim tables & their count
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

df_fact_performance.select('gpd_campaign_id', 'campaign_desc').dropDuplicates().groupBy('gpd_campaign_id').agg(count('campaign_desc')).filter(col('count(campaign_desc)') > 1 ).display()

# COMMAND ----------

dim_campaign.groupBy('gpd_campaign_id').count().filter(col("count") > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Data present in Olive & Datorama should be latest.
# MAGIC --> Fact performance table refreshes daily on the asset layer. So the dates should be latest in access layer as well.

# COMMAND ----------

df_planned_spend.groupBy("country_id").agg(
    min("campaign_start_date"), max("campaign_end_date"), count("country_id")
).display()

# COMMAND ----------

df_1 = df_fact_performance.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")


df_2 = df_fact_performance.withColumn('Year', year('date'))
df_2 = df_2.groupBy('country_id', 'Year').agg(count('date'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---->> Overview of Datorama data

# COMMAND ----------

df_1 = df_fact_performance.select("country_id").distinct()
years_df_1 = spark.createDataFrame([ ("2023",), ("2024",)], ["Year"])
df_1 = df_1.crossJoin(years_df_1)
df_1 = df_1.withColumnRenamed("col", "Year")

df_2 = df_fact_performance.withColumn('Year', year('date'))
df_2 = df_2.join(dim_product, on='product_id', how='left').join(dim_channel, on='channel_id', how='left')

df_2 = df_2.groupBy(df_fact_performance.country_id, 'Year', 'brand', 'platform_desc').agg(sum('actual_spend_dollars'), sum('impressions'), count('*').alias('rows count'))

df_1.join(df_2, on=['country_id', 'Year'], how='left').display()

# COMMAND ----------


df_fact_performance.groupBy("country_id").agg(
    min("date"), max("date"), count("date")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. There shouldn't be Multiple budgets for same plan line ID
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
# MAGIC ### 5. For all campaigns, campaign_platform start and end dates must fall b/w campaign start and end date of each campaign<br>
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
# MAGIC ### 6. There should  No duplicate rows in any tables

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
# MAGIC ### 7. There should not be a High Variance b/w Actual and Planned Spend for countries
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
# MAGIC ### 8. The Planned Spend LC/USD ratio should be similar to Actual Spend LC/USD ratio, to show consistent currency conversion
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

# MAGIC %md
# MAGIC ### 9. Check null in Brand, Region

# COMMAND ----------

dim_product.select('brand').drop_duplicates().display()


# COMMAND ----------

dim_product.display()
df_fact_performance.select("country_id").distinct().join(
    dim_country, "country_id"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. OPID count
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

print(
    f"Total campaign count: {df.select('country_id','campaign_desc').distinct().count()}"
)
print(f"No OPID campaign count: {no_opid_campaigns.count()}")

# Final campaigns with no OPID along with their creative and media buy name
df.join(no_opid_campaigns, ["country_id", "campaign_desc"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Granularity of performance & reach data
# MAGIC --> Although fact_performance data is at a much granular level of site or strategy, it is still a daily level data. So we need to check that for given granularity, there is a unique row for all the metrices.<br>
# MAGIC     In reach table, the granularity goes to campaign_key x platform, meaning each combination of these should've only one reach.

# COMMAND ----------

df_fact_performance.groupBy(
    "country_id",
    "gpd_campaign_id",
    "channel_id",
    "product_id",
    "creative_id",
    "media_buy_id",
    "strategy_id",
    "site_id",
    "date",
).count().filter(col("count") > 1).display()

# COMMAND ----------

# df_reach.join(
#     dim_campaign.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
# ).join(
#     dim_channel.withColumnRenamed("country_id", "country_id2"), "channel_id"
# ).groupBy(
#     "gpd_campaign_id", "platform_desc"
# ).count().filter(
#     col("count") > 1
# ).display()

df_reach.groupBy("gpd_campaign_id","channel_id","product_id").count().filter(
    col("count") > 1
).display()

# COMMAND ----------

df_reach.filter(col('gpd_campaign_id')=='AU_53217751').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### planned spend granularity

# COMMAND ----------

df_planned_spend.groupBy("gpd_campaign_id","channel_id","product_id","plan_line_id").count().filter(
    col("count") > 1
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Countries should be consistent in all fact tables
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
# MAGIC ###13. There shouldn't be any null values in any fact tables

# COMMAND ----------

df_fact_performance.select(
    [count(when(isnull(c), c)).alias(c) for c in df_fact_performance.columns]
).display()
df_planned_spend.select(
    [count(when(isnull(c), c)).alias(c) for c in df_planned_spend.columns]
).display()
df_reach.select([count(when(isnull(c), c)).alias(c) for c in df_reach.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. Spend shouldn't be 0 at year level
# MAGIC --> A sense check to make sure the planned spend and actual spend shouldn't be 0 at year level for any country.

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
# MAGIC ### 15.Check whether all Id s in fact table are in corresponding dim table

# COMMAND ----------

df_fact_performance.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').select("gpd_campaign_id","campaign_desc","country_id").distinct().display()
df_fact_performance.join(dim_product, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
df_fact_performance.join(dim_channel, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()


# COMMAND ----------

df_fact_performance.join(dim_creative, on = "creative_id", how = 'left_anti').select("creative_id").distinct().display()
df_fact_performance.join(dim_mediabuy, on = "media_buy_id", how = 'left_anti').select("media_buy_id").distinct().display()
df_fact_performance.join(dim_strategy, on = "strategy_id", how = 'left_anti').select("strategy_id").distinct().display()
df_fact_performance.join(dim_site, on = "site_id", how = 'left_anti').select("site_id").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16. Naming Convention flat files
# MAGIC --> For content health, proper naming convention should be followed for all the campaign names, creative names and mediabuy names strating from 2023 Jan 1. This code will generate the flat files which can be later analysed manually or by code.

# COMMAND ----------

df_fact_performance1 = df_fact_performance.join(dim_product, on="product_id")
df_fact_performance1 = df_fact_performance1.join(dim_country, on="country_id")
df_fact_performance1 = df_fact_performance1.join(
    dim_channel.withColumnRenamed("country_id", "country_id1"), on="channel_id"
)
df_fact_performance2 = df_fact_performance1.filter(
    col("campaign_start_date") >= '2023-01-01'
).join(dim_creative.withColumnRenamed("country_id", "country_id2"), on="creative_id")
df_fact_performance2.select(
    "marketregion_code",
    "country_id2",
    "channel_desc",
    "platform_desc",
    "campaign_desc",
    "campaign_start_date",
    "creative_id",
    "creative_desc",
    "creative_subject",
    "creative_variant",
    "creative_type",
    "ad_tag_size",
    "dimension",
    "feature_message",
    "cta",
    "landing_page",
).distinct().display()

# COMMAND ----------

df_fact_performance1 = df_fact_performance.join(dim_product, on="product_id")
df_fact_performance1 = df_fact_performance1.join(dim_country, on="country_id")
df_fact_performance1 = df_fact_performance1.join(
    dim_channel.withColumnRenamed("country_id", "country_id1"), on="channel_id"
)
df_fact_performance2 = df_fact_performance1.filter(
    col("campaign_start_date") >= '2023-01-01'
).join(dim_mediabuy.withColumnRenamed("country_id", "country_id2"), on="media_buy_id")
df_fact_performance2.select(
    "marketregion_code",
    "country_id2",
    "channel_desc",
    "platform_desc",
    "campaign_desc",
    "campaign_start_date",
    "media_buy_id",
    "media_buy_desc",
    "media_buy_type",
    "mediabuy_campaign_desc_free_field",
    "language",
    "buying_type",
    "costing_model",
    "mediabuy_campaign_type",
    "audience_type",
    "audience_desc",
    "data_type",
    "optimisation",
    "placement_type",
    "mediabuy_format",
    "device",
    "mediabuy_dimensions",
).distinct().display()

# COMMAND ----------

df_fact_performance1 = df_fact_performance.join(dim_product, on="product_id")
df_fact_performance1 = df_fact_performance1.join(dim_country, on="country_id")
df_fact_performance1 = df_fact_performance1.join(
    dim_channel.withColumnRenamed("country_id", "country_id1"), on="channel_id"
)
df_fact_performance2 = df_fact_performance1.filter(
    col("campaign_start_date") >= '2023-01-01'
).join(
    dim_campaign.withColumnRenamed("country_id", "country_id2").withColumnRenamed(
        "campaign_desc", "campaign_desc1"
    ),
    on="gpd_campaign_id",
)
df_fact_performance2.select(
    "marketregion_code",
    "country_id2",
    "channel_desc",
    "platform_desc",
    "gpd_campaign_id",
    "campaign_desc",
    "campaign_start_date",
    "campaign_type",
).distinct().display()
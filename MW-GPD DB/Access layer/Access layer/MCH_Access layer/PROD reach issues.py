# Databricks notebook source
# MAGIC %md
# MAGIC ### Synapse DB

# COMMAND ----------

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
dim_calender = jdbc_connection('mm.vw_dim_calendar')
ih_spend = jdbc_connection( "mm.vw_mw_gpd_fact_ih_spend")
ih_ingoing =jdbc_connection("mm.vw_mw_gpd_fact_ih_ingoingspend")
df_fact_creativetest = jdbc_connection( "mm.vw_mw_gpd_fact_creativetest")
df_fact_som = jdbc_connection( "mm.vw_mw_gpd_fact_som")


# COMMAND ----------

df_fact_performance.join(dim_product.select('product_id','brand'), on = 'product_id',how = 'left').filter(col('brand') == 'Kind').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Datatype change in Reach table

# COMMAND ----------

df_reach = df_reach.withColumn("frequency", df_reach["frequency"].cast(DecimalType(20, 2))).withColumn("in_platform_reach", df_reach["in_platform_reach"].cast(DecimalType(20, 2)))

# COMMAND ----------

# df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_desc') == 'us_stg_ttd fep_mw_na_frc_d2c_olv_awareness_bid-nadr_0724_hulu-q3-direct-7/15 to 9/15 -ma3-stg 027').display()


# COMMAND ----------

df_fact_performance.filter(col('campaign_desc') == 'us_gm5_core-mikmak_mw_na_gum_brand_social_traffic_bid-adr_0724_ma3-meta-7/23-to-7/26-1PD/LAL').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Date column values should fall under campaign platfrom start & end dates.

# COMMAND ----------

from pyspark.sql.functions import current_date
df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_platform_start_date')>= '2024-07-15').filter(~((col('date')>= col('campaign_platform_start_date'))   & (col('date')<= col('campaign_platform_end_date')+1))).display()


# for old campaigns
# df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(~((col('date')>= col('campaign_platform_start_date')-1)   & (col('date')<= current_date() ))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2.The number of records at CampaignId X platform level are lower than number of days the campaign ran referring to the campaign start&end date-------- for the campaigns which run after 15th of July 2024

# COMMAND ----------

from pyspark.sql import functions as F
days_campaign_run = df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_platform_start_date')>= '2024-07-15').withColumn('days_campaign_run', F.datediff(F.col("campaign_platform_end_date"), F.col("campaign_platform_start_date"))).select('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','days_campaign_run')

rowsForDate =  df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_platform_start_date')>= '2024-07-15').groupBy('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date').agg(count('date'))

days_campaign_run.join(rowsForDate , on = ['country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date'], how = 'left').withColumn("days_campaign_run", col("days_campaign_run") + lit(1)).filter(col('days_campaign_run') != col('count(date)')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC need to get only one row for the campaigns run before 15 july

# COMMAND ----------

from pyspark.sql import functions as F
days_campaign_run = df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_platform_start_date')< '2024-07-15').withColumn('days_campaign_run', F.datediff(F.col("campaign_platform_end_date"), F.col("campaign_platform_start_date"))).select('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','days_campaign_run')

rowsForDate =  df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_platform_start_date')< '2024-07-15').groupBy('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date').agg(count('date'))

days_campaign_run.join(rowsForDate , on = ['country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date'], how = 'left').filter(col('days_campaign_run') != col('count(date)')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC 3.In-platform reach is not coming as a cumulative increase at the CampaignId X platform level.

# COMMAND ----------

reach_15 = df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_platform_start_date')>= '2024-07-15')


from pyspark.sql import Window
import pyspark.sql.functions as F



# Create a window partitioned by col1 and col2, ordered by date
window_spec = Window.partitionBy('campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date').orderBy('date')

# Add a lagged column to get the previous value of col3 within the window
df_with_lag = reach_15.withColumn('prev_in_platform_reach', F.lag('in_platform_reach').over(window_spec))

# Add a column that checks if the current col3 is greater than or equal to the previous col3
df_with_check = df_with_lag.withColumn(
    'is_increasing', 
    F.when(
        F.col('prev_in_platform_reach').isNull(), 
        True  # If prev_col3 is null, it's the first row, so treat as valid
    ).when(
        F.col('in_platform_reach') < F.col('prev_in_platform_reach'),
        False
    ).otherwise(
        True
    )
)
# df_with_check = df_with_lag.withColumn(
#     'is_increasing',
#     col('prev_in_platform_reach')<= col('in_platform_reach')
# )


# Filter the rows where col3 is not increasing
non_increasing_rows = df_with_check.filter(F.col('is_increasing') == False)

# Show the result
df_with_check.display()
non_increasing_rows.display()

# COMMAND ----------

df_reach.join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('campaign_desc') == 'uk_sni_rnp-p8_mw_eur_cho_brand_social_views_bid-adr_0724_#171649').display()


# COMMAND ----------



# COMMAND ----------

dff = df_fact_performance_synapse.select('gpd_campaign_id','creative_id').dropDuplicates()


# COMMAND ----------

# Get creative name and media buy name of distinct campaigns
df = df_fact_performance_synapse.select(
    "country_id", "gpd_campaign_id", "creative_id", "media_buy_id"
).distinct()
df = (
    df.join(
        dim_campaign_synapse.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
    )
    .join(dim_creative_synapse.withColumnRenamed("country_id", "country_id2"), "creative_id")
    .join(dim_mediabuy_synapse.withColumnRenamed("country_id", "country_id3"), "media_buy_id")
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

df_planned_spend_synapse.display()

# COMMAND ----------

df_planned_spend_synapse.join(dim_campaign_synapse, on = 'gpd_campaign_id', how = 'left').join(dff, on = 'gpd_campaign_id', how = 'left').join(dim_creative_synapse, on = 'creative_id', how = 'left').select(df_planned_spend_synapse.country_id, 'gpd_campaign_code',dim_campaign_synapse.campaign_desc,'creative_desc','plan_line_id','planned_budget_dollars').display()

# COMMAND ----------

opid_campaigns.display()

# COMMAND ----------

df_fact_performance_synapse.select('country_id').distinct().join(dim_country_synapse, on = "country_id", how = 'left').select("country_id",'country_desc').display()

# COMMAND ----------

fact_creativeX.select('country_id').distinct().join(dim_country_synapse, on = "country_id", how = 'left').select("country_id",'country_desc').display()

# COMMAND ----------

prod_ih_spend.select('country_id').distinct().join(dim_country_synapse, on = "country_id", how = 'left').select("country_id",'country_desc').display()
prod_ih_ingoing.select('country_id').distinct().join(dim_country_synapse, on = "country_id", how = 'left').select("country_id",'country_desc').display()

# COMMAND ----------

df_fact_performance_synapse.join(dim_product_synapse, on = "product_id", how = 'left_anti').select("product_id").distinct().display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('product_id').isin([
'DATORAMA_NL_MW_5',
'DATORAMA_GB_MW_9',
'DATORAMA_CA_MW_5',
'DATORAMA_SA_MW_4',
'DATORAMA_MX_MW_6',
'DATORAMA_AE_MW_2',
'DATORAMA_DE_MW_4'
])).select('country_id','product_id','gpd_campaign_id',
                                                'campaign_desc',
                                                      
                                                'campaign_start_date',
                                                'campaign_end_date'
                         ).dropDuplicates().display()

                         

# COMMAND ----------

df_fact_performance_synapse.join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'left').filter(col('brand').isin(['PK','Mars Inc','Tasty Bite'])).display()
# df_reach_synapse.join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'left').filter(col('brand').isin(['PK','Mars Inc','Tasty Bite'])).display()
# df_planned_spend_synapse.join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'left').filter(col('brand').isin(['PK','Mars Inc','Tasty Bite'])).display()
prod_ih_spend.join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'left').filter(col('brand').isin(['PK','Mars Inc','Tasty Bite'])).display()

# COMMAND ----------

# df_reach_synapse.join(dim_product_synapse, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
# df_planned_spend_synapse.join(dim_product_synapse, on = "product_id", how = 'left_anti').select("product_id").distinct().display()
df_planned_spend_synapse.join(dim_product_synapse, on = "product_id", how = 'left_anti').select("product_id").distinct().display()



# COMMAND ----------

dim_product_synapse.filter(col('brand').isin(['Tasty Bite',"PK",'Mars Inc'])).display()

# COMMAND ----------

dim_product_synapse.display()

# COMMAND ----------



# COMMAND ----------

df_fact_performance_synapse.filter(col('date')>='2023-06-01').filter(col('date')<='2024-05-31').join(dim_channel_synapse.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').filter(col('platform_desc').isin(['Facebook Ads'])).filter(col('campaign_desc')== 'ph_mms_ph-mw-461_mw_apac_cho_brand_olv_awareness_bid-adr_0324_rnf').display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('date')>='2023-06-01').filter(col('date')<='2024-05-31').join(dim_campaign_synapse.select('gpd_campaign_id','gpd_campaign_code','campaign_advertiser','campaign_advertiser_code'), on = 'gpd_campaign_id', how = 'left').join(dim_channel_synapse.select('channel_id','platform_desc'), on = 'channel_id', how = 'left').groupby('country_id','campaign_advertiser','campaign_advertiser_code','buying_currency','gpd_campaign_code','campaign_desc').agg(sum('impressions').alias('impressions'), sum('clicks').alias('clicks'), sum('actual_spend_local_currency').alias('actual_spend_local_currency'),sum('views').alias('views')).display()

# COMMAND ----------

dim_channel_synapse.distict('platform_desc').display()

# COMMAND ----------

dim_country_synapse.display()

# COMMAND ----------

# Get creative name and media buy name of distinct campaigns
df = df_fact_performance_synapse.select(
    "country_id", "gpd_campaign_id", "creative_id", "media_buy_id"
).distinct()
df = (
    df.join(
        dim_campaign_synapse.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
    )
    .join(dim_creative_synapse.withColumnRenamed("country_id", "country_id2"), "creative_id")
    .join(dim_mediabuy_synapse.withColumnRenamed("country_id", "country_id3"), "media_buy_id")
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
# df1 = df1.withColumn(
#     "OPID2",
#     when(
#         regexp_extract("media_buy_desc", "(?:.*OPID-)([0-9]+)", 1) != "",
#         regexp_extract("media_buy_desc", "(?:.*OPID-)([0-9]+)", 1),
#     ).otherwise(None),
# )
df1 = df1.withColumn(
    "OPID3",
    when(
        regexp_extract("creative_desc", "(?:.*opid-)([0-9]+)", 1) != "",
        regexp_extract("creative_desc", "(?:.*opid-)([0-9]+)", 1),
    ).otherwise(None),
)
# df1 = df1.withColumn(
#     "OPID4",
#     when(
#         regexp_extract("media_buy_desc", "(?:.*opid-)([0-9]+)", 1) != "",
#         regexp_extract("media_buy_desc", "(?:.*opid-)([0-9]+)", 1),
#     ).otherwise(None),
# )

# Consolidate OPID values into a single column 'OPID'
# df1 = df1.withColumn("OPID", coalesce("OPID1", "OPID2", "OPID3", "OPID4"))
df1 = df1.withColumn("OPID", coalesce("OPID1", "OPID3"))

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

# MAGIC %md
# MAGIC no OPID

# COMMAND ----------

no_opid_campaigns.count()

# COMMAND ----------

no_opid_campaigns.join(df_fact_performance_synapse.select('campaign_desc','gpd_campaign_id','product_id','channel_id','creative_id','actual_spend_dollars','actual_spend_local_currency'), on = 'campaign_desc', how = 'left').join(dim_campaign_synapse.select('gpd_campaign_id','gpd_campaign_code'),on = 'gpd_campaign_id', how = 'left').join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'left').join(dim_channel_synapse.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').join(dim_creative_synapse.select('creative_id','creative_desc'), on = 'creative_id',how = 'left').join(dim_country_synapse.select('country_id','marketregion_code'), on = 'country_id', how = 'left').groupBy('marketregion_code','country_id','campaign_desc','gpd_campaign_id','gpd_campaign_code','brand','platform_desc','creative_desc').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).filter(col('marketregion_code').isin(['LATAM','APAC'])).display()

# COMMAND ----------

opid_campaigns.join(df_planned_spend, on = 'campaign_desc', how = 'left_anti').join(df_fact_performance.select('campaign_desc','gpd_campaign_id','product_id','channel_id','creative_id','actual_spend_dollars','actual_spend_local_currency'), on = 'campaign_desc', how = 'left').join(dim_campaign.select('gpd_campaign_id','gpd_campaign_code'),on = 'gpd_campaign_id', how = 'left').join(dim_product.select('product_id','brand'), on = 'product_id', how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').join(dim_creative.select('creative_id','creative_desc'), on = 'creative_id',how = 'left').join(dim_country_synapse.select('country_id','marketregion_code'), on = 'country_id', how = 'left').groupBy('marketregion_code','country_id','campaign_desc','gpd_campaign_id','gpd_campaign_code','brand','platform_desc','creative_desc').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()
 

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')== 'US').display()

# COMMAND ----------

df_planned_spend_synapse.groupBy(
    "country_id",'gpd_campaign_id','channel_id','product_id'
).count().filter(col("count") > 1).display()

# COMMAND ----------

dim_country_synapse.display()

# COMMAND ----------

df_fact_performance_synapse.groupBy('country_id','gpd_campaign_id','campaign_desc','channel_id','product_id','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('actual_spend_local_currency').alias('actual_spend_local_currency')).withColumn("year", expr("substr(campaign_start_date, 1, 4)")).filter(col('year').isin('2023','2024')).join(df_planned_spend_synapse.select('gpd_campaign_id','channel_id','product_id','plan_line_id','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency'), on = ['gpd_campaign_id','channel_id','product_id'], how = 'left').dropDuplicates().join(dim_country_synapse.filter(~col('country_id').isin(['US','CA'])).select('country_id','marketregion_code','marketregion_desc'), on = 'country_id').select('country_id','year','gpd_campaign_id','campaign_desc','campaign_start_date','campaign_end_date','actual_spend_dollars','actual_spend_local_currency','plan_line_id','plan_start_date','plan_end_date','planned_budget_dollars','planned_budget_local_currency','marketregion_code','marketregion_desc').dropDuplicates(['gpd_campaign_id']).display()

# COMMAND ----------

df_planned_spend_synapse.display()

# COMMAND ----------

# count of campaigns for a single plan line id
df_planned_spend_synapse.select('gpd_campaign_id','plan_line_id').groupBy(['plan_line_id']).count().display()
# .filter(col("count") > 1).display()

# COMMAND ----------

df_planned_spend_synapse.select('country_id','gpd_campaign_id','plan_line_id','campaign_desc').display()

# COMMAND ----------

df_fact_performance_synapse.filter(col("country_id") == "US").join(dim_channel_synapse, on = "channel_id", how = 'left_anti').select("channel_id").distinct().display()


# COMMAND ----------

df_fact_performance_synapse.display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='US').filter(col('date') >= '20230101').filter(col('date') <= '20230630').join(dim_product_synapse.select('product_id','brand'),on = 'product_id', how = 'inner').filter(col('publisher').isin('Instagram','Facebook','SnapChat')).groupBy('gpd_campaign_id','campaign_desc','brand','publisher','date').agg(sum('actual_spend_dollars').alias('actual_spend_dollars'),sum('actual_spend_local_currency').alias('actual_spend_local_currency'), sum('impressions').alias('impressions'), sum('clicks').alias('clicks')).display()



# COMMAND ----------

dim_channel_synapse.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ########### brand X monthly -spend and planned spend

# COMMAND ----------

df_fact_performance_synapse.withColumn("date", to_date("date", "yyyyMMdd")).withColumn("month_end_date", last_day(col("date"))).join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'inner').groupBy('brand','month_end_date').agg(sum('actual_spend_dollars').alias('actual_spend_dollars')).filter(col('month_end_date')<='2023-06-30').filter(col('month_end_date')>='2022-01-01').display()

# COMMAND ----------

df_fact_performance_synapse.withColumn("date", to_date("date", "yyyyMMdd")).withColumn("month_end_date", last_day(col("date"))).join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'inner').groupBy('country_id','gpd_campaign_id','channel_id','product_id','brand','month_end_date').agg(sum('actual_spend_dollars').alias('actual_spend_dollars')).display()

# COMMAND ----------

df_planned_spend_synapse.select('country_id','gpd_campaign_id','channel_id','product_id','plan_start_date','plan_end_date','planned_budget_dollars').withColumn("plan_start_date", to_date("plan_start_date", "yyyyMMdd")).withColumn("plan_end_date", to_date("plan_end_date", "yyyyMMdd")).withColumn("date", explode(expr("sequence(plan_start_date, plan_end_date, interval 1 day)"))).withColumn("num_days", datediff(col("plan_end_date"), col("plan_start_date")) + 1).withColumn("planned_budget_dollars_per_day", col("planned_budget_dollars") / col("num_days")).withColumn("month_end_date", last_day(col("date"))).join(dim_product_synapse.select('product_id','brand'), on = 'product_id', how = 'inner').groupBy('country_id','gpd_campaign_id','channel_id','product_id','brand','month_end_date').agg(sum('planned_budget_dollars_per_day').alias('planned_budget_dollars')).display()


# COMMAND ----------

df_fact_performance

# COMMAND ----------

df_planned_spend_synapse.display()


# COMMAND ----------

df_fact_performance_synapse.filter(col('date')>= '20230101').groupBy('date').count().display()

# COMMAND ----------

df_reach_synapse.display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('date') <= '28102023').join(dim_product_synapse.select('product_id','brand'), on= 'product_id',how= 'inner').withColumn("year", expr("substr(date, 1, 4)")).join(dim_country_synapse,on = 'country_id',how = 'inner').filter(col('gpd_region_desc') == 'EU/CEABT').groupBy('country_id','brand').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('date') <= '28102023').join(dim_product_synapse.select('product_id','brand'), on= 'product_id',how= 'inner').filter(col('country_id')=='DE').select('brand').dropDuplicates().display()

# COMMAND ----------

df_fact_performance_synapse.filter((col('date') >= '01012022') & (col('date') <= '24102023')).withColumn("year", expr("substr(date, 1, 4)")).join(dim_country_synapse,on = 'country_id',how = 'inner').filter(col('gpd_region_desc') == 'EU/CEABT').groupBy('country_id').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('campaign_desc') =='uk_exg_work&study-p5-p6_mw_eur_gum_brand_social_awareness_bid-nadr_0423_na').join(dim_mediabuy_synapse.select('media_buy_id','audience_name','placement_type'),on= 'media_buy_id',how= 'inner').display()


# COMMAND ----------

df_fact_performance.filter(col('campaign_desc') =='uk_exg_work&study-p5-p6_mw_eur_gum_brand_social_awareness_bid-nadr_0423_na').join(dim_mediabuy.select('media_buy_id','audience_desc','placement_type'),on= 'media_buy_id',how= 'inner').display()


# COMMAND ----------

df_fact_performance_synapse.join(dim_country_synapse.filter(col('gpd_region_desc').isin('EU/CEABT','GEM')),on = 'country_id',how = 'inner').join(dim_channel_synapse.select('channel_id','channel_desc','platform_desc'),on = 'channel_id',how= 'inner').groupBy('country_id','gpd_campaign_id','campaign_desc','campaign_start_date','campaign_end_date','channel_desc').agg(sum('clicks'),sum('video_completion_25'),sum('video_completion_50'),sum('video_completion_75'),sum('video_fully_played'),sum('views')).filter((col('sum(video_fully_played)')!=0 ) & (col('sum(video_completion_25)')!=0) &  ( col('sum(video_completion_50)')!=0 ) & (col('sum(video_completion_75)')!=0)).display()

# COMMAND ----------

dim_channel_synapse.display()

# COMMAND ----------

df_fact_performance_synapse.groupBy("country_id").agg(
    min("date"), max("date"), count("date")
).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('campaign_desc')=='uk_exg_work&study-p5-p6_mw_eur_gum_brand_social_awareness_bid-nadr_0423_na').select('creative_id').dropDuplicates().join(dim_creative_synapse,on= 'creative_id',how= 'inner').display()

# COMMAND ----------

df_fact_performance.filter(col('campaign_desc')=='uk_exg_work&study-p5-p6_mw_eur_gum_brand_social_awareness_bid-nadr_0423_na').select('creative_id').dropDuplicates().join(dim_creative,on= 'creative_id',how= 'inner').display()

# COMMAND ----------

df_fact_performance.filter(col('campaign_desc')=='gb_ecg_p2-3-refreshers_mw_eur_cho_brand_social_awareness_bid-nadr_0323_110025').select('creative_id').dropDuplicates().join(dim_creative,on= 'creative_id',how= 'inner').display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('campaign_desc')=='gb_ecg_p2-3-refreshers_mw_eur_cho_brand_social_awareness_bid-nadr_0323_110025').select('creative_id').dropDuplicates().join(dim_creative_synapse,on= 'creative_id',how= 'inner').display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id').isin('SA','CA','EG','AE','US')).groupBy('country_id').agg(min('date'),max('date')).display()

# COMMAND ----------

df_reach_synapse.select('country_id').dropDuplicates().display()

# COMMAND ----------

df_reach_synapse.join(dim_channel_synapse.select('channel_id','platform_desc'),on= 'channel_id').select('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','in_platform_reach').groupBy('country_id','campaign_desc','platform_desc').agg(sum('in_platform_reach')).display()

# COMMAND ----------

df_reach_synapse.filter(col('country_id')=='SA').display()

# COMMAND ----------

df_reach_synapse.filter(col('country_id')=='MX').display()

# COMMAND ----------

df_reach_synapse

# COMMAND ----------

df_reach_synapse.filter(col('campaign_platform_end_date')< '20231004').groupBy('country_id','gpd_campaign_id').agg(sum('in_platform_reach')).display()

# COMMAND ----------

dim_country_synapse.filter(col('country_id').isin(['CA','CR','EC','KE','MX','NZ','PA','PH','SA'])).display()

# COMMAND ----------


l=[row['gpd_campaign_id'] for row in df_reach_synapse.filter(col('campaign_platform_end_date')< '20231004').filter(col('country_id')=='PA').collect()]
print(l)

# COMMAND ----------

df_fact_performance_synapse.groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='NL').groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('date')< '20231004').groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('date')<= '20231003').groupBy("country_id").agg(
    min("date"), max("date"), count("date")
).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('date')< '20231004').groupBy('country_id').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='BR').orderBy(col("date").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### planned spend
# MAGIC
# MAGIC

# COMMAND ----------

df_planned_spend.withColumn("year", expr("substr(plan_end_date, 1, 4)")).groupBy('country_id','year').count().display()

# COMMAND ----------

df_planned_spend_synapse.withColumn("year", expr("substr(plan_end_date, 1, 4)")).groupBy('country_id','year').count().display()

# COMMAND ----------

df_planned_spend_synapse.select('country_id',
 'gpd_campaign_id',
 'product_id',
 'plan_line_id','planned_budget_dollars','planned_budget_local_currency').dropDuplicates().groupBy('country_id').agg(sum('planned_budget_dollars'),sum('planned_budget_local_currency')).display()

# COMMAND ----------

df_planned_spend.select('country_id',
 'gpd_campaign_id',
 'product_id',
 'plan_line_id','planned_budget_dollars','planned_budget_local_currency').dropDuplicates().groupBy('country_id').agg(sum('planned_budget_dollars'),sum('planned_budget_local_currency')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### reach
# MAGIC

# COMMAND ----------

df_reach.withColumn("year", expr("substr(campaign_platform_end_date, 1, 4)")).groupBy('country_id','year').count().display()

# COMMAND ----------

df_reach_synapse.withColumn("year", expr("substr(campaign_platform_end_date, 1, 4)")).groupBy('country_id','year').count().display()

# COMMAND ----------

df_reach_synapse.join(dim_channel_synapse.select('channel_id','platform_desc'),on= 'channel_id').select('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','in_platform_reach').groupBy('country_id','campaign_desc','platform_desc').agg(sum('in_platform_reach')).display()

# COMMAND ----------

df_reach.join(dim_channel.select('channel_id','platform_desc'),on= 'channel_id').select('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','in_platform_reach').groupBy('country_id','campaign_desc','platform_desc').agg(sum('in_platform_reach')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_performance
# MAGIC

# COMMAND ----------

df_fact_performance_synapse.withColumn("year", expr("substr(date, 1, 4)")).groupBy('country_id','year').count().display()

# COMMAND ----------

df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).groupBy('country_id','year').count().display()

# COMMAND ----------

df_fact_performance_synapse.groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance.groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),count('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance_synapse.filter(col('country_id')=='TW').groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),count('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------



# COMMAND ----------

df_planned_spend_synapse.select('country_id',
 'gpd_campaign_id',
 'product_id',
 'plan_line_id','planned_budget_dollars','planned_budget_local_currency').dropDuplicates().groupBy('country_id').agg(sum('planned_budget_dollars'),sum('planned_budget_local_currency')).display()

# COMMAND ----------

dim_product_synapse.groupBy('product','brand','portfolio').count().display()

# COMMAND ----------

## 
dim_mediabuy_synapse.display()


# COMMAND ----------

dim_campaign_synapse.display()

# COMMAND ----------

## creative
dim_creative_synapse.display()

# COMMAND ----------

dim_creative_synapse.groupBy('country_id').count().display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### synapse

# COMMAND ----------

# print((df_reach_synapse.filter(col('country_id')!='MX').count(), len(df_reach_synapse.columns)))
# print((df_planned_spend_synapse.filter(col('country_id')!='MX').count(), len(df_planned_spend_synapse.columns)))
print((df_fact_performance_synapse.filter(col('country_id')!='MX').count(), len(df_fact_performance_synapse.columns)))

# COMMAND ----------

print((dim_campaign_synapse.filter(col('country_id')!='MX').count(), len(dim_campaign_synapse.filter(col('country_id')!='MX').columns)))
print((dim_channel_synapse.filter(col('country_id')!='MX').count(), len(dim_channel_synapse.filter(col('country_id')!='MX').columns)))
print((dim_creative_synapse.filter(col('country_id')!='MX').count(), len(dim_creative_synapse.filter(col('country_id')!='MX').columns)))
print((dim_strategy_synapse.filter(col('country_id')!='MX').count(), len(dim_strategy_synapse.filter(col('country_id')!='MX').columns)))
print((dim_mediabuy_synapse.filter(col('country_id')!='MX').count(), len(dim_mediabuy_synapse.filter(col('country_id')!='MX').columns)))
print((dim_site_synapse.filter(col('country_id')!='MX').count(), len(dim_site_synapse.filter(col('country_id')!='MX').columns)))
print((dim_country_synapse.filter(col('country_id')!='MX').count(), len(dim_country_synapse.filter(col('country_id')!='MX').columns)))
print((dim_product_synapse.filter(col('country_id')!='MX').count(), len(dim_product_synapse.filter(col('country_id')!='MX').columns)))





# COMMAND ----------

df_fact_performance_synapse.groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),count('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sql
# MAGIC

# COMMAND ----------

print((df_reach.filter(col('country_id')!='MX').count(), len(df_reach.columns)))
print((df_planned_spend.filter(col('country_id')!='MX').count(), len(df_planned_spend.filter(col('country_id')!='MX').columns)))
# print((df_fact_performance.filter(col('country_id')!='MX').count(), len(df_fact_performance.filter(col('country_id')!='MX').columns)))


# COMMAND ----------

print((df_fact_performance.filter(col('country_id')!='MX').count(), len(df_fact_performance.filter(col('country_id')!='MX').columns)))


# COMMAND ----------

print((dim_campaign.filter(col('country_id')!='MX').count(), len(dim_campaign.filter(col('country_id')!='MX').columns)))
print((dim_channel.filter(col('country_id')!='MX').count(), len(dim_channel.filter(col('country_id')!='MX').columns)))
print((dim_creative.filter(col('country_id')!='MX').count(), len(dim_creative.filter(col('country_id')!='MX').columns)))
print((dim_strategy.filter(col('country_id')!='MX').count(), len(dim_strategy.filter(col('country_id')!='MX').columns)))
print((dim_mediabuy.filter(col('country_id')!='MX').count(), len(dim_mediabuy.filter(col('country_id')!='MX').columns)))
print((dim_site.filter(col('country_id')!='MX').count(), len(dim_site.filter(col('country_id')!='MX').columns)))
print((dim_country.filter(col('country_id')!='MX').count(), len(dim_country.filter(col('country_id')!='MX').columns)))
print((dim_product.filter(col('country_id')!='MX').count(), len(dim_product.filter(col('country_id')!='MX').columns)))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sql DB

# COMMAND ----------

def jdbc_connection(dbtable):
    url = "jdbc:sqlserver://marsanalyticsprodsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationprodsqldb"
    user_name = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databricksusername"
    )
    password = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databrickspassword"
    )
    df = (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user_name)
        .option("password", password)
        .option("authentication", "ActiveDirectoryPassword")
        .load()
    )
    return df

# COMMAND ----------

df_reach = jdbc_connection('mm.vw_gpd_fact_reach')
df_fact_performance = jdbc_connection('mm.vw_gpd_fact_performance')
df_planned_spend = jdbc_connection('mm.vw_gpd_fact_plannedspend')
dim_campaign = jdbc_connection('mm.vw_gpd_dim_campaign')
dim_channel = jdbc_connection('mm.vw_gpd_dim_channel')
dim_creative = jdbc_connection('mm.vw_gpd_dim_creative')
dim_strategy = jdbc_connection('mm.vw_gpd_dim_strategy')
dim_mediabuy = jdbc_connection('mm.vw_gpd_dim_mediabuy')
dim_site = jdbc_connection('mm.vw_gpd_dim_site')
dim_country = jdbc_connection('mm.vw_gpd_dim_country')
dim_product = jdbc_connection('mm.vw_gpd_dim_product')

# COMMAND ----------

print((df_reach.count(), len(df_reach.columns)))

# COMMAND ----------

df_reach.select('country_id').dropDuplicates().display()

# COMMAND ----------

df_reach.join(dim_channel.select('channel_id','platform_desc'),on= 'channel_id').select('country_id','campaign_desc','platform_desc','campaign_platform_start_date','campaign_platform_end_date','in_platform_reach').groupBy('country_id','campaign_desc','platform_desc').agg(sum('in_platform_reach')).display()

# COMMAND ----------

df_reach.filter(col('country_id')=='SA').display()

# COMMAND ----------

df_reach.filter(col('country_id')=='MX').display()

# COMMAND ----------

df_reach.join(dim_channel,on= 'channel_id').groupBy('gpd_campaign_id','platform_desc').agg(sum('in_platform_reach')).display()

# COMMAND ----------

df_reach.filter(col('country_id')=='US').display()

# COMMAND ----------

df_reach.filter(col('campaign_platform_end_date')< '20231004').groupBy('country_id','gpd_campaign_id').agg(sum('in_platform_reach')).display()

# COMMAND ----------

df_reach.filter(col('campaign_platform_end_date')< '20231004').filter(col('country_id')=='PA').filter(~col('gpd_campaign_id').isin(l)).display()

# COMMAND ----------

dim_product.columns

# COMMAND ----------

df_planned_spend.select('country_id',
 'gpd_campaign_id',
 'product_id',
 'plan_line_id','planned_budget_dollars','planned_budget_local_currency').dropDuplicates().groupBy('country_id').agg(sum('planned_budget_dollars'),sum('planned_budget_local_currency')).display()

# COMMAND ----------

dim_product.groupBy('product','brand','portfolio').count().display()

# COMMAND ----------

df_fact_performance.filter(col('date')< '20231004').groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),count('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance.filter(col('date')< '20231004').groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),count('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance.filter(col('date')< '20231004').groupBy('country_id').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

df_fact_performance.groupBy('country_id').agg(count('actual_spend_dollars')).display()

# COMMAND ----------

df_fact_performance.groupBy('country_id','campaign_desc','campaign_start_date','campaign_end_date').agg(sum('actual_spend_dollars'),sum('actual_spend_local_currency'),sum('clicks'),sum('impressions')).display()

# COMMAND ----------

## 
dim_mediabuy.display()


# COMMAND ----------

dim_campaign.display()

# COMMAND ----------

dim_creative.display()

# COMMAND ----------

print((dim_country.count(), len(dim_country.columns)))

# COMMAND ----------

dim_country.display()

# COMMAND ----------

dim_product.columns

# COMMAND ----------

dim_product_synapse.columns

# COMMAND ----------

df_reach
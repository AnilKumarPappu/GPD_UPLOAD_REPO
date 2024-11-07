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
# dim_cal = jdbc_connection('mm.vw_dim_calendar')             

# COMMAND ----------

dim_calendar = jdbc_connection('mm.vw_dim_calendar')             

# COMMAND ----------

df_fact_performance.filter(col('campaign_desc').isin(['uk_dsg_p4-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0323_na',
'uk_twx_p4-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0323_na',
'uk_dsg_p4-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0323_na',
'uk_twx_p4-rnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0323_na',
'fr_mal_p4-vnp_mw_eur_cho_brand_olv_awareness_bid-nadr_0323_na',
'de_wav_p9-12-social-reach-p13extension_mw_eur_cho_brand_social_awareness_bid-nadr_1223_#140249',
'nl_twx_p4-camping_mw_eur_cho_brand_olv_awareness_bid-nadr_0323_na',
'nl_twx_camping-p4_mw_eur_cho_brand_social_views_bid-nadr_0323_#120845',
'nl_twx_camping-p4_mw_eur_cho_brand_social_views_bid-nadr_0323_#120842',
'de_exg_p7-vnp_mw_eur_gum_brand_olv_awareness_bid-nadr_0623_olv-130829-cobranding',
'de_exg_p7-vnp_mw_eur_gum_brand_olv_awareness_bid-nadr_0623_olv-130829',
'de_twx_social-reach-layer-p5_mw_eur_cho_brand_social_awareness_bid-nadr_0423_#121680',
'de_wav_social-reach-rebound-p9-12(extension)_mw_eur_cho_brand_social_awareness_bid-nadr_1223_#140251',
'de_twx_social-reach-layer-p5_mw_eur_cho_brand_social_awareness_bid-nadr_0423_#121678',
'uk_exg_chew-good-p1-lens_mw_eur_gnm_brand_social_awareness_bid-nadr_0124_152822',
'DE Extra GYDB P2 Social reach - Change name',
'de_exg_p8-13-rebound-addressable-social-reach_mw_eur_gum_brand_social_awareness_bid-adr_0923_pulse-io141172-Premature'
])).select('gpd_campaign_id','campaign_desc','creative_id','media_buy_id').drop_duplicates().join(dim_creative.withColumnRenamed('naming_convention_flag','naming_convention_flag_creative'), on = "creative_id", how = 'left').join(dim_mediabuy.withColumnRenamed('naming_convention_flag','naming_convention_flag_media_buy'), on = "media_buy_id", how = 'left').select('campaign_desc','creative_id','media_buy_id','creative_desc','naming_convention_flag_creative','media_buy_desc','naming_convention_flag_media_buy').display()

# COMMAND ----------

dim_creative.filter(col('naming_convention_flag') == 0).select('creative_desc','naming_convention_flag').display()

# COMMAND ----------

dim_country.display()

# COMMAND ----------

from pyspark.sql.functions import col, date_format


df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).withColumn("date_as_string", date_format(col("date"), 'yyyyMMdd')).join(dim_calendar.select('calendar_date','mars_period').withColumnRenamed("calendar_date", 'date_as_string'), on = 'date_as_string', how = 'left').filter(col('country_id') == 'VN').filter(col('year').isin(['2023'])).groupBy('country_id', 'year', 'mars_period').agg(sum('actual_spend_dollars')).display()
# 

# COMMAND ----------

df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year').isin(['2023','2024'])).groupBy('country_id', 'year').agg(sum('actual_spend_dollars')).display()

# COMMAND ----------

# check the australia 2023 in access layer
# df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year').isin(['2023'])).filter(col('country_id') == 'AU').display()
df_fact_performance.filter(col('country_id') == 'AU').join(dim_product, on = 'product_id', how = 'left').filter(col('brand').isin(["M&M's",'M&Ms','M&MS']) ).display()
# df_fact_performance.join(dim_campaign, on = 'gpd_campaign_id', how = 'left').filter(col('campaign_name') == 'au_mms_au-mw-148-aor2024_mw_apac_con_brand_olv').display()

# COMMAND ----------

df_ALL_countries = df_fact_performance.withColumn("year", expr("substr(date, 1, 4)")).filter(col('year').isin(['2023','2024'])).groupBy('country_id', 'gpd_campaign_id','channel_id','product_id').agg(sum('clicks').alias('clicks'), sum('impressions').alias('impressions'), sum('views').alias('views'), sum('actual_spend_dollars').alias('actual_spend_dollars')).join(dim_channel.select('channel_id','platform_desc') , on = 'channel_id', how = 'inner').join(df_reach.select('country_id', 'gpd_campaign_id','channel_id','product_id','in_platform_reach').dropDuplicates(), on = ['country_id', 'gpd_campaign_id','channel_id','product_id'], how = 'inner')
# .filter(col('country_id') == 'SA')
# .display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import corr
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

dim_product.select('segment').distinct().display()

# COMMAND ----------

df_SA = df_ALL_countries.filter(col('country_id') == 'SA')
df_CA = df_ALL_countries.filter(col('country_id') == 'CA')
df_CL = df_ALL_countries.filter(col('country_id') == 'CL')

df_face = df_ALL_countries.filter(col('platform_desc') == 'FACEBOOK ADS')
df_goo = df_ALL_countries.filter(col('platform_desc') == 'GOOGLE ADWORDS')
df_pint = df_ALL_countries.filter(col('platform_desc') == 'PINTEREST ADS')


# COMMAND ----------

df_face.display()

# COMMAND ----------

######## All _countries

numeric_cols = [col for col, dtype in df_ALL_countries.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_ALL_countries.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()




# COMMAND ----------

########## CA
numeric_cols = [col for col, dtype in df_CA.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_CA.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------

########### CL
numeric_cols = [col for col, dtype in df_CL.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_CL.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------

########### SA
numeric_cols = [col for col, dtype in df_SA.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_SA.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######## Platform wise

# COMMAND ----------

################ Facebook
numeric_cols = [col for col, dtype in df_face.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_face.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------

###############   GOOGLE
numeric_cols = [col for col, dtype in df_goo.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_goo.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------

#################  PINTREST
numeric_cols = [col for col, dtype in df_face.dtypes if dtype == 'double']  # Filter numeric columns
correlation_matrix = pd.DataFrame({col: [df_face.corr(col, other) for other in numeric_cols] for col in numeric_cols},
                                  index=numeric_cols)

# Visualize the correlation matrix using a heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', square=True)
plt.title('Correlation Matrix')
plt.show()


# COMMAND ----------

df_pint.display()

# COMMAND ----------

df_fact_performance.select('gpd_campaign_id','date', 'country_id','media_buy_id','creative_id','channel_id','campaign_desc','actual_spend_dollars').withColumn("year", expr("substr(date, 1, 4)")).filter(col('year')== '2024').join(dim_mediabuy.select('media_buy_id','audience_desc','audience_type'), on = 'media_buy_id', how = 'inner' ).join(dim_creative.select('creative_id','creative_desc','creative_type'), on = 'creative_id', how= 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id' , how = 'inner').filter(col('country_id').isin(['US'])).filter(col('platform_desc') == 'FACEBOOK ADS').display()
#Campaign - Actual spend Values - Campaign - audience name , audience type , creative name , creative type

# facebook, us, 

# COMMAND ----------

# DATORAMA_US_3620
dim_creative.filter(col('creative_id') == 'DATORAMA_US_16').display()

# COMMAND ----------

df_fact_performance.filter(col('campaign_desc')== 'br_sni_p13-hunger-bars_mw_latam_cho_brand_social_reach_bid-dda_1223_youtube-15s').display()

# COMMAND ----------

# for active campaigns the campaigns with campaign_end_date not equal to the max(date) according to country should be 0
df_fact_performance.select('campaign_desc','campaign_start_date','campaign_end_date','campaign_status','country_id').join(df_fact_performance.groupBy('country_id').agg(max('date')), on = 'country_id',how = 'inner').filter(col('campaign_status')=='active').filter(col('campaign_end_date') != col('max(date)')).display()

# COMMAND ----------

# for inactive campaigns the campaigns with campaign_end_date grater than or equal to the max(date) according to country should be 0
df_fact_performance.select('campaign_desc','campaign_start_date','campaign_end_date','campaign_status','country_id').join(df_fact_performance.groupBy('country_id').agg(max('date')), on = 'country_id',how = 'inner').filter(col('campaign_status')=='inactive').filter(col('campaign_end_date') >= col('max(date)')).display()

# COMMAND ----------

df_fact_performance.groupBy('country_id').agg(max('date').alias('latest_date')).display()

# COMMAND ----------

# Germany 2024 TICKTOK
df_fact_performance.filter(col('country_id')== 'DE').withColumn("year", expr("substr(date, 1, 4)")).filter(col('year')== '2024').join(dim_channel.select('channel_id','platform_desc'),on = 'channel_id', how = 'left').filter(col('platform_desc')== 'TIKTOK ADS').join(dim_campaign, on = 'gpd_campaign_id', how = 'inner').display()
# .filter(col('campaign_name') == 'de_sni_addressable-reach-p1-6_mw_eur_cho_brand_social').display()
# .groupBy('platform_desc').count().display()
# .select(col('platform_desc')).distinct().display()

# COMMAND ----------

dim_country.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# df_fact_performance.join(dim_campaign, on = "gpd_campaign_id", how = 'left_anti').display()
# df_fact_performance.join(dim_product, on = "product_id", how = 'left_anti').display()
df_fact_performance.join(dim_channel, on = "channel_id", how = 'left_anti').display()
df_fact_performance.join(dim_creative, on = "creative_id", how = 'left_anti').display()
df_fact_performance.join(dim_mediabuy, on = "media_buy_id", how = 'left_anti').display()
df_fact_performance.join(dim_strategy, on = "strategy_id", how = 'left_anti').display()
df_fact_performance.join(dim_site, on = "site_id", how = 'left_anti').display()


# COMMAND ----------

df_fact_performance.columns
# dim_channel = jdbc_connection('mm.vw_mw_gpd_dim_channel')
# dim_creative = jdbc_connection('mm.vw_mw_gpd_dim_creative')
# dim_strategy = jdbc_connection('mm.vw_mw_gpd_dim_strategy')
# dim_mediabuy = jdbc_connection('mm.vw_mw_gpd_dim_mediabuy')
# dim_site = jdbc_connection('mm.vw_mw_gpd_dim_site')
# dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
# dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')

# COMMAND ----------

df_fact_performance.join(dim_product, on = "product_id", how = 'left').filter(col("portfolio").isNull()).select("product_id", ).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### reach granularity

# COMMAND ----------

df_reach.join(
    dim_campaign.withColumnRenamed("country_id", "country_id1"), "gpd_campaign_id"
).join(
    dim_channel.withColumnRenamed("country_id", "country_id2"), "channel_id"
).groupBy(
    "gpd_campaign_id", "platform_desc"
).count().filter(
    col("count") > 1
).display()

# df_reach.groupBy("gpd_campaign_id","channel_id","product_id").count().filter(
#     col("count") > 1
# ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### planned spend granularity

# COMMAND ----------

df_planned_spend.groupBy("gpd_campaign_id","channel_id","product_id","plan_line_id").count().filter(
    col("count") > 1
).display()

# COMMAND ----------

df_reach.filter(col('gpd_campaign_id')=='DATORAMA_FR_54096841').display()

# COMMAND ----------

x = list(set(
    ['CT',
'DMM',
'CML',
'SPH',
'PH',
'FSH',
'CHO',
'CON',
'GUM',
'ICE',
'MIN',
'RTH',
'MF-HS',
'MF-FS',
'TB',
'TB-FS',
'SOC',
'DRY',
'OTH',
'care-treat',
'dog-mm',
'cat-mml',
'Ferret',
'Fish',
'choc',
'conf',
'ice-cream',
'mints',
'other',
'sub-snacks',
'dolmio',
'Care and treats',
'Dog Main Meal',
'Cat Main Meal and Litter',
'Super Premium Health',
'Ferret',
'Fish',
'Chocolate',
'Gum/Mints',
'Fruity Confections',
'Ready to Heat',
'Masterfoods Herbs and Spices',
'Masterfoods Foodservice',
'Tasty Bite',
'Tasty Bite Foodservice',
'Seeds of Change',
'Dry',
'Other',
'CT',
'DMM',
'CML',
'SPH',
'PH',
'FSH',
'CHO',
'GNM',
'FRC',
'RTH',
'MF-HS',
'MF-FS',
'TB',
'TB-FS',
'SOC',
'DRY',
'OTH',

]
))

# COMMAND ----------

t = ""
for i in x:
    t += i+', '

# COMMAND ----------

t

# COMMAND ----------

df_fact_performance.select('country_id').distinct().display()

# COMMAND ----------

dim_country.display()

# COMMAND ----------

# MAGIC %md
# MAGIC --> Filtering countries as per our requirements. Skip if all countries are needed

# COMMAND ----------

countries = [

]

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
# MAGIC ## Data Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Check if the primary keys are valid in dim tables & their count
# MAGIC --> Since the tables are following star schema, it's important to check if the primary keys are holding correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Data present in Datorama should be latest.
# MAGIC --> Fact performance table refreshes daily on the asset layer. So the dates should be latest in access layer as well.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. There shouldn't be Multiple budgets for same plan line ID
# MAGIC --> As seen from analysis, the planned budget should be unique to a plan line ID. In case of multiple occurences, the logic doesn't holds true and the dashboard breaks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Reach frequency in data should match with calculated frequency
# MAGIC --> Frequency = Total number of impressions / Total reach <br>
# MAGIC     Impressions are aggregated on platform level from fact_performance which are the divided by the in_platform_reach. If the outcome is not same as in the frequency column, there is an error in data transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. For all campaigns, campaign_platform start and end dates must fall b/w campaign start and end date of each campaign<br>
# MAGIC --> Asserting the start and end date of a campaign/platform with min and max date of data availability.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. There should be no duplicate rows any tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. There should not be a High Variance b/w Actual and Planned Spend for countries
# MAGIC --> The planned amount and actual spend is expected to be in similar range. Anything above 110% and below 30% is an outlier.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. The Planned Spend LC/USD ratio should be similar to Actual Spend LC/USD ratio, to show consistent currency conversion
# MAGIC --> The USD vs Local ratio should be somewhat similar among actual and planned spend. Since the data is only a couple years old, the currency shouldn't appreciate or depreciate to a huge extent compared to USD. THe ratio should make sense with current conversion ratio found on Google.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Check null in Brand, Region

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. OPID count
# MAGIC --> Since OPID is the joining column of a campaign to it's planned budget and we know that it is either extracted from creative or mediabuy name. So here we are trying to check the number of campaign that do not have even a single OPID.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Granularity of performance & reach data
# MAGIC --> Although fact_performance data is at a much granular level of site or strategy, it is still a daily level data. So we need to check that for given granularity, there is a unique row for all the metrices.<br>
# MAGIC     In reach table, the granularity goes to campaign_key x platform, meaning each combination of these should've only one reach.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Countries should be consistent in all fact tables
# MAGIC --> It is expected that the data for countries should be consistent among all tables. If not, it should be raised to the source.

# COMMAND ----------

# MAGIC %md
# MAGIC ###13. There shouldn't be any null values in any fact tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. Spend shouldn't be 0 at year level
# MAGIC --> A sense check to make sure the planned spend and actual spend shouldn't be 0 at year level for any country.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. Naming Convention flat files
# MAGIC --> For content health, proper naming convention should be followed for all the campaign names, creative names and mediabuy names strating from 2023 Jan 1. This code will generate the flat files which can be later analysed manually or by code.

# COMMAND ----------


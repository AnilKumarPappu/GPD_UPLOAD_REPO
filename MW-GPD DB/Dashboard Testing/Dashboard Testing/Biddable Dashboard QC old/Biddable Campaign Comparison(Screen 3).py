# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.column import *

# COMMAND ----------

# prod
def jdbc_connection(dbtable):
    url = 'jdbc:sqlserver://marsanalyticsprodsqlsrv.database.windows.net:1433;database=globalxsegsrmdatafoundationprodsqldb'
    user_name =dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databricksusername')
    password = dbutils.secrets.get(scope = 'mwoddasandbox1005devsecretscope', key = 'databrickspassword')
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

df_planned_spend = jdbc_connection('mm.vw_gmd_fact_plannedspend')
df_fact_performance = jdbc_connection('mm.vw_gmd_fact_performance')
df_dim_country = jdbc_connection('mm.vw_gmd_dim_country')
df_dim_product = jdbc_connection('mm.vw_gmd_dim_product')
df_dim_calendar = jdbc_connection('mm.vw_dim_calendar')
df_reach = jdbc_connection('mm.vw_gmd_fact_reach')
df_dim_campaign = jdbc_connection('mm.vw_gmd_dim_campaign')

# COMMAND ----------

df_fact_performance = df_fact_performance.filter(col('platform')!='Campaign Manager')

# COMMAND ----------

df_fact_performance.groupBy('country_id').agg(min(col('date')), max(col('date')), count(col('date'))).display()

# COMMAND ----------

# Display all unique campaign name with given country_id

df_fact_performance.filter(col('country_id')=='MX').select('campaign_name').drop_duplicates().display()

# COMMAND ----------

df_dim_country.filter(col('marketregioncode')=='LATAM').select('country_id').drop_duplicates().display()

# COMMAND ----------

# All Values are in local Currency.




def sample_local(campaign1, campaign2):
    df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
    return df_fact_performance2.filter((col('campaign_name') == campaign1)|(col('campaign_name')==campaign2))\
                        .groupBy('channel','platform','campaign_name')\
                        .agg(sum('Impressions').alias('Impressions'),\
                             sum('views').alias('views'),\
                             sum('actual_spend_local_currency').alias('actual_spend_local_currency'),\
                             #sum('actual_spend_dollars').alias('actual_spend_dollars'),\
                            sum('video_fully_played').alias('video_fully_played'),\
                            sum('video_completions_25').alias('video_completions_25'),\
                             sum('video_completions_50').alias('video_completions_50'),\
                             sum('video_completions_75').alias('video_completions_75'))\
                        .withColumn('VTR', col('views')/col('Impressions'))\
                        .withColumn('VCR', col('video_fully_played')/col('views'))\
                        .withColumn('VCR25', col('video_completions_25')/col('views'))\
                        .withColumn('VCR50', col('video_completions_50')/col('views'))\
                        .withColumn('VCR75', col('video_completions_75')/col('views'))\
                        .withColumn('CPM', (col('actual_spend_local_currency')/col('impressions'))*1000)\
                        .withColumn('CPCV', col('actual_spend_local_currency')/col('video_fully_played'))


# COMMAND ----------

# All Values are in USD.

def sample_usd(campaign1, campaign2):
    df_fact_performance2 = df_fact_performance.join(df_dim_product,  'product_id','left')
    return df_fact_performance2.filter((col('campaign_name') == campaign1)|(col('campaign_name')==campaign2))\
                        .groupBy('channel','platform','campaign_name')\
                        .agg(sum('Impressions').alias('Impressions'),\
                             sum('views').alias('views'),\
                             sum('actual_spend_dollars').alias('actual_spend_dollars'),\
                            sum('video_fully_played').alias('video_fully_played'),\
                            sum('video_completions_25').alias('video_completions_25'),\
                             sum('video_completions_50').alias('video_completions_50'),\
                             sum('video_completions_75').alias('video_completions_75'))\
                        .withColumn('VTR', col('views')/col('Impressions'))\
                        .withColumn('VCR', col('video_fully_played')/col('views'))\
                        .withColumn('VCR25', col('video_completions_25')/col('views'))\
                        .withColumn('VCR50', col('video_completions_50')/col('views'))\
                        .withColumn('VCR75', col('video_completions_75')/col('views'))\
                        .withColumn('CPM', (col('actual_spend_dollars')/col('impressions'))*1000)\
                        .withColumn('CPCV', col('actual_spend_dollars')/col('video_fully_played'))


# COMMAND ----------

#EMEA
# campaign1 = "EG_Snickers_Matchday Reminder P12_1122"
# campaign2 = "EGY_Snickers_Matchday Reminder P11_1022"

campaign1 = "MX_M&Ms_MUNDIAL BPI_1122"
campaign2 = "MX_Turin_Madres_Instream Reach_May_0522"


# campaign1 = "FR_Snickers_IC VNP July P7_0722"
# campaign2 = "FR_M&Ms_VNP P2_0122"


# campaign1 = "DE_Snickers_VNP P10_0922"
# campaign2 = "DE_Airwaves_VNP P8_0722"

# campaign1 = "KE_Juicy Fruit_P13_1222"
# campaign2 = "KE_Juicy Fruit_P12_1122"


# campaign1 = "NL_M&Ms_Project Rain Songs Social P9"
# campaign2 = "NL_M&Ms_Tablets P7_0722"


# campaign1 = "PL_Snickers_Creamy Aftercare_0422"
# campaign2 = "PL_Orbit Gum_Refreshers P11_1022"

# campaign1 = "SA_Twix_Camping P11_1022"
# campaign2 = "SA_Extra_GYDB3_P10_0922"

# campaign1 = "AE_Dove / Galaxy_EBA2.0_P10_0922"
# campaign2 = "ae_kbs_p3-core_mw_mea_cho_brand_olv_awareness_bid-dda_0223_na"


#APAC
# campaign1 = "AU_Maltesers_AOR P6_0522"
# campaign2 = "AU_Eclipse_Eclipse Signals P10-P13 2022 1172_0922"


# campaign1 = "IN_Snickers Social Campaign P13  #2068_1223"
# campaign2 = "IN_Snickers_Snickers Social Campaign_Onam_0922_#1055"




sample_usd(campaign1, campaign2).display()

# COMMAND ----------

sample_local(campaign1, campaign2).display()

# COMMAND ----------


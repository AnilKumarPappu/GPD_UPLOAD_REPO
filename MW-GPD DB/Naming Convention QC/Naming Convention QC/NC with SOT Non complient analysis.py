# Databricks notebook source
pip install openpyxl

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
import openpyxl

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

# MAGIC %md
# MAGIC #### Loading tables

# COMMAND ----------

fact_performance = jdbc_connection('mm.vw_mw_gpd_fact_performance')
dim_campaign = jdbc_connection('mm.vw_mw_gpd_dim_campaign')
dim_creative = jdbc_connection('mm.vw_mw_gpd_dim_creative')
dim_mediabuy = jdbc_connection('mm.vw_mw_gpd_dim_mediabuy')
dim_country = jdbc_connection('mm.vw_mw_gpd_dim_country')
dim_channel = jdbc_connection('mm.vw_mw_gpd_dim_channel')
dim_product = jdbc_connection('mm.vw_mw_gpd_dim_product')

# COMMAND ----------

SOT_file_path = "/Workspace/Shared/MW - GPD/Naming Convention QC/Source of truth (Naming-Convenison) 11-09-2024 updated.xlsx"
campaign_codes = pd.read_excel(SOT_file_path, sheet_name='CampaignAcceptedValues', dtype=str)
creative_codes = pd.read_excel(SOT_file_path, sheet_name='CreativeAcceptedValues', dtype=str)
mediabuy_codes = pd.read_excel(SOT_file_path, sheet_name='MediabuyAcceptedValues', dtype=str)

# COMMAND ----------

# creative_codes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering 2023 and later Data 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### for US ans CA the marketregion code is written as NORTHAM 
# MAGIC ##### all the numbers we get in summary are unique at the corrseponding level like, the campaigns desc numbers are unique at region level and similarly, they are unique at region X country level.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC with the below code when adding brand, we are actually removing the kind products also, by considering the inner join

# COMMAND ----------

fact_performance1 = fact_performance.join(dim_country, on="country_id").withColumn("marketregion_code",
                   when(col('marketregion_code') != '', col('marketregion_code')).otherwise('NORTHAM')).join(dim_product.select('product_id','brand'), on = 'product_id', how = 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'inner')

                   
# fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2023-01-01').filter(col('campaign_start_date') < '2024-01-01')   # only comapigns started in 2023
# fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2024-01-01')   # only comapigns started in 2024
fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2023-01-01')   # 2023 and 2024 campaigns

dim_campaign23 = dim_campaign.join(fact_performance1.select('gpd_campaign_id','marketregion_code','campaign_start_date','country_desc','brand','platform_desc').distinct(), on='gpd_campaign_id')
dim_creative23 = dim_creative.join(fact_performance1.select('creative_id','marketregion_code','campaign_start_date','country_desc','brand','platform_desc').distinct(), on='creative_id')
dim_mediabuy23 = dim_mediabuy.join(fact_performance1.select('media_buy_id','marketregion_code','campaign_start_date','country_desc','brand','platform_desc').distinct(), on='media_buy_id')

# COMMAND ----------

# MAGIC %md
# MAGIC added an "year" column from the "campaign_start_date" column

# COMMAND ----------

dim_campaign23 = dim_campaign23.withColumn("year", substring(col("campaign_start_date"), 0, 4))
dim_creative23 = dim_creative23.withColumn("year", substring(col("campaign_start_date"), 0, 4))
dim_mediabuy23 = dim_mediabuy23.withColumn("year", substring(col("campaign_start_date"), 0, 4))

# COMMAND ----------

# dim_campaign23.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting the dictionaries with actual values

# COMMAND ----------

# MAGIC %md
# MAGIC #### Campaign dictionary

# COMMAND ----------

def get_campaign_codes(column_name):
    temp = campaign_codes[column_name].dropna()
    temp = temp.to_list()
    temp = [str(item).strip() for item in temp]
    # removed the preprocessing of codes as it is not needed
    # temp = [''.join(c for c in s if c.isalnum()).lower() for s in temp]
    return list(set(temp))
    

# COMMAND ----------

# List of all accepted Campaign codes for each columns

campaign_type_list = get_campaign_codes('campaign_type_list')
campaign_market_list = get_campaign_codes('campaign_market_list')
campaign_subproduct_list = get_campaign_codes('campaign_subproduct_list')
segment_list = get_campaign_codes('segment_list')
region_list = get_campaign_codes('region_list')
portfolio_list = get_campaign_codes('portfolio_list')
business_channel_list = get_campaign_codes('business_channel_list')
media_channel_list = get_campaign_codes('media_channel_list')
media_objective_list = get_campaign_codes('media_objective_list')
start_list = get_campaign_codes('starting_month_list')

# COMMAND ----------

# dictionary
keys = ['campaign_type', 'campaign_market', 'campaign_subproduct', 'segment', 'region', 'portfolio', 'business_channel', 'media_channel', 'media_objective', 'starting_month']
values = [campaign_type_list, campaign_market_list, campaign_subproduct_list, segment_list, region_list, portfolio_list, business_channel_list, media_channel_list, media_objective_list, start_list]

campaign_dict = {k: v for k, v in zip(keys, values)}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creative dictionary

# COMMAND ----------

def get_creative_codes(column_name):
    temp = creative_codes[column_name].dropna()
    temp = temp.to_list()
    temp = [str(item).strip() for item in temp]
    # removed the preprocessing of codes as it is not needed
    # temp = [''.join(c for c in s if c.isalnum()).lower() for s in temp]
    return list(set(temp))
    

# COMMAND ----------

# List of all accepted Creative codes for each columns

creative_variant_list = get_creative_codes('creative_variant_list')
creative_type_list = get_creative_codes('creative_type_list')
ad_tag_size_list = get_creative_codes('ad_tag_size_list')
dimension_list = get_creative_codes('dimension_list')
cta_list = get_creative_codes('cta_list')
landing_page_list = get_creative_codes('landing_page_list')
creative_market_list = get_creative_codes('creative_market_list')
creative_subproduct_list = get_creative_codes('creative_subproduct_list')
creative_language_list = get_creative_codes('creative_language_list')
creative_platform_list = get_creative_codes('creative_platform_list')
creative_partner_list = get_creative_codes('creative_partner_list')
creative_campaign_type_list = get_creative_codes('creative_campaign_type_list')
creative_audience_type_list = get_creative_codes('creative_audience_type_list')
creative_audience_desc_list = get_creative_codes('creative_audience_desc_list')


# COMMAND ----------

# dictionary
keys = ['creative_variant','creative_type','ad_tag_size','dimension','cta','landing_page','creative_market','creative_subproduct','creative_language','creative_platform','creative_partner','creative_campaign_type','creative_audience_type','creative_audience_desc']
values = [creative_variant_list ,creative_type_list ,ad_tag_size_list ,dimension_list ,cta_list ,landing_page_list ,creative_market_list ,creative_subproduct_list ,creative_language_list ,creative_platform_list ,creative_partner_list ,creative_campaign_type_list ,creative_audience_type_list ,creative_audience_desc_list ]

creative_dict = {k: v for k, v in zip(keys, values)}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mediabuy dictionary

# COMMAND ----------



def get_mediabuy_codes(column_name):
    temp = mediabuy_codes[column_name].dropna()
    temp = temp.to_list()
    temp = [str(item).strip() for item in temp]
    # removed the preprocessing of codes as it is not needed
    # temp = [''.join(c for c in s if c.isalnum()).lower() for s in temp]
    return list(set(temp))
    

# COMMAND ----------

# List of all accepted Mediabuy codes for each columns

mediabuy_dimensions_list = get_mediabuy_codes('mediabuy_dimensions_list')
mediabuy_ad_tag_size_list = get_mediabuy_codes('mediabuy_ad_tag_size_list')
device_list = get_mediabuy_codes('device_list')
mediabuy_format_list = get_mediabuy_codes('mediabuy_format_list')
placement_type_list = get_mediabuy_codes('placement_type_list')
optimisation_list = get_mediabuy_codes('optimisation_list')
data_type_list = get_mediabuy_codes('data_type_list')
costing_model_list = get_mediabuy_codes('costing_model_list')
buying_type_list = get_mediabuy_codes('buying_type_list')
language_list = get_mediabuy_codes('language_list')

mediabuy_campaign_type_list = get_mediabuy_codes('mediabuy_campaign_type_list')
audience_type_list = get_mediabuy_codes('audience_type_list')
audience_desc_list = get_mediabuy_codes('audience_desc_list')
mediabuy_market_list = get_mediabuy_codes('mediabuy_market_list')

mediabuy_subproduct_list = get_mediabuy_codes('mediabuy_subproduct_list')
strategy_list = get_mediabuy_codes('strategy_list')
mediabuy_platform_list = get_mediabuy_codes('mediabuy_platform_list')
mediabuy_partner_list = get_mediabuy_codes('mediabuy_partner_list')
mediabuy_objective_list = get_mediabuy_codes('mediabuy_objective_list')



# COMMAND ----------

# dictionary
keys = ['language','buying_type','costing_model','mediabuy_campaign_type','audience_type','audience_desc','data_type','optimisation','placement_type','mediabuy_format','device','mediabuy_ad_tag_size','mediabuy_market','mediabuy_subproduct','strategy','mediabuy_platform','mediabuy_partner','mediabuy_objective','mediabuy_dimensions']

values = [language_list,buying_type_list,costing_model_list ,mediabuy_campaign_type_list ,audience_type_list ,audience_desc_list ,data_type_list ,optimisation_list ,placement_type_list ,mediabuy_format_list ,device_list ,mediabuy_ad_tag_size_list ,mediabuy_market_list ,mediabuy_subproduct_list ,strategy_list ,mediabuy_platform_list ,mediabuy_partner_list ,mediabuy_objective_list ,mediabuy_dimensions_list ]

mediaBuy_dict = {k: v for k, v in zip(keys, values)}



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. with latest codes

# COMMAND ----------

def low(x):
    return (x).lower()

low_udf = udf(low, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC campaign

# COMMAND ----------

##### converting all the column values to lower case

dim_campaign23_lower = dim_campaign23.withColumn('campaign_type',low_udf(col("campaign_type")))\
    .withColumn('campaign_market',low_udf(col("campaign_market"))) \
    .withColumn('campaign_subproduct',low_udf(col("campaign_subproduct"))) \
    .withColumn('segment',low_udf(col("segment"))) \
    .withColumn('region',low_udf(col("region"))) \
    .withColumn('portfolio',low_udf(col("portfolio"))) \
    .withColumn('business_channel',low_udf(col("business_channel"))) \
    .withColumn('media_channel',low_udf(col("media_channel"))) \
    .withColumn('media_objective',low_udf(col("media_objective"))) \
    .withColumn('starting_month',low_udf(col("starting_month")))

# COMMAND ----------

##### to get naming conv issues
dim_campaign23_lower = dim_campaign23_lower.withColumn(
    "calculated_naming_flag", when(
        ((col("campaign_type")).isin(campaign_dict['campaign_type'])) & \
        ((col('campaign_market')).isin(campaign_dict['campaign_market'])) & 
        ((col("campaign_subproduct")).isin(campaign_dict['campaign_subproduct'])) & 
        ((col("segment")).isin(campaign_dict['segment'])) & 
        ((col("region")).isin(campaign_dict['region'])) & 
        ((col("portfolio")).isin(campaign_dict['portfolio'])) & 
        ((col("business_channel")).isin(campaign_dict['business_channel'])) & 
        ((col("media_channel")).isin(campaign_dict['media_channel'])) & 
        ((col("media_objective")).isin(campaign_dict['media_objective'])) & 
        ((col("starting_month")).isin(campaign_dict['starting_month'])), 0).otherwise(1))

# COMMAND ----------

# ###### to get access layer issues
# dim_campaign23 = dim_campaign23.withColumn(
#     "calculated_naming_flag", when(
#         ((col("campaign_type")).isin(campaign_dict['campaign_type'])) & \
#         ((col('campaign_market')).isin(campaign_dict['campaign_market'])) & 
#         ((col("campaign_subproduct")).isin(campaign_dict['campaign_subproduct'])) & 
#         ((col("segment")).isin(campaign_dict['segment'])) & 
#         ((col("region")).isin(campaign_dict['region'])) & 
#         ((col("portfolio")).isin(campaign_dict['portfolio'])) & 
#         ((col("business_channel")).isin(campaign_dict['business_channel'])) & 
#         ((col("media_channel")).isin(campaign_dict['media_channel'])) & 
#         ((col("media_objective")).isin(campaign_dict['media_objective'])) & 
#         ((col("starting_month")).isin(campaign_dict['starting_month'])), 0).otherwise(1))

# COMMAND ----------

# checking with calculated_naming_flag
dim_campaign23_lower.select('marketregion_code','country_id','country_desc','year','brand','platform_desc','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_id','country_desc','year','brand','platform_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC media buy

# COMMAND ----------

########### converting all the column values to lower case


dim_mediabuy23_lower = dim_mediabuy23.withColumn('mediabuy_market',low_udf(col("mediabuy_market")))\
.withColumn('mediabuy_subproduct',low_udf(col("mediabuy_subproduct")))\
.withColumn('language',low_udf(col("language")))\
.withColumn('strategy',low_udf(col("strategy")))\
.withColumn('mediabuy_platform',low_udf(col("mediabuy_platform")))\
.withColumn('mediabuy_partner',low_udf(col("mediabuy_partner")))\
.withColumn('buying_type',low_udf(col("buying_type")))\
.withColumn('costing_model',low_udf(col("costing_model")))\
.withColumn('mediabuy_campaign_type',low_udf(col("mediabuy_campaign_type")))\
.withColumn('audience_type',low_udf(col("audience_type")))\
.withColumn('audience_desc',low_udf(col("audience_desc")))\
.withColumn('data_type',low_udf(col("data_type")))\
.withColumn('mediabuy_objective',low_udf(col("mediabuy_objective")))\
.withColumn('optimisation',low_udf(col("optimisation")))\
.withColumn('placement_type',low_udf(col("placement_type")))\
.withColumn('mediabuy_format',low_udf(col("mediabuy_format")))\
.withColumn('device',low_udf(col("device")))\
.withColumn('mediabuy_ad_tag_size',low_udf(col("mediabuy_ad_tag_size")))\
.withColumn('mediabuy_dimensions',low_udf(col("mediabuy_dimensions")))


# COMMAND ----------

##### to get naming conv issues
dim_mediabuy23_lower = dim_mediabuy23_lower.withColumn(
    "calculated_naming_flag", when(
        ((col("language")).isin(mediaBuy_dict['language'])) \
        & ((col('buying_type')).isin(mediaBuy_dict['buying_type'])) \
        & ((col("costing_model")).isin(mediaBuy_dict['costing_model'])) \
        & ((col("mediabuy_campaign_type")).isin(mediaBuy_dict['mediabuy_campaign_type'])) \
        & ((col("audience_type")).isin(mediaBuy_dict['audience_type'])) \
        & ((col("audience_desc")).isin(mediaBuy_dict['audience_desc'])) \
        & ((col("data_type")).isin(mediaBuy_dict['data_type'])) \
        & ((col("optimisation")).isin(mediaBuy_dict['optimisation'])) \
        & ((col("placement_type")).isin(mediaBuy_dict['placement_type'])) \
        & ((col("mediabuy_format")).isin(mediaBuy_dict['mediabuy_format'])) \
        & ((col("device")).isin(mediaBuy_dict['device'])) \
        & ((col("mediabuy_ad_tag_size")).isin(mediaBuy_dict['mediabuy_ad_tag_size'])) \
        & ((col("mediabuy_market")).isin(mediaBuy_dict['mediabuy_market'])) \
        & ((col("mediabuy_subproduct")).isin(mediaBuy_dict['mediabuy_subproduct'])) \
        & ((col("strategy")).isin(mediaBuy_dict['strategy'])) \
        & ((col("mediabuy_platform")).isin(mediaBuy_dict['mediabuy_platform'])) \
        & ((col("mediabuy_partner")).isin(mediaBuy_dict['mediabuy_partner'])) \
        & ((col("mediabuy_objective")).isin(mediaBuy_dict['mediabuy_objective'])) \
        & ((col("mediabuy_dimensions")).isin(mediaBuy_dict['mediabuy_dimensions'])), 0).otherwise(1)
)

# COMMAND ----------

# ###### to get access layer issues
# dim_mediabuy23 = dim_mediabuy23.withColumn(
#     "calculated_naming_flag", when(
#         ((col("language")).isin(mediaBuy_dict['language'])) \
#         & ((col('buying_type')).isin(mediaBuy_dict['buying_type'])) \
#         & ((col("costing_model")).isin(mediaBuy_dict['costing_model'])) \
#         & ((col("mediabuy_campaign_type")).isin(mediaBuy_dict['mediabuy_campaign_type'])) \
#         & ((col("audience_type")).isin(mediaBuy_dict['audience_type'])) \
#         & ((col("audience_desc")).isin(mediaBuy_dict['audience_desc'])) \
#         & ((col("data_type")).isin(mediaBuy_dict['data_type'])) \
#         & ((col("optimisation")).isin(mediaBuy_dict['optimisation'])) \
#         & ((col("placement_type")).isin(mediaBuy_dict['placement_type'])) \
#         & ((col("mediabuy_format")).isin(mediaBuy_dict['mediabuy_format'])) \
#         & ((col("device")).isin(mediaBuy_dict['device'])) \
#         & ((col("mediabuy_ad_tag_size")).isin(mediaBuy_dict['mediabuy_ad_tag_size'])) \
#         & ((col("mediabuy_market")).isin(mediaBuy_dict['mediabuy_market'])) \
#         & ((col("mediabuy_subproduct")).isin(mediaBuy_dict['mediabuy_subproduct'])) \
#         & ((col("strategy")).isin(mediaBuy_dict['strategy'])) \
#         & ((col("mediabuy_platform")).isin(mediaBuy_dict['mediabuy_platform'])) \
#         & ((col("mediabuy_partner")).isin(mediaBuy_dict['mediabuy_partner'])) \
#         & ((col("mediabuy_objective")).isin(mediaBuy_dict['mediabuy_objective'])) \
#         & ((col("mediabuy_dimensions")).isin(mediaBuy_dict['mediabuy_dimensions'])), 0).otherwise(1)
# )

# COMMAND ----------

dim_mediabuy23_lower.select('marketregion_code','country_id','country_desc','year','brand','platform_desc','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_id','country_desc','year','brand','platform_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC creative

# COMMAND ----------

########## converting all the column values to lower case

dim_creative23_lower = dim_creative23.withColumn('creative_market',low_udf(col("creative_market")))\
    .withColumn('creative_subproduct',low_udf(col("creative_subproduct")))\
    .withColumn('creative_language',low_udf(col("creative_language")))\
    .withColumn('creative_platform',low_udf(col("creative_platform")))\
    .withColumn('creative_partner',low_udf(col("creative_partner")))\
    .withColumn('creative_campaign_type',low_udf(col("creative_campaign_type")))\
    .withColumn('creative_audience_type',low_udf(col("creative_audience_type")))\
    .withColumn('creative_audience_desc',low_udf(col("creative_audience_desc")))\
    .withColumn('creative_variant',low_udf(col("creative_variant")))\
    .withColumn('creative_type',low_udf(col("creative_type")))\
    .withColumn('ad_tag_size',low_udf(col("ad_tag_size")))\
    .withColumn('dimension',low_udf(col("dimension")))\
    .withColumn('cta',low_udf(col("cta")))\
    .withColumn('landing_page',low_udf(col("landing_page")))


# COMMAND ----------

####### to get naming conv issues
dim_creative23_lower = dim_creative23_lower.withColumn(
    "calculated_naming_flag", when(
    ((col("creative_variant")).isin(creative_dict['creative_variant'])) & \
    ((col('creative_type')).isin(creative_dict['creative_type'])) & \
    ((col("ad_tag_size")).isin(creative_dict['ad_tag_size'])) & \
    ((col("dimension")).isin(creative_dict['dimension'])) & \
    ((col("cta")).isin(creative_dict['cta'])) & \
    ((col("landing_page")).isin(creative_dict['landing_page'])) & \
    ((col("creative_market")).isin(creative_dict['creative_market'])) & \
    ((col("creative_subproduct")).isin(creative_dict['creative_subproduct'])) & \
    ((col("creative_language")).isin(creative_dict['creative_language'])) & \
    ((col("creative_platform")).isin(creative_dict['creative_platform'])) & \
    ((col("creative_partner")).isin(creative_dict['creative_partner'])) & \
    ((col("creative_campaign_type")).isin(creative_dict['creative_campaign_type'])) & \
    ((col("creative_audience_type")).isin(creative_dict['creative_audience_type'])) & \
    ((col("creative_audience_desc")).isin(creative_dict['creative_audience_desc'])), 0).otherwise(1)
)

# COMMAND ----------

# ####### to get access layer issues
# dim_creative23 = dim_creative23.withColumn(
#     "calculated_naming_flag", when(
#     ((col("creative_variant")).isin(creative_dict['creative_variant'])) & \
#     ((col('creative_type')).isin(creative_dict['creative_type'])) & \
#     ((col("ad_tag_size")).isin(creative_dict['ad_tag_size'])) & \
#     ((col("dimension")).isin(creative_dict['dimension'])) & \
#     ((col("cta")).isin(creative_dict['cta'])) & \
#     ((col("landing_page")).isin(creative_dict['landing_page'])) & \
#     ((col("creative_market")).isin(creative_dict['creative_market'])) & \
#     ((col("creative_subproduct")).isin(creative_dict['creative_subproduct'])) & \
#     ((col("creative_language")).isin(creative_dict['creative_language'])) & \
#     ((col("creative_platform")).isin(creative_dict['creative_platform'])) & \
#     ((col("creative_partner")).isin(creative_dict['creative_partner'])) & \
#     ((col("creative_campaign_type")).isin(creative_dict['creative_campaign_type'])) & \
#     ((col("creative_audience_type")).isin(creative_dict['creative_audience_type'])) & \
#     ((col("creative_audience_desc")).isin(creative_dict['creative_audience_desc'])), 0).otherwise(1)
# )

# COMMAND ----------


dim_creative23_lower.select('marketregion_code','country_id','country_desc','year','brand','platform_desc','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_id','country_desc','year','brand','platform_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering only non complient values for the analysis

# COMMAND ----------

# dim_campaign23_Complient = dim_campaign23.filter(col('calculated_naming_flag')==0)
# dim_campaign23_Complient.select('campaign_desc','campaign_market','campaign_subproduct','segment', 'region', 'portfolio', 'business_channel', 'media_channel', 'media_objective','campaign_type', 'starting_month','calculated_naming_flag').display()

# COMMAND ----------

dim_campaign23_NonComplient = dim_campaign23_lower.filter(col('calculated_naming_flag')==1)

# COMMAND ----------

# dim_campaign23_lower_NonComplient = dim_campaign23_lower.filter(col('calculated_naming_flag')==1)

# COMMAND ----------

dim_campaign23_NonComplient = dim_campaign23_NonComplient.withColumn("campaign_type", when(~(col("campaign_type")).isin(campaign_dict['campaign_type']), col("campaign_type")).otherwise(lit(None)))\
    .withColumn("campaign_market", when(~(col("campaign_market")).isin(campaign_dict['campaign_market']), col("campaign_market")).otherwise(lit(None)))\
    .withColumn("campaign_subproduct", when(~(col("campaign_subproduct")).isin(campaign_dict['campaign_subproduct']), col("campaign_subproduct")).otherwise(lit(None)))\
    .withColumn("segment", when(~(col("segment")).isin(campaign_dict['segment']), col("segment")).otherwise(lit(None)))\
    .withColumn("region", when(~(col("region")).isin(campaign_dict['region']), col("region")).otherwise(lit(None)))\
    .withColumn("portfolio", when(~(col("portfolio")).isin(campaign_dict['portfolio']), col("portfolio")).otherwise(lit(None)))\
    .withColumn("business_channel", when(~(col("business_channel")).isin(campaign_dict['business_channel']), col("business_channel")).otherwise(lit(None)))\
    .withColumn("media_channel", when(~(col("media_channel")).isin(campaign_dict['media_channel']), col("media_channel")).otherwise(lit(None)))\
    .withColumn("media_objective", when(~(col("media_objective")).isin(campaign_dict['media_objective']), col("media_objective")).otherwise(lit(None)))\
    .withColumn("starting_month", when(~(col("starting_month")).isin(campaign_dict['starting_month']), col("starting_month")).otherwise(lit(None)))

# COMMAND ----------

dim_campaign23_NonComplient.select('marketregion_code','country_id','year','brand','platform_desc','campaign_desc','campaign_market','campaign_subproduct','segment', 'region', 'portfolio', 'business_channel', 'media_channel', 'media_objective','campaign_type', 'starting_month','calculated_naming_flag').dropDuplicates().display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Mediabuy

# COMMAND ----------

dim_mediabuy23_NonComplient = dim_mediabuy23_lower.filter(col('calculated_naming_flag')==1)

# COMMAND ----------

dim_mediabuy23_NonComplient = dim_mediabuy23_NonComplient.withColumn("language", when(~(col("language")).isin(mediaBuy_dict['language']), col("language")).otherwise(lit(None)))\
    .withColumn("buying_type", when(~(col("buying_type")).isin(mediaBuy_dict['buying_type']), col("buying_type")).otherwise(lit(None)))\
    .withColumn("costing_model", when(~(col("costing_model")).isin(mediaBuy_dict['costing_model']), col("costing_model")).otherwise(lit(None)))\
    .withColumn("mediabuy_campaign_type", when(~(col("mediabuy_campaign_type")).isin(mediaBuy_dict['mediabuy_campaign_type']), col("mediabuy_campaign_type")).otherwise(lit(None)))\
    .withColumn("audience_type", when(~(col("audience_type")).isin(mediaBuy_dict['audience_type']), col("audience_type")).otherwise(lit(None)))\
    .withColumn("audience_desc", when(~(col("audience_desc")).isin(mediaBuy_dict['audience_desc']), col("audience_desc")).otherwise(lit(None)))\
    .withColumn("data_type", when(~(col("data_type")).isin(mediaBuy_dict['data_type']), col("data_type")).otherwise(lit(None)))\
    .withColumn("optimisation", when(~(col("optimisation")).isin(mediaBuy_dict['optimisation']), col("optimisation")).otherwise(lit(None)))\
    .withColumn("placement_type", when(~(col("placement_type")).isin(mediaBuy_dict['placement_type']), col("placement_type")).otherwise(lit(None)))\
    .withColumn("mediabuy_format", when(~(col("mediabuy_format")).isin(mediaBuy_dict['mediabuy_format']), col("mediabuy_format")).otherwise(lit(None)))\
    .withColumn("device", when(~(col("device")).isin(mediaBuy_dict['device']), col("device")).otherwise(lit(None)))\
    .withColumn("mediabuy_ad_tag_size", when(~(col("mediabuy_ad_tag_size")).isin(mediaBuy_dict['mediabuy_ad_tag_size']), col("mediabuy_ad_tag_size")).otherwise(lit(None)))\
    .withColumn("mediabuy_market", when(~(col("mediabuy_market")).isin(mediaBuy_dict['mediabuy_market']), col("mediabuy_market")).otherwise(lit(None)))\
    .withColumn("mediabuy_subproduct", when(~(col("mediabuy_subproduct")).isin(mediaBuy_dict['mediabuy_subproduct']), col("mediabuy_subproduct")).otherwise(lit(None)))\
    .withColumn("strategy", when(~(col("strategy")).isin(mediaBuy_dict['strategy']), col("strategy")).otherwise(lit(None)))\
    .withColumn("mediabuy_platform", when(~(col("mediabuy_platform")).isin(mediaBuy_dict['mediabuy_platform']), col("mediabuy_platform")).otherwise(lit(None)))\
    .withColumn("mediabuy_partner", when(~(col("mediabuy_partner")).isin(mediaBuy_dict['mediabuy_partner']), col("mediabuy_partner")).otherwise(lit(None)))\
    .withColumn("mediabuy_objective", when(~(col("mediabuy_objective")).isin(mediaBuy_dict['mediabuy_objective']), col("mediabuy_objective")).otherwise(lit(None)))\
    .withColumn("mediabuy_dimensions", when(~(col("mediabuy_dimensions")).isin(mediaBuy_dict['mediabuy_dimensions']), col("mediabuy_dimensions")).otherwise(lit(None)))


# COMMAND ----------

# dim_mediabuy23.filter(col('calculated_naming_flag')==0).select('media_buy_desc','mediabuy_market','mediabuy_subproduct','language','strategy','mediabuy_platform','mediabuy_partner','buying_type','costing_model','mediabuy_campaign_type','audience_type','audience_desc','data_type','mediabuy_objective','optimisation','placement_type','mediabuy_format','device','mediabuy_ad_tag_size','mediabuy_dimensions','calculated_naming_flag').display()

# COMMAND ----------

dim_mediabuy23_NonComplient.select('marketregion_code','country_id','year','brand','platform_desc','media_buy_desc','mediabuy_market','mediabuy_subproduct','language','strategy','mediabuy_platform','mediabuy_partner','buying_type','costing_model','mediabuy_campaign_type','audience_type','audience_desc','data_type','mediabuy_objective','optimisation','placement_type','mediabuy_format','device','mediabuy_ad_tag_size','mediabuy_dimensions','calculated_naming_flag').dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC creative

# COMMAND ----------

dim_creative23_lower.count()

# COMMAND ----------

dim_creative23_NonComplient = dim_creative23_lower.filter(col('calculated_naming_flag')==1)

# COMMAND ----------

dim_creative23_NonComplient = dim_creative23_NonComplient.withColumn("creative_variant", when(~(col("creative_variant")).isin(creative_dict['creative_variant']), col("creative_variant")).otherwise(lit(None))) \
            .withColumn("creative_type", when(~(col("creative_type")).isin(creative_dict['creative_type']), col("creative_type")).otherwise(lit(None))) \
            .withColumn("ad_tag_size", when(~(col("ad_tag_size")).isin(creative_dict['ad_tag_size']), col("ad_tag_size")).otherwise(lit(None))) \
            .withColumn("dimension", when(~(col("dimension")).isin(creative_dict['dimension']), col("dimension")).otherwise(lit(None))) \
            .withColumn("cta", when(~(col("cta")).isin(creative_dict['cta']), col("cta")).otherwise(lit(None))) \
            .withColumn("landing_page", when(~(col("landing_page")).isin(creative_dict['landing_page']), col("landing_page")).otherwise(lit(None))) \
            .withColumn("creative_market", when(~(col("creative_market")).isin(creative_dict['creative_market']), col("creative_market")).otherwise(lit(None)))\
            .withColumn("creative_subproduct", when(~(col("creative_subproduct")).isin(creative_dict['creative_subproduct']), col("creative_subproduct")).otherwise(lit(None))) \
            .withColumn("creative_language", when(~(col("creative_language")).isin(creative_dict['creative_language']), col("creative_language")).otherwise(lit(None))) \
            .withColumn("creative_platform", when(~(col("creative_platform")).isin(creative_dict['creative_platform']), col("creative_platform")).otherwise(lit(None))) \
            .withColumn("creative_partner", when(~(col("creative_partner")).isin(creative_dict['creative_partner']), col("creative_partner")).otherwise(lit(None))) \
            .withColumn("creative_campaign_type", when(~(col("creative_campaign_type")).isin(creative_dict['creative_campaign_type']), col("creative_campaign_type")).otherwise(lit(None))) \
            .withColumn("creative_audience_type", when(~(col("creative_audience_type")).isin(creative_dict['creative_audience_type']), col("creative_audience_type")).otherwise(lit(None))) \
            .withColumn("creative_audience_desc", when(~(col("creative_audience_desc")).isin(creative_dict['creative_audience_desc']), col("creative_audience_desc")).otherwise(lit(None)))


# COMMAND ----------

# dim_creative23.filter(col('calculated_naming_flag')==0).select('creative_desc','creative_market','creative_subproduct','creative_language','creative_platform','creative_partner','creative_campaign_type','creative_audience_type','creative_audience_desc','creative_variant','creative_type','ad_tag_size','dimension','cta','landing_page','calculated_naming_flag').display()

# COMMAND ----------

dim_creative23_NonComplient.select('marketregion_code','country_id','year','brand','platform_desc','creative_desc','creative_market','creative_subproduct','creative_language','creative_platform','creative_partner','creative_campaign_type','creative_audience_type','creative_audience_desc','creative_variant','creative_type','ad_tag_size','dimension','cta','landing_page','calculated_naming_flag').dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### After filtering the uneditable campaigns

# COMMAND ----------

exceptions_file_path = "/Workspace/Shared/MW - GPD/Naming Convention QC/Exception list 07-10-2024.xlsx"
octra_campaign = pd.read_excel(exceptions_file_path, sheet_name='Octra - campaign', dtype=str)
octra_creative = pd.read_excel(exceptions_file_path, sheet_name='Octra - Creative', dtype=str)
octra_mediabuy = pd.read_excel(exceptions_file_path, sheet_name='Octra - mediabuy', dtype=str)

NA_campaign = pd.read_excel(exceptions_file_path, sheet_name='NA - campaign', dtype=str)
NA_creative = pd.read_excel(exceptions_file_path, sheet_name='NA - creative', dtype=str)
NA_mediabuy = pd.read_excel(exceptions_file_path, sheet_name='NA - mediabuy', dtype=str)

# COMMAND ----------

def get_unique_list(temp):
    temp = [str(item).strip() for item in temp]
    return list(set(temp))

# COMMAND ----------

# MAGIC %md
# MAGIC note: considering here only which can be filtered for the summary

# COMMAND ----------

# MAGIC %md
# MAGIC campaigns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Campaigns filtered for which naming convention cannot be changed at source

# COMMAND ----------

oc = octra_campaign['campaign'].dropna().to_list()
nc = NA_campaign['campaign'].dropna().to_list()


# COMMAND ----------

NorthAmericaOldCampaignList = get_unique_list(nc)
RestOldCampaignList = get_unique_list(oc)

# COMMAND ----------

campaignsToBeFiltered = NorthAmericaOldCampaignList + RestOldCampaignList
# considering here only which can be filtered for the summary
dim_campaign23_lower_campaigns_filtered = dim_campaign23_lower.filter(col('campaign_desc').isin(campaignsToBeFiltered))
# considering values after the exceptions are filterd outfor the summary
# dim_campaign23_lower_campaigns_filtered = dim_campaign23_lower.filter(~col('campaign_desc').isin(campaignsToBeFiltered))



# COMMAND ----------

# checking with calculated_naming_flag
dim_campaign23_lower_campaigns_filtered.select('marketregion_code','country_id','country_desc','year','brand','platform_desc','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_id','country_desc','year','brand','platform_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC media buy

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mediabuy filtered for which naming convention cannot be changed at source

# COMMAND ----------

om = octra_mediabuy['mediabuy'].dropna().to_list()
nm = NA_mediabuy['mediabuy'].dropna().to_list()


# COMMAND ----------

NorthAmericaOldMediaList = get_unique_list(nm)
RestOldMediaList = get_unique_list(om)

# COMMAND ----------

MediabuyToBeFiltered = NorthAmericaOldMediaList + RestOldMediaList
# considering here only which can be filtered for the summary
dim_mediabuy23_lower_mediabuy_filtered = dim_mediabuy23_lower.filter(col('media_buy_desc').isin(MediabuyToBeFiltered))

# considering values after the exceptions are filterd outfor the summary
# dim_mediabuy23_lower_mediabuy_filtered = dim_mediabuy23_lower.filter(~col('media_buy_desc').isin(MediabuyToBeFiltered))


# COMMAND ----------

dim_mediabuy23_lower_mediabuy_filtered.select('marketregion_code','country_id','country_desc','year','brand','platform_desc','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_id','country_desc','year','brand','platform_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Creatives

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creative filtered for which naming convention cannot be changed at source

# COMMAND ----------

ocr = octra_creative['creative'].dropna().to_list()
ncr = NA_creative['creative'].dropna().to_list()


# COMMAND ----------

NorthAmericaOldCreativeList = get_unique_list(ncr)
RestOldCreativeList = get_unique_list(ocr)

# COMMAND ----------

creativeToBeFiltered = NorthAmericaOldCreativeList + RestOldCreativeList
# considering here only which can be filtered for the summary
dim_creative23_lower_creative_filtered = dim_creative23_lower.filter(col('creative_desc').isin(creativeToBeFiltered))

# considering values after the exceptions are filterd out for the summary
# dim_creative23_lower_creative_filtered = dim_creative23_lower.filter(~col('creative_desc').isin(creativeToBeFiltered))


# COMMAND ----------


dim_creative23_lower_creative_filtered.select('marketregion_code','country_id','country_desc','year','brand','platform_desc','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_id','country_desc','year','brand','platform_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()
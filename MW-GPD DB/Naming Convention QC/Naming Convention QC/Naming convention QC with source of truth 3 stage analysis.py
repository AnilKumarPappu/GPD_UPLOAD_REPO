# Databricks notebook source
# MAGIC %md
# MAGIC #### Importing libraries

# COMMAND ----------

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

# COMMAND ----------

SOT_file_path = "/Workspace/Shared/MW - GPD/Naming Convention QC/Source of truth (Naming-Convenison) 11-09-2024 updated.xlsx"
campaign_codes = pd.read_excel(SOT_file_path, sheet_name='CampaignAcceptedValues', dtype=str)
creative_codes = pd.read_excel(SOT_file_path, sheet_name='CreativeAcceptedValues', dtype=str)
mediabuy_codes = pd.read_excel(SOT_file_path, sheet_name='MediabuyAcceptedValues', dtype=str)

# COMMAND ----------

exceptions_file_path = "/Workspace/Shared/MW - GPD/Naming Convention QC/Exception list 11-09-2024.xlsx"
octra_campaign = pd.read_excel(exceptions_file_path, sheet_name='Octra - campaign', dtype=str)
octra_creative = pd.read_excel(exceptions_file_path, sheet_name='Octra - Creative', dtype=str)
octra_mediabuy = pd.read_excel(exceptions_file_path, sheet_name='Octra - mediabuy', dtype=str)

NA_campaign = pd.read_excel(exceptions_file_path, sheet_name='NA - campaign', dtype=str)
NA_creative = pd.read_excel(exceptions_file_path, sheet_name='NA - creative', dtype=str)
NA_mediabuy = pd.read_excel(exceptions_file_path, sheet_name='NA - mediabuy', dtype=str)

# COMMAND ----------

dim_creative.filter(col('country_id') == 'UK').display()

# COMMAND ----------

# type(campaign_codes)
creative_codes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering 2023 and later Data 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### for US ans CA the marketregion code is written as NORTHAM 
# MAGIC ##### all the numbers we get in summary are unique at the corrseponding level like, the campaigns numbers are unique at region level and similarly, they are unique at region X country level.
# MAGIC

# COMMAND ----------

fact_performance1 = fact_performance.join(dim_country, on="country_id").withColumn("marketregion_code",
                   when(col('marketregion_code') != '', col('marketregion_code')).otherwise('NORTHAM'))

                   
# fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2023-01-01').filter(col('campaign_start_date') < '2024-01-01')   # only comapigns started in 2023
# fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2024-01-01')   # only comapigns started in 2024
fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2023-01-01')   # 2023 and 2024 campaigns

dim_campaign23 = dim_campaign.join(fact_performance1.select('gpd_campaign_id','marketregion_code','campaign_start_date','country_desc').distinct(), on='gpd_campaign_id')
dim_creative23 = dim_creative.join(fact_performance1.select('creative_id','marketregion_code','campaign_start_date','country_desc').distinct(), on='creative_id')
dim_mediabuy23 = dim_mediabuy.join(fact_performance1.select('media_buy_id','marketregion_code','campaign_start_date','country_desc').distinct(), on='media_buy_id')

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

# removed the preprocessing of codes as it is not needed

# def preproc(x):
#     return (''.join(c for c in x if c.isalnum())).lower()

# preproc_udf = udf(preproc, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. current access layer summary

# COMMAND ----------

# MAGIC %md
# MAGIC Region level

# COMMAND ----------

dim_campaign23.groupby('marketregion_code')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

dim_mediabuy23.select('marketregion_code','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# dim_mediabuy23.filter(col('naming_convention_flag')==0).filter(col('mediabuy_partner').isin(['dv36o'])).display()
# dim_mediabuy23.filter(col('naming_convention_flag')==0).filter(col('mediabuy_market').isin(['gb'])).display()

# COMMAND ----------

# dim_mediabuy23.select('mediabuy_partner').filter(col('mediabuy_partner').isin(['dv36o'])).display()
# dim_mediabuy23.select('mediabuy_market').filter(col('mediabuy_market').isin(['gb'])).display()

# COMMAND ----------

# dim_mediabuy23.select('costing_model').distinct().display()

# COMMAND ----------


dim_creative23.select('marketregion_code','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. with latest codes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Region level
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC campaign

# COMMAND ----------

dim_campaign23 = dim_campaign23.withColumn(
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

# checking with calculated_naming_flag
dim_campaign23.select('marketregion_code','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC media buy

# COMMAND ----------

dim_mediabuy23 = dim_mediabuy23.withColumn(
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

dim_mediabuy23.select('marketregion_code','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC creative

# COMMAND ----------

dim_creative23 = dim_creative23.withColumn(
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


dim_creative23.select('marketregion_code','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Region X country level
# MAGIC

# COMMAND ----------

dim_campaign23.select('marketregion_code','country_desc','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_mediabuy23.select('marketregion_code','country_desc','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_creative23.select('marketregion_code','country_desc','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. with latest codes and excluding the correspoding list

# COMMAND ----------

def get_unique_list(temp):
    temp = [str(item).strip() for item in temp]
    return list(set(temp))

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
# campaign_channel = fact_performance.select('gpd_campaign_id','channel_id').dropDuplicates()
dim_campaign23_campaigns_filtered = dim_campaign23.filter(~col('campaign_desc').isin(campaignsToBeFiltered))
# .join(campaign_channel, on = 'gpd_campaign_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left')


# COMMAND ----------

# checking with calculated_naming_flag
dim_campaign23_campaigns_filtered.select('marketregion_code','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC D2C campaigns count

# COMMAND ----------

# only d2c campaigns are considered
dim_campaign23_campaigns_d2c_filtered = dim_campaign23_campaigns_filtered.filter(col("campaign_desc").contains("d2c") | col("campaign_desc").contains("dtc") | col("campaign_desc").contains("dcom"))

# COMMAND ----------

# checking with calculated_naming_flag
dim_campaign23_campaigns_d2c_filtered.select('marketregion_code','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
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
# media_channel = fact_performance.select('media_buy_id','channel_id').dropDuplicates()
dim_mediabuy23_mediabuy_filtered = dim_mediabuy23.filter(~col('media_buy_desc').isin(MediabuyToBeFiltered))
# .join(media_channel, on = 'media_buy_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left')

# COMMAND ----------

dim_mediabuy23_mediabuy_filtered.select('marketregion_code','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Creatives

# COMMAND ----------

# df2 = fact_performance.select('creative_id','channel_id').dropDuplicates()
# just for a check
# dim_creative23.join(df2, on = 'creative_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').filter(col('platform_desc').isNull()).display()

# .select("platform_desc").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### only these filterd platforms are showing in dashboard for content health

# COMMAND ----------

# only these filterd platforms are showing in dashboard for content health
# dim_creative23_platform_filtered = dim_creative23.join(df2, on = 'creative_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').filter(col('platform_Desc').isin(['GOOGLE ADWORDS', 'DV360-YOUTUBE', 'FACEBOOK ADS', 'PINTEREST ADS', 'SNAPCHAT', 'SNAPCHAT ADS', 'TIKTOK ADS', 'REDDIT ADS']))

# dim_creative23_platform_filtered.select('marketregion_code','country_desc','platform_desc','creative_desc','naming_convention_flag').display()

# SNAPCHAT ADS
# DV360-YOUTUBE
# REDDIT ADS
# GOOGLE ADWORDS
# FACEBOOK ADS
# TIKTOK ADS
# PINTEREST ADS

# COMMAND ----------

# dim_creative23_platform_filtered.select('platform_desc').distinct().display()

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
# dim_creative23_creative_platform_filtered = dim_creative23_platform_filtered.filter(~col('creative_desc').isin(creativeToBeFiltered))
dim_creative23_creative_filtered = dim_creative23.filter(~col('creative_desc').isin(creativeToBeFiltered))

# COMMAND ----------


dim_creative23_creative_filtered.select('marketregion_code','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Region X country level

# COMMAND ----------

# checking with calculated_naming_flag
dim_campaign23_campaigns_filtered.select('marketregion_code','country_desc','campaign_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

dim_mediabuy23_mediabuy_filtered.select('marketregion_code','country_desc','media_buy_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------


dim_creative23_creative_filtered.select('marketregion_code','country_desc','creative_desc','calculated_naming_flag').dropDuplicates().groupby('marketregion_code','country_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Cases

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Naming convention flag and calculated flag should be same

# COMMAND ----------

# MAGIC %md
# MAGIC #### Campaign

# COMMAND ----------

dim_campaign23 = dim_campaign23.withColumn(
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
        ((col("starting_month")).isin(campaign_dict['starting_month'])), 0).otherwise(1)
)

# code for preprocessing of codes, not needed now
        # (preproc_udf(col("campaign_type")).isin(campaign_dict['campaign_type'])) & \
        # (preproc_udf(col('campaign_market')).isin(campaign_dict['campaign_market'])) & 
        # (preproc_udf(col("campaign_subproduct")).isin(campaign_dict['campaign_subproduct'])) & 
        # (preproc_udf(col("segment")).isin(campaign_dict['segment'])) & 
        # (preproc_udf(col("region")).isin(campaign_dict['region'])) & 
        # (preproc_udf(col("portfolio")).isin(campaign_dict['portfolio'])) & 
        # (preproc_udf(col("business_channel")).isin(campaign_dict['business_channel'])) & 
        # (preproc_udf(col("media_channel")).isin(campaign_dict['media_channel'])) & 
        # (preproc_udf(col("media_objective")).isin(campaign_dict['media_objective'])) & 
        # (preproc_udf(col("starting_month")).isin(campaign_dict['starting_month'])), 0).otherwise(1)

# COMMAND ----------

campaign_mismatch = dim_campaign23.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
campaign_mismatch.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Campaigns filtered for which naming convention cannot be changed at source

# COMMAND ----------

NorthAmericaOldCampaignList = ["US_Ethel M_Holiday_1122_Mars_Q4_11/9-12/25_Facebook/Instagram_Conversion (Purchase)_MA3 ETM 003",
"US_Ethel M_Holiday_1122_Mars_Q4_11/10-12/25_Facebook/Instagram_Awareness (Store Traffic)_MA3 ETM 003",
"US_Ethel M_Holiday_1122_Mars_Q4_11/30-12/25_Facebook/Instagram_Reach_Influencer Campaign_MA3 ETM 003",
"US_Twix_Core_1222_Mars_Q4_12/5-12/25_FB/IG_Reach_Holiday Cookie_Find Heat Test 2",
"434408_MARS, INCORPORATED_M&M'S BRAND CANDY_M&M's Easter OLV PG Q1-Q2'23_23332385_3/11/2023-4/9/2023",
"US_Dove Ice Cream_Core GM_0620_Mars_Q2-Q3_6/22-6/30, 7/10 - 8/16_Pinterest_Awareness_Social",
"US_American Heritage_American Heritage GM_0820_Mars_Q3_8/17 - 8/31_Pinterest_Awareness_Social_Ezine",
"US_American Heritage_American Heritage GM_0920_Mars_Q3_9/25 - 9/30_Pinterest_Traffic_Social_ E-Zine Flight 2",
"US_American Heritage_American Heritage GM_1020_Mars_Q4_10/21 - 11/8_Pinterest_Traffic_Social_",
"US_Dove / Galaxy_Seasonal GM_1120_Mars_Q4_11/9 - 12/20_Pinterest_Awareness_Social",
"US_American Heritage_American Heritage GM_1120_Mars_Q4_11/9 - 11/30_Pinterest_Traffic_Social - Seasonal",
"US_American Heritage_American Heritage GM_1120_Mars_Q4_11/24 - 11/30_Pinterest_Traffic_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Traffic_Social - Seasonal",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Awareness_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Traffic_Social - Holiday Ezine",
"US_M&Ms_Seasonal GM_1220_Mars_Q4_12/7 - 12/27_Pinterest_Awareness_Social_Holiday",
"US_M&Ms_Fudge Brownie GM_0121_Mars_Q1_1/11 - 2/21_Pinterest_Reach_Social",
"US_American Heritage_American Heritage GM_0221_Mars_Q1_2/9 - 3/31_Pinterest_Traffic_Social - Hot Cocoa",
"US_American Heritage_American Heritage GM_0221_Mars_Q1_2/9 - 2/14_Pinterest_Awareness_Social - Valentine's Day",
"US_Starburst_Seasonal_0221_Mars_Q1/Q2_2/22-4/4_Pinterest_Awareness_PBT Targeting",
"US_Dove / Galaxy_Core GM_0321_Mars_Q1-Q2_3/1 - 4/4_Pinterest_Awareness_Social_Tastemade",
"US_American Heritage_American Heritage GM_0321_Mars_Q1_3/3 - 3/31_Pinterest_Awareness_Social - Recipe/FGBC",
"US_M&Ms_Minis GM_0321_Mars_Q1-Q2_3/8 - 4/4_Pinterest_Awareness_Social Tastemade",
"US_American Heritage_American Heritage GM_0421_Mars_Q2_4/5 - 5/9_Pinterest_Consideration_Social - Mothers Day",
"US_American Heritage_American Heritage GM_0421_Mars_Q2_4/5 - 5/9_Pinterest_Awareness_Social - Pancakes",
"US_American Heritage_American Heritage GM_0521_Mars_Q2_5/10 - 6/27_Pinterest_Consideration_Social - Cherry Smoothie",
"US_American Heritage_American Heritage GM_0521_Mars_Q2_5/10 - 6/6_Pinterest_Awareness_Social - Cherry Smoothie",
"US_American Heritage_American Heritage GM_0721_Mars_Q3_7/1 - 9/26_Pinterest_Awareness_Social - Recipes",
"US_American Heritage_American Heritage GM_0721_Mars_Q3_7/7 - 8/15_Pinterest_Consideration_Social - Tablet Bar",
"US_M&Ms_Fudge Brownie GM_0821_Mars_Q3_8/2-10/10_Pinterest_Reach_Social",
"US_Twix_Seasonal GM_0921_Mars_Q3/Q4_9/20 - 10/31_Pinterest _Reach_Social",
"US_Snickers_Seasonal GM_1021_Mars_Q4_10/6 - 10/31_Pinterest_Reach_Social",
"US_Ethel M_Holiday Evergreen_1121_Mars_Q4_11/5 - 12/22_Pinterest_Conversion (ATC)",
"US_Starburst_Seasonal_0222_Mars_Q1_3/3-5/8_Pinterest_Awareness_NCS PBT",
"US_Orbit_Core_0322_Mars_Q1/Q2_3/9-5/8_Pinterest_Reach_NA",
"US_M&Ms_Crunchy Cookie GM_0322_Mars_Q1/Q2_3/21-5/1_Pinterest_Awareness_Social",
"US_Orbit_Core _0322_Mars_Q1/Q2_3/22-5/8_Pinterest_Reach_G9",
"US_Ethel M_Mother's Day_0422_Mars_Q2_4/19 - 5/8_Pinterest_Conversion (Purchase)",
"US_Dove_Large Promise GM_0722_Mars_Q3_7/18-9/25_Pinterest_Awareness_Native Targeting_NCS Study",
"US_Dove_Large Promise GM_0722_Mars_Q3_7/18-9/25_Pinterest_Awareness_Dove PBT_NCS Study",
"US_Dove_Large Promise GM_0922_Mars_Q3_9/1-9/25_Pinterest_Awareness_Native Targeting AV Coupon_NCS Study",
"US_Snickers_Seasonal Halloween GM_1022_Mars_Q4_10/3-10/31_Pinterest_Reach_Social",
"US_M&M's_Seasonal Minis GM_1022_Mars_Q4_10/12-12/25_Pinterest_Awareness_M&M's PBT_NCS Study",
"US_Ethel M_Holiday_1122_Mars_Q4_11/9 - 12/25_Pinterest_Conversion (Checkout) Prospecting",
"US_Ethel M_Holiday_1122_Mars_Q4_11/21 - 12/25_Pinterest_Conversion (Checkout) Retargeting",
"US_Ethel M_Holiday_1122_Mars_Q4_11/23 - 12/25_Pinterest_Conversion (Checkout) Prospecting_AV",
"US_5 Gum_Digital Core_PBU Spotify Audio FY_0123_MA3_GM5_PG",
"US_EssenceMediacom_Mars_Dove Core_Q3'23_PG (SO-597585)_GAM",
"US_EssenceMediacom_Mars_Dove Ice Cream_Q3'23_PG (SO-597567)_GAM",
"US_EssenceMediacom_Mars_M&M'S Ice Cream_Q3'23_PG (SO-597587)_GAM",
"US_EssenceMediacom_Mars_Snickers Core_Q3'23_PG (SO-597590)_GAM",
"US_EssenceMediacom_Mars_Starburst Airs Gummies_Q3'23_PG (SO-597599)_GAM",
"US_EssenceMediacom_Mars_Twix Cookie Dough_Q3'23_PG (SO-597592)_GAM",
"US_Extra Refreshers_Digital Core_PBU Spotify Audio FY_0223_MA3_GM5_PG",
"US_M&M's Ice Cream_Digital Core_PBU Spotify Audio FY_0523_MA3_MIC_PG",
"US_Mediacom_Mars_M&M'S Caramel Cold Brew Spotify Audio_Q2'23_PG (SO-572013)_GAM",
"US_Mediacom_Mars_M&M'S Purple_Q2'23_PG (SO-566551)_GAM",
"US_Mediacom_Mars_Skittles Core_Q2_PG (SO-581367)_GAM",
"US_Mediacom_Mars_Snickers Core_Q1'23_PG (SO-562320)_GAM",
"US_Mediacom_Mars_Snickers Core_Q2'23_PG (SO-566557)_GAM",
"US_Mediacom_Mars_Starburst Airs Gummies_Q2'23_PG (SO-581366)_GAM",
"US_Mediacom_Mars_Twix Cookie Dough_Q1_PG (SO-558723)_GAM",
"US_Mediacom_Mars_Twix Cookie Dough_Q2'23_PG (SO-566556)_GAM",
"US_Skittles_Digital Gummies_PBU Spotify Audio FY_0123_MA3_SKS_PG",
"US_Skittles_Digital Pride_PBU Spotify Audio FY_0623_MA3_SKS_PG",
"US_Starburst Equity_Digital_PBU Spotify Audio FY_0223_MA3_STB_PG",
"ca_ecg_moments_mw_na_gum_brand_olv_awareness_bid-adr_0123_media-io-o-3scbh",
"ca_ecg_p1-vnp_mw_na_gum_brand_olv_awareness_bid-adr_0323_media-io-o-3smgp",
"ca_kbs_digital_kind_na_oth_brand_display_awareness_bid-adr_0623_media-io-o-43thx",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122",
"CA_Mars_Excel Post Secondary, Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_1222",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122",
"US_Extra Refreshers BTS_Digital Core_PBU Spotify Audio FY_0823_MA3_GM5_PG",
"US_MMS_Caramel_Cold_Brew_mw_SPOTIFY Q3 PG_CON_Brand_audio_awareness_bid-adr_0223_NA-Q1-PBU- 3/27 to 8/13",
"MWC_Twix_Core_AUD_MULTI_PG_GAM_Spotify_NA_Q3",
"MWC_Dove_Core_AUD_MULTI_PG_GAM_Spotify_NA_",
"US_EssenceMediacom_Mars_Skittles Sours_Q3_PG (SO-619746)_GAM",
"US_EssenceMediacom_Mars_Starburst All Pink_Q3 '23_PG (SO-614488)_GAM",
"US_Dove Core_Digital PBU Spotify Audio FY_0223_MA3_DOV",
"US_EssenceMediacom_Mars_Snickers NFL_Q3-Q4 '23_PG (SO-610822)_GAM",
"US_Mediacom_Mars_Twix Core_Q1_PG (SO-555442)_GAM",
"US_Skittles_Digital Core_PBU Spotify Audio FY_0723_MA3_SKS_PG",
"MWC_Snickers_Seasonal__MULTI_PG__Spotify_NA_Q3",
"US_Mediacom_Mars_Twix Core_Q2'23_PG (SO-566555)_GAM",
"US_MMS_Caramel_Cold_Brew_mw_BLAVITY PG_CON_Brand_olv_awareness_bid-adr_0223_NA-Q1-PBU- 3/27 to 8/13",
"US_MCC_Crunchy Cookie_mw_BLAVITY PG_OTH_Brand_olv_awareness_bid-adr_0523_TTD/YouTube-Q3-PBU-5/15-8/13-MA3",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Blavity_AA_",
"MWC_Dove_Core_OLV_MULTI_PG_GAM_Blavity_AA_Q3",
"MWC_Starburst_Equity + All Pink_OLV_MULTI_PG_YM_Yieldmo_AA_Q3",
"MWC_Twix_Core_OLV_MULTI_PG_YM_Yieldmo_AA_Q3",
"MWC_M&Ms_Credit_OLV_MULTI_PG_GAM_Blavity_AA_Q3'23",
"Twix_Seasonal__MULTI_PG__Spotify_NA_Q3 to Q4",
"Snickers_Protein__MULTI_PG__Spotify_NA_Q3",
"US_5 Gum_Digital Core_PBU Spotify Audio Q4_1023_MA3_GM5_PG",
"US_DGIC_ICE-CREAM_MW_NA_ICE_BRAND_OLV_CONVERSIONS_BID-ADR_0423_TRADE-DESK-Q2-Q3-PBU-BLAVITY-4/17---8/24-MA3",
"US_DGIC_DOVE ICE CREAM Spotify Audio_MW_NA_ICE_BRAND_OLV_CONVERSIONS_BID-ADR_0423_TTD-Q2-PBU-4/17 TO 8/24-MA3",
"US_ORG_SPOTIFY-AUDIO-PG_MW_NA_GUM_BRAND_DISPLAY_REACH_BID-ADR_0323_THE-TRADE-DESK-Q1-PBU-3/1-3/31-MA3",
"US_ORG_SPOTIFY-AUDIO-PG_MW_NA_GUM_BRAND_DISPLAY_REACH_BID-ADR_1023_THE-TRADE-DESK-Q4-PBU-10/1-12/31-MA3",
"Snickers_Seasonal__MULTI_PG__Spotify_NA_Q4",
"Snickers_Protein__MULTI_PG__Spotify_NA_Q4",
"US_Starburst All Pink_Digital Core_PBU Spotify Audio Q4_1023_MA3_STB_PG",
"Excel_TikTok_P13_O-34NL7-R4",
"CA_Excel Gum_2022 Moments TikTok Reach_1122_O-3NX9J",
"CA_Excel_TikTok P12_O-34NL7-R4",
"CA_Excel Gum_Young Adults TikTok Reach_1022_O-3MXK5",
"CA_Excel Gum_Young Adults TikTok Traffic_1022_O-3MXK5",
"CA_Excel Gum_2021 FreshStart Influencers_101421_M21_EB_12",
"CA_Excel Gum_2021 Excel FreshStart_061821_M21_EB_12",
"CA_Excel Gum_2021 Excel Moments_0121_M21_EB_7",
"CA_M&Ms_VNP P13_1122_O-3343J-R6",
"CA_M&Ms_VNP P11_1022_O-3343J-R6",
"CA_M&Ms_VNP P6_0522_O-3343J-R6",
"CA_Skittles_TikTok P4_0322_O-35818-R1",
"CA_Skittles_Target The Rainbow_FR_1121",
"CA_Mars_Bars EN_1122_O-3N53X",
"CA_Mars_Bars FR_1122_O-3N53X",
"CA_Temptations_2022 Temptations Holiday Snap Ad_1122_O-3NNWL",
"CA_Greenies_2022 Greenies Holiday Awareness_1122_O-3MMPR",
"CA_Cesar_WC & SC National Launch EN_O-3HSS1-R1",
"CA_Cesar_FL2 Awareness EN_0922_O-3HSS1-R1",
"CA_Cesar_FL2 Awareness FR_0922_O-3HSS1-R1",
"CA_Cesar_FL2 Awarenes Bonus Media_O-3HSS1-R1",
"2020 - Seeds of Change - BLS - Oct 19 - Nov 29- Awareness - English - CPE:",
"CA_Nutro_ Feed Clean Pinterest_1122_O-3M1HT-R1",
"Iams Dog 2022 H2 Awareness Health Concerns & Wellbeing_O-3M1M0",
"Iams Dog 2022 H2 Awareness Early Ownership_O-3M1M0",
"Iams Dog 2022 H2 Awareness Munchkins_O-3M1M0",
"Iams Cat 2022 H2 Awareness Kitten_O-3ML7P",
"Iams Cat 2022 H2 Awareness Health Concerns & Wellbeing_O-3ML7P",
"Iams Cat 2022 H2 Awareness Adult Nutrition_O-3ML7P",
"US_Ben's Original_Ben's 1H_0123_MZS_AMF",
"CA_Cesar_Digital Carting Pinterest FR_0922_O-3HSS1",
"CA_Cesar_Digital Carting Pinterest EN_0922_O-3HSS1",
"CA_Iams Dog_EOY Regional Traffic_1220_M20*IA*10",
"CA_Iams Dog - Relevance Traffic Campaign - 0321_M21*IA*02",
"CA_Iams Cat_Digital Couponing Pinterest Traffic_1022_O-3ML8H",
"CA_Iams Dog_Digital Couponing Pinterest Traffic_1022_O-3MKY4",
"Iams Dog 2022 H2 Consideration Health Concerns & Wellbeing_O-3M1M0",
"Iams Dog 2022 H2 Consideration Early Ownership_O-3M1M0",
"Iams Dog 2022 H2 Consideration Munchkins_O-3M1M0",
"Iams Cat 2022 H2 Consideration Health Concerns & Wellbeing_O-3ML7P",
"Iams Cat 2022 H2 Consideration Adult Nutrition_O-3ML7P",
"Iams Cat 2022 H2 Consideration Kitten_O-3ML7P",
"CA_Iams Cat_Couponing Pinterest Traffic_0322_O-3TPV4",
"Dentastix Halloween - Homefeed Spotlight",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_Display EN",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_Quebecor Video PMP Deal",
"US_Pedigree_Wet Display Conversion_0122_MA3_PDG",
"US_Greenies_Supplements Display Conversions_1022_MA3_GRS",
"US_Greenies_Pill Pockets Display Conversions_1022_MA3_GRS",
"US_Greenies_Canine Display Conversions_1222_MA3_GRN",
"US_Pedigree_Adoption/Loyalty_0820_Mars_Q3_8/3 - 10/31_Pinterest_Awareness_Social",
"US_Pedigree_Rescue Doodles_0322_Mars_Q2/Q3_3/14 - 5/1_Pinterest_Awareness_Social",
"USE_ US_Pedigree_Ipouch_0322_Mars_Q2/Q3_3/22 - 5/29_Pinterest_Awareness_Social",
"US_Pedigree_Marrobites_0422_Mars_Q2_4/11 - 5/29_Pinterest_Awareness_Social",
"US_Cesar_Wholesome Bowls_0321_Mars_Q2_3/29-5/30_Pinterest_Reach_Social_Wholesome Bowls Test",
"US_Cesar_Real Food_0122_Mars_Q1/Q2_1/24-5/29_Pinterest_Reach_Social",
"Mars_FY20_Q3_US_7/13-8/23_NA_Nutro_Reach_Pinterest_Social",
"US_Nutro_Core_0121_Mars_Q1_1/4-3/28_Pinterest_Brand Awareness_Flight 1",
"US_Nutro_Core_0321_Mars_Q2_3/29-6/27_Pinterest_Brand Awareness_Flight 2",
"US_Nutro_Core_0621_Mars_Q3_6/28-9/26_Pinterest_Brand Awareness_Flight 3",
"US_Nutro_Core_0122_Mars_Q1_01/10 - 03/27_Pinterest_Brand Awareness_Non-GMO",
"US_Nutro_So Simple_0422_Mars_Q2_4/13-5/29_Pinterest_Reach_Social",
"US_Sheba_NCS Study_0720_Mars_Q3_7/24 - 9/25_Pinterest_Awareness_Social",
"US_Sheba_Core_0321_Mars_Q2_3/29 - 5/30_Pinterest_Reach_Social - Bistro Test",
"US_Temptations_Camping_0820_Mars_Q3_8/3 - 9/27, 10/2 - 10/9_Pinterest_Awareness_Social_NCS Study",
"US_Temptations_Holiday_1120_Mars_Q4_11/11 - 12/27_Pinterest_Awareness_Promoted Pins",
"US_Temptations_Sublines_0321_Mars_Q1_3/1- 3/28_Pinterest_Awareness_Promoted Pins",
"US_Temptations_Sublines_0321_Mars_Q2_3/29-6/27_Pinterest_Awareness_Promoted Pins",
"US_Temptations_Sublines_0621_Mars_Q3_6/28 - 9/26_Pinterest_Awareness_Promoted Pins",
"US_Temptations_Purrrr-ee_0122_Mars_Q1_1/3 - 3/27_Pinterest_Awareness_Promoted Pins",
"US_Greenies_GCBC/Balloon_0720_Mars_Q3_7/15-9/27_Pinterest_Awareness_Promoted Pins",
"US_Greenies_Doggy IQ/Holiday_1020_Mars_Q4_10/1 - 12/27_Pinterest_Awareness_Promoted Pins",
"US_Greenies_Feline_0122_Mars_Q1_1/3 - 3/27_Pinterest_Awareness_Promoted Pins",
"US_Seeds of Change_Home Hacks_0720_Mars_Q3_7/15 - 9/18_Pinterest_Brand Awareness_Social_Brand Lift_Smart Commerce",
"US_Seeds of Change_Brand and Home Hacks_1220_Mars_Q4_12/1/2020 - 1/2/2021_Pinterest_Awareness_Social",
"US_Seeds of Change_Brand and Home Hacks_0121_Mars_Q1_1/5 - 1/31_Pinterest_Awareness_Social",
"US_Seeds of Change_Tastemade_0921_Mars_Q4_9/27 - 12/5_Pinterest_Awareness_Social",
"US_Seeds of Change_Core GM_0422_Mars_Q2_4/18-6/5_Pinterest_Reach_Social",
"US_Seeds of Change_Healthy Eating for Real Life_0822_Mars_Q3_8/1-9/30_Pinterest_Reach_Social",
"US_Seeds of Change_Core_1222_Mars_Q4_12/14-12/25_Pinterest_Awaresness_NA",
"US_IAMS Dog_Large Breed_0720_Mars_Q3_7/27-9/27_Pinterest_Brand Awareness_Social",
"US_IAMS Dog_Large Breed_0920_Mars_Q4_9/28-12/27_Pinterest_Brand Awareness_Social",
"US_IAMS Dog_Healthy Weight_0122_Mars_Q1_01/10-03/27_Pinterest_Brand Awareness_Social",
"US_IAMS Dog_Healthy Weight_0622_Mars_Q2_06/16-06/26_Pinterest_Brand Awareness_Social",
"US_Ben's Original_Core_Ben's 2H_0722_MZS_AMF",
"CA_Greenies Dog Dental Health | Net Media: O-3SV30-R1 | Standard Display | Jan 30 - Feb 28 2023",
"CA_Dentastix Canine Dental Health | Net Media: O-3SWLC | VCB | Jan 30 - Feb 28 2023",
"CA_Cesar Amazon Full Year | Awareness & Education | Net Media IO: O-3Y3TP | April & August [English]",
"CA_Iams Dog Amazon Full Year | Awareness & Education | Net Media IO: O-3YGM6 | April & June [French]",
"CA_Nutro Dog Amazon Full Year | Awareness & Education | Net Media IO: O-40X1Y | May - June",
"CA_Greenies Feline Dental Health | Net Media: O-36WLJ | VCB | Jan 30 - Feb 28 2023",
"CA_Greenies Feline Dental Health | Net Media: O-36WLJ | Standard Display | Jan 30 - Feb 28 2023",
"CA_Iams Cat Amazon Full Year | Awareness & Education | Net Media IO: O-3YFKL | April 2023 [English]",
"CA_Temptation Amazon Full Year | Awareness & Education | Net Media IO: O-3WLC5 | May & August [English]",
"CA_Dentastix Canine Dental Health | Net Media: O-3SWLC | Standard Display | Jan 30 - Feb 28 2023",
"CA_Greenies Dog Dental Health | Net Media: O-3SV30-R1 | VCB | Jan 30 - Feb 28 2023",
"CA_Temptation Amazon Full Year | Awareness & Education | Net Media IO: O-3WLC5 | Mar 1 - Mar 31 2023 [English]",
"CA_Temptation Amazon Full Year | Awareness & Education | Net Media IO: O-3WLC5 | Mar 1 - Mar 31 2023 [French]",
"CA_Cesar Amazon Full Year | Awareness & Education | Net Media IO: O-3Y3TP | April & August [French]",
"CA_Iams Dog Amazon Full Year | Awareness & Education | Net Media IO: O-3YGM6 | April & June [English]",
"CA_Iams Cat Amazon Full Year | Awareness & Education | Net Media IO: O-3YFKL | April 2023 [French]",
"CA_Greenies Dog Smartbites | Net Media: O-48FLH | Sep 18 - Oct 29",
"CA_Cesar_Amazon Always On REC Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | Oct 17 - Dec 31 2022_0622",
"CA_Greenies_Dog | Amazon Always On Standard Display | Net Media IO: O-3CL9K | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Cesar_Amazon Always On Standard Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | Oct 17 - Dec 31 2022_0622",
"CA_Temptations_Amazon Always On Standard Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | July 6 - Dec 31 2022_0622",
"2019_1 Day_Chewy_Iams_Programmatic & Shopper_O-1NMNC",
"2019_3 months_MZS_Chewy_Nutro Ultra Always On_O-1N15Z",
"2019_Full Year_MA3_Cesar_Cesar Wet GM_Digital_Display Shopper_O-1FGWF-R17",
"2019_Full Year_MA3_Temptations_Temptations_Display Shopper_Digital_O-1FGX1-R19",
"2019_Q3_MA3_Cesar_Cesar Dry_PBU_Display_O-1MFYF - MTA",
"2019_Q3_MA3_Cesar_Cesar Wet GM_Digital_Display Shopper_O-1MFX2 - MTA",
"2019_Q4_MA3_Crave_Dog_Digital_Display_OE_1PD_O-1SHDB",
"2019_Q4_MA3_Temptations_Temptations_Digital_Display_OE_1PD_O-1R9W2_R23",
"2019_Q3-Q4_MA3_Cesar_Cesar Wet GM_Digital_Video_Peas Please_O-1MFX2_R10",
"2019_Q3-Q4_MA3_Cesar_Cesar Wet GM_Digital_Video_The Dish_O-1MFX2_R10",
"2019_Q3-Q4_MA3_Cesar_Cesar Wet GM_Digital_Video_Mixed or Not_O-1MFX2_R10",
"2019_Q4_MA3_IAMS_IAMS Cat_Digital_Video_OE_O-1SHD4",
"CA_Nutro_Dog | Amazon Always On REC Display | Net Media IO: O-3CL9N-R2 | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Greenies_Dog | Amazon Always On REC Display | Net Media IO: O-3CL9K | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Greenies_Feline | Amazon Always On Standard Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | July 6 - Dec 31 2022_0622",
"CA_Iams Dog_Amazon Always On Standard Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | July 6 - Dec 31 2022_0622",
"CA_Iams Cat_Amazon Always On Standard Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | July 6 - Dec 31 2022_0622",
"CA_Greenies_Feline | Amazon Always On REC Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Whiskas_Amazon Always On REC Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Temptations_Amazon Always On REC Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Iams Dog_Amazon Always On REC Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"CA_Iams Cat_Amazon Always On REC Display | Net Media IO: Pending | Tech Fee IO: Pending | CPE: Pending | June 28 - Dec 31 2022_0622",
"2023_ca_grf_greenies-cat-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0323_en-o-3wbtq_GMI Exception",
"2023_ca_grn_greenies-dog-always-on-amazon-product-consideration_pn_na_dmm_brand_display_awareness_bid-nadr_0323_en-o-3wb57_GMI Exception",
"2023_ca_nut_nutro-always-on-amazon-product-consideration_pn_na_dmm_brand_display_awareness_bid-nadr_0523_en-o-3y1ev_GMI Exception",
"2023_ca_whi_whiskas-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0523_fr-o-411ns_GMI Exception",
"2023_ca_ces_cesar-always-on-amazon-product-consideration_pn_na_dmm_brand_display_awareness_bid-nadr_0423_en-o-3wpe7_GMI Exception",
"2023_ca_tem_temptations-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0323_en-o-3wn4f_GMI Exception",
"2023_ca_dts_dentastix-always-on-amazon-product-consideration_pn_na_dmm_brand_display_awareness_bid-nadr_0323_o-3w9m0_GMI Exception",
"2023_ca_idg_iams-dog-always-on-amazon-product-consideration_pn_na_dmm_brand_display_awareness_bid-nadr_0323_en-o-3w9eh_GMI Exception",
"2023_ca_she_sheba-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0423_en-o-3y5lg_GMI Exception",
"2023_ca_ict_iams-cat-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0323_en-o-3wn57_GMI Exception",
"2023_ca_tem_temptations-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0323_fr-o-3wn4f_GMI Exception",
"2023_ca_whi_whiskas-always-on-amazon-product-consideration_pn_na_cml_brand_display_awareness_bid-nadr_0523_en-o-411ns_GMI Exception",
"CA_Mars_Greenies Always On | Net Media IO: O-3K3M3  | Tech Fee IO: | _0922 | Standard Display English",
"CA_Mars_Greenies Always On | Net Media IO: O-3K3M3  | Tech Fee IO: | _0922 | Positioning Display English",
"CA_Mars_Greenies Always On | Net Media IO: O-3K3M3  | Tech Fee IO: | _0922 | Positioning Display French",
"CA_Mars_Greenies Always On | Net Media IO: O-3K3M3  | Tech Fee IO: | _0922 | Standard Display French",
"CA_Mars_IAMS Dog Couponing | Net Media IO: O-3M355 | Tech Fee IO | | _1122 - Standard Display English",
"CA_Mars_IAMS Dog Couponing | Net Media IO: O-3M355 | Tech Fee IO | | _1122 - Standard Display Prospecting English",
"CA_Mars_Iams Dog_Always On Display | Net Media IO: O-3GN4H | Standard Display English - Chicken",
"CA_Mars_Iams Dog_Always On Display | Net Media IO: O-3GN4H  | Standard Display English Healthy Digestion",
"CA_Mars_Iams Dog_Always On Display | Net Media IO: O-3GN4H  | Standard Display English Healthy Senior/Aging",
"CA_Mars_Iams Dog_Always On Display | Net Media IO: O-3GN4H  | Standard Display English Healthy Weight",
"CA_Mars_Iams Dog_Always On Display | Net Media IO: O-3GN4H | Standard Display English - Puppy",
"CA_Mars_Iams Dog_Always On Display | Net Media IO: O-3GN4H  | Positioning Display English TripleLift",
"CA_Mars_IAMS Cat Couponing | Net Media IO: | Tech Fee IO | CPE: T_M22_CA_12 | _1022 - Standard Display Behavioural English",
"CA_Mars_IAMS Cat Couponing | Net Media IO: | Tech Fee IO | CPE: T_M22_CA_12 | _1022 - Standard Display Prospecting English",
"CA_Mars_Iams Cat_Always On Display | Net Media IO: O-3M8KP | Tech Fee IO: O-38MKN | T_M22_CA_6 |_1122 | Standard Display English",
"CA_Mars_Iams Cat H2_Always On Display | Net Media IO: O-3M8KP | Tech Fee IO: O-3M8KN | T_M22_CA_6 |_1122 | GumGum Shoppable English",
"CA_Mars_Whiskas Purr More | Net Media IO: O-3M188 | Tech Fee IO: | T_M22_WD_6 |_1122 | Outstream",
"CA_Mars_Temptations Holiday | Net Media IO: O-3M6Z0 | _1122 - Native Display English",
"US_Campaign_2020",
"Mars - Skittles - Canada 151 - Traffic - English - MW8*SK*003 - IWOe9599",
"Mars - Skittles - Canada 151 - Reach - English - MW8*SK*003 - IWOe9599",
"Mars - Skittles - Halloween - Awareness - English - MW8*SK*007 - IWOe10366",
"CA_Greenies_2022 Greenies Feline Awareness_1122_O-3N46J",
"Awareness Greenies Holiday FY21",
"Engagement - Greenies - AR Lens",
"2022 Q4 Temptations Holiday VCB | Net Media IO: O-3M6YZ | Tech Fee IO: Pending | CPE: | Video | Nov 23 - Dec 31, 2022",
"2022 Q4 Greenies Feline VCB | Net Media IO: | Tech Fee IO: Pending | CPE: | Video | Nov 22 - Dec 4, 2022",
"2022 Q4 Greenies Dog Holiday | Net Media IO: O-3M0KV | Tech Fee IO: Pending | CPE: M22 WD 6 |  Video | Nov 25 - Dec 29, 2022",
"2022 Q4 Whiskas Purr More | Net Media IO: O-3M188 | Tech Fee IO: Pending | CPE: M22 WD 6 |  Video | Nov 18 - Dec 31, 2022",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_Instream After a Meal EN",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_Instream Styling Up EN",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_Instream Styling Up FR",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_FourSquare  After A Meal FR",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Game (3P Segments)",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Game (In Market)",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Beauty (3P Segments)",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Scare (3P Segments)",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Sharethrough PMP EN Beauty",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Teads PMP EN Scare",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Teads PMP FR Game",
"CA_Mars_Excel Post Secondary, Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_1222_Geo Targeting English",
"CA_Mars_Excel Post Secondary, Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_1222_Geo Targeting French",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00HQXV8DM",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00HQXVCJC",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B06XRVBXWS",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00HQXV6OI",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00HQXUZFO",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B0BD923XPL (Original)",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B0BD94SVY9",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B07KDY4ZJ5",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B084KHXCN6",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00LAEFWFI",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B0064V4E88",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00LAFNEFW",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00EOAQW3G (Original)",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00HQXV8IM",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B09R2L65KX",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B0BD94J3LJ",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B006V75ILW",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00HQXVALW",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00BPXNZYO",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00BPXO0GQ",
"CA_Mars_Excel Test & Learn Campaign, Net Media IO: O-3LMXM, Tech Fee IO: O-3QHCR, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B09R2JZNCM",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00LAEFWIU",
"CA_Mars_M&Ms Test & Learn Campaign, Net Media IO: O-3LMXY, Tech Fee IO: O-3QHHV, CPE: M22 COS 005_Test and Learn Campaign_1022_1022 ASIN B00LAFNBU0",
"US_5 Gum_Digital Core Control PBU OLV FY_1022_MA3_GM5",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_DoubleMint_Digital PBU OLV FY_0922_MA3_DMG",
"US_M&M's Mix_Digital PBU OLV FY_1122_MA3_MMX",
"US_M&M's Seasonal Christmas AA_Digital PBU OLV FY_1122_MA3_MSE_Connatix PG",
"US_M&M's _Seasonal Christmas PBU OLV FY_1122_MA3_MSE",
"US_Orbit Core_Digital PBU OLV FY_0722_MA3_ORB",
"US_Snickers Nut Brownie HM_Digital PBU OLV FY_1022_MA3_SNB",
"US_MMs_Crunchy Cookie_0722_MZS_COP",
"US_Skittles BBL_Digital PBU OLV FY_0722_MA3_SKG",
"US_Snickers Core Firepower Test_Digital PBU OLV FY_1122_MA3_SNI",
"US_M&M's Peanut Butter_Digital PBU OLV FY_1022_MA3_MPB",
"US_Royal Canin_Petco_1022_RYC_RRD",
"US_Royal Canin_Petsmart_1022_RYC_RRD",
"US_M&M's Peanut Butter AA_Digital PBU OLV FY_1022_MA3_MPB_Connatix PG",
"US_Snickers Ice Cream Test Cell 1_Digital PBU OLV FY_0722_MA3_SIC",
"US_Snickers Ice Cream Test Cell 2_Digital PBU OLV FY_0722_MA3_SIC",
"CA_UBN_BEN'S-ORIGINAL_FOOD_NA_OTH_BRAND_OLV_AWARENESS_BID-ADR_0223_TRADE-DESK-Q1-Q2-PBU-2/3-TO-6/1-MA3",
"US_M&M's Ice Cream_Digital PBU OLV FY_0422_MA3_MIC",
"US_5 Gum_Digital Core PBU OLV FY_0122_MA3_GM5_PG",
"US_MW_Halloween 22_0722_MZS_COP",
"US_Starburst Airs_Digital PBU OLV FY_0722_MA3_STB",
"US_DOV_INSTAGRANTS_MW_NA_CHO_BRAND_SOCIAL_REACH_BID_0223_FACEBOOK-Q1-SOCIAL-2/17-2/24-MA3",
"US_Insurgent Gum_Double Mint_1122_Mars_Q4_11/1-12/25_Snapchat_Awaresness_NA",
"US_M&Ms_Mix GM_1122_Mars_Q4_11/21-12/25_Snapchat_Reach_Social_70/30- Snap Ads/Lens_AV",
"US_Extra_Equity Core_0922_Mars_Q4_9/26-11/27_Snapchat_Awareness_Cell B",
"US_Extra_Equity Core_0922_Mars_Q4_9/26-11/27_Snapchat_Awareness_Cell A",
"US_5 Gum_Masterpieces_1022_Mars_Q4_10/16-12/25_Snapchat_Awareness_NA",
"US_Orbit_Core_0922_Mars_Q4_10/17-12/25_Snapchat_Awareness_Cell A_ BAU",
"US_Twix_Core GM_1122_Mars_Q4_11/14-12/25_Snapchat_Reach_Social_NCS Study_70/30- Snap Ads/Lens NCS Study Cell 2 Holiday Cookie + PBT_AV",
"US_Twix_Core GM_1122_Mars_Q4_11/14-12/25_Snapchat_Reach_Social_NCS Study_70/30- Snap Ads/Lens NCS Study Cell 1 Holiday Cookie BAU_AV",
"US_M&Ms_Seasonal Christmas GM_1122_Mars_Q4_11/14-12/25_Snapchat_Reach_Social_70/30 - Snap Ads/Lens",
"US_Orbit_Core_0922_Mars_Q4_10/17-12/25_Snapchat_Awareness_Cell B_ HVC Creative",
"US_Greenies_Greenies Dog_1022_Mars_Q4_10/5-11/23_Snapchat_Reach_Lens",
"US_Insurgent Gum_Juicy Fruit_1122_Mars_Q4_11/21-12/25_Snapchat_Awaresness_NA",
"US_Twix_Core GM_1122_Mars_Q4_11/14-12/25_Snapchat_Reach_Social_NCS Study_70/30- Snap Ads/Lens NCS Study Cell 2 Holiday Cookie BAU_AV",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_1121_11/15-to-12/12",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_0422_4/21-to-6/26",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_0622_6/22-to-6/30-bonus-media",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_0822_q3/q4-8/23-to-10/9-eba",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_0822_q3/q4-8/23-to-10/9",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_0822_q3/q4-8/23-to-10/9-pbt",
"us_ubn_core_food_na_oth_brand_social_awareness_bid-adr_1222_12/13-to-12/26",
"US_Tasty Bite_Core_0522_Mars_Q2_5/19-6/16_Pinterest_Reach_Social",
"US_Twix_Seasonal Halloween GM_1022_Mars_Q4_10/3-10/31_Pinterest_Reach",
"2021_Q2/Q3_Mars Shopper_CVS Easter_Awareness",
"US_M&Ms_Minis GM_1121_Mars_Q4_11/17-12/26_Pinterest_Reach_Social Tastemade",
"US_M&M's_Seasonal Minis GM_1022_Mars_Q4_10/12-12/25_Pinterest_Awareness_Native Targeting_NCS Study",
"US_Snickers NFL_Digital PBU OLV FY_1022_MA3_SNN #2",
"US_Snickers Core_Digital PBU OLV FY_1022_MA3_SNI #2",
"US_Snickers Core HM_Digital PBU OLV FY_1022_MA3_SNI #2",
"US_Snickers Nut Brownie_Digital_PBU_OLV_FY_1022_MA3_SNB #2",
"US_Extra Gum_MA3 Digital Extra Core_Cookie_0921",
"2020 Starburst Core",
"MWC_Snickers_NFL_AUD_MULTI_PG_GAM_Spotify_NA_Q3-Q4",
"MWC_M&Ms_Halloween SL_AUD_MULTI_PG_GAM_Spotify_NA_Q4",
"US_Extra Refreshers_Digital Core_PBU Spotify Audio Q4_1023_MA3_GM5_PG",
"MWC_M&Ms_Halloween_AUD_MULTI_PG_GAM_Spotify_NA_Q4",
"MWC_Dove_Core_AUD_MULTI_PG_GAM_Spotify_NA_Q4",
"MWC_M&Ms_Holiday_AUD_MULTI_PG_GAM_Spotify_NA_Q4",
"MWC_Twix_Core_AUD_MULTI_PG_GAM_Spotify_AA_Q4",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Blavity_AA_Q4",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Yieldmo_AA_Nov 2023",
"MWC_M&Ms_Holiday SL_AUD_MULTI_PG_GAM_Spotify_NA_Q4",
"MWC_M&Ms_Core_OLV_MULTI_PG_GAM_Yieldmo_AA_Q4",
"3727245 - M&M's Super Bowl -1/24 - DV360",
"3727457 - M&M's Super Bowl - 1/27 - DV360",
"3727535 - Mars M&Ms AdBlitz MH",
"49985_Mars Snickers Core AA PG Q1'2023",
"Insider: PG-NA-23-0710 Mars Confections PG M&M's Cold Brew Q2 2023",
"50047_Mars Extra AA PG Q1'2023",
"50048_Mars Extra AA PG Q2'2023",
"52385_Mars Extra Refreshers BTS PG - Q3'23",
"52475_Mars Extra PG 3Q23",
"50080_Mars MWC 5 Gum PG Q1'2023",
"50750_MWC 5 Gum AA PG 2Q'23",
"50608_Mars MWC 5 Gum Audience Intelligence PG Q2'2023",
"53026_Mars 5 Gum - PG Q3'23",
"50045_Mars Orbit AA PG Q1'2023",
"50046_Mars Orbit AA PG Q2'2023",
"52474_Mars Orbit PG 3Q23",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Vevo_AA_Q4",
"US_ORB_Core_mw_na_con_Brand_DCO-Connection-Test_awareness_bid-adr_0923_NA-Q1-PBU-9/1 to 12/31",
"US_Starburst_Seasonal_0222_Mars_Q1_3/3-5/8_Pinterest_Awareness_NCS Native Targeting",
"US_Extra_Core_0321_Mars_Q2_4/5-6/27_Pinterest_Awareness_Moments Targeting NCS Study",
"US_Extra_Core_0821_Mars_Q3_8/9-9/26_Pinterest_Awareness_NA",
"US_SKT_SHAZAM_MW_NA_FRC_BRAND_SOCIAL_AWARENESS_BID-ADR_0223_REDDIT-Q1-SOCIAL-3/9-to-4/23-MA3",
"US_SKT_GUMMIES_MW_NA_FRC_BRAND_SOCIAL_AWARENESS_BID-ADR_0223_REDDIT-Q1-SOCIAL-2/20-to-4/23-MA3",
"US_Extra_Core_1022_Mars_Q4_10/3-11/27_Reddit_Awareness_Interest Targeting",
"US_Extra_Core_0222_Mars_Q1_2/21-5/15_Reddit_Awareness_NCS",
" US_Extra_Core_0222_Mars_Q1_3/1-5/15_Reddit_Awareness_NCS",
"US_Extra_Core_1022_Mars_Q4_10/5-12/5_Reddit_Awareness_ROS",
"US_Extra_Core_0222_Mars_Q1_3/8-5/15_Reddit_Awareness_AV NCS v2",
"US_Extra_Core_0222_Mars_Q1_2/21-5/15_Reddit_Awareness_AV NCS ",
"US_Extra_Core_0222_Mars_Q1_2/21-5/15_Reddit_Awareness_NCS COPY",
"US_Orbit Gum _Core_0622_Mars_Q2_6/1-7/24_Reddit_Awareness_",
"US_5 Gum_Core_1022_Mars_Q4_11/7-12/25_Reddit_Awareness_NA",
"US_5 Gum_Masterpieces_1022_Mars_Q4_11/7-11/21_Reddit_Awaresness_AV",
"US_Skittles_Core_0321_Mars_Q2_3/29-5/23_Reddit_Awareness_Platform Proveout NCS Study",
"US_Skittles_Lime_0721_Mars_Q3_7/30-8/23_Reddit_Awareness_NA",
"US_Extra_Core_0621_Mars_Q3_6/28-8/24_Reddit_Awareness_We're Back_ NCS Study_Targeting ",
"Mars_FY2020_Q1_2/4-3/1_Skittles Dips_Reddit_Reach_NA",
"Mars_FY2020_Q1_2/4-3/2_Skittles Dips_Reddit_Reach_NA",
"test",
"DO NOT USE",
"US_Nutro_0721_Mars_Q3_7/12 - 9/26_Reddit_Brand Awareness and Reach_Promoted Posts",
"US_Nutro_0921_Mars_Q3_9/8 - 9/26_Reddit_Brand Awareness and Reach_Promoted Posts AV",
"us_she_kitten_pn_na_cml_brand_social_reach_bid-adr_0823_reddit-q3-to-q4-social-8/21 to 10/29-ma3-NCS Study",
"US_Temptations_0121_Mars_Q1_1/27 - 2/14_Reddit_Brand Awareness and Reach_Promoted Posts",
"US_Temptations_NCS Study_0321_Mars_Q1/Q2_3/4 - 5/5_Reddit_Brand Awareness and Reach_Cell #1: Brand",
"US_Temptations_NCS Study_0321_Mars_Q1/Q2_3/4 - 5/5_Reddit_Brand Awareness and Reach_Cell #2: Bespoke",
"US_Temptations_NCS Study_0321_Mars_Q1_3/18 -3/28_Reddit_Brand Awareness and Reach_Cell #2: Bespoke - Incremental",
"US_Temptations_NCS Study_0321_Mars_Q1_3/18-3/28_Reddit_Brand Awareness and Reach_Cell #1: Brand - Incremental",
"US_Temptations_USH_0122_Mars_Q1_1/3 - 2/27_Reddit_Reach_Hispanic Market",
"DNU US_SNI_CORE_MW_NA_CHO_BRAND_SOCIAL_REACH_BID-ADR_0423_TIKTOK-Q2-SOCIAL-4/3-TO-5/14-MA3-NCS-STUDY-CELL-1-BAU-CREATOR-HVC",
"CA_Mars Bars_Pinterest Awareness EN_0822_O-3GS6S",
"CA_Mars Bars_Pinterest Awareness FR_0822_O-3GS6S",
"CA_Mars Bars_Awareness Pinterest EN_1022_O-3M4MC-R1",
"CA_Mars Bars_ Awareness Pinterest FR_1022_O-3M4MC-R1",
"M21_MM_Awareness EN_1221_CPE M21*MM*017",
"M21_MM_Awareness FR_1221_CPE M21*MM*017",
"CA_M&Ms_Social Pinterest EN P4_0322_O-36JHL-R2",
"CA_M&Ms_Social Pinterest FR P4_0322_O-36JHL-R2",
"CA_M&Ms_Social Pinterest EN P5_0422_O-36JHL-R2",
"CA_M&Ms_Social Pinterest FR P5_0422_O-36JHL-R2",
"CA_M&Ms_Social Pinterest EN P6_0522_O-36JHL-R2",
"CA_M&Ms_Social Pinterest EN P7_0622_O-36JHL-R2",
"CA_M&Ms_Social Pinterest FR P7_0622_O-36JHL-R2",
"CA_M&Ms_Social Pinterest EN P9_0822_O-36JHL-R2",
"CA_M&Ms_Social Pinterest FR P9_0822_O-36JHL-R2",
"CA_M&Ms_Social Pinterest EN P13_1122_O-36JHL-R2",
"CA_M&Ms_Social Pinterest FR P13_1122_O-36JHL-R2",
"Greenes Search Spotlight - Nov 27",
"CA_Cesar_Digital Carting Pinterest FR_0922_O-3HSS1",
"CA_Cesar_Digital Carting Pinterest EN_0922_O-3HSS1",
"Mars_FY20_Q3_US_7/13-8/23_NA_Nutro_Reach_Pinterest_Social",
"US_Nutro_Core_0121_Mars_Q1_1/4-3/28_Pinterest_Brand Awareness_Flight 1",
"US_Nutro_Core_0321_Mars_Q2_3/29-6/27_Pinterest_Brand Awareness_Flight 2",
"US_Nutro_Core_0621_Mars_Q3_6/28-9/26_Pinterest_Brand Awareness_Flight 3",
"US_Nutro_Core_0122_Mars_Q1_01/10 - 03/27_Pinterest_Brand Awareness_Non-GMO",
"US_Nutro_So Simple_0422_Mars_Q2_4/13-5/29_Pinterest_Reach_Social",
"US_Cesar_Wholesome Bowls_0321_Mars_Q2_3/29-5/30_Pinterest_Reach_Social_Wholesome Bowls Test",
"US_Cesar_Real Food_0122_Mars_Q1/Q2_1/24-5/29_Pinterest_Reach_Social",
"US_Pedigree_Adoption/Loyalty_0820_Mars_Q3_8/3 - 10/31_Pinterest_Awareness_Social",
"US_Pedigree_Rescue Doodles_0322_Mars_Q2/Q3_3/14 - 5/1_Pinterest_Awareness_Social",
"USE_ US_Pedigree_Ipouch_0322_Mars_Q2/Q3_3/22 - 5/29_Pinterest_Awareness_Social",
"US_Pedigree_Marrobites_0422_Mars_Q2_4/11 - 5/29_Pinterest_Awareness_Social",
"US_Kroger_Bens Taco Night_0422_MZS_KRG",
"US_Kroger_Seeds Of Change_0422_MZS_KRG",
"US_Sam's Club_Concessions Wave 2_0722_MZS_SMG_043",
"US_Snickers_NFL & Fantasy Football_0821",
"US_Kroger_Dentastix_0622_MZS_KPC _008",
"US_Kroger_Crunchy Cookie_0422_MZS_KRG_009",
"US_Kroger_Summer Wave 1 Music_0522_MZS_KRG _012",
"US_Kroger_Skittles Pride_0622_MZS_KRG _013",
"US_Kroger_Starburst Airs_0522_MZS_KRG _014",
"2019_Full Year_MA3_Snickers_Snickers NFL Fantasy_Digital",
"US_Snickers_MA3 Snickers NFL Fantasy_0720",
"2019_Full Year_MA3_Uncle Ben's_Uncle Ben's_Digital",
"US_Walmart_Ben's 2H_0822_MZS_WMF_002",
"US_Walmart_Greenies_522_MZS_WAM _ 086",
"2020_2 months_MZS_Chewy _Full Year OLV _Shopper",
"US_SCB_Valentine_0122_MZS_SCB_035",
"BidManager_Campaign_DO_NOT_EDIT_11531879_1639153693610",
"BidManager_Campaign_DO_NOT_EDIT_13671165_1695308720317",
"US_MMS_Movie Night Targeted_US_mw_na_OTH_Brand_olv_awareness_bid-adr_0323_ma3 mms 033 Amazon-Q4-PBU-10/2-10/31-MA3",
"US_MMS_Movie Night Untargeted_US_mw_na_OTH_Brand_olv_awareness_bid-adr_0323_ma3 mms 033 Amazon-Q4-PBU-10/2-10/31-MA3",
"ca_grn_greenies-dog-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1023_en-o-42evk",
"ca_grf_greenies-cat-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-490xp",
"ca_tem_temptations-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-3wlc5-use",
"CA_Royal Canin_Stay Curious Cat Pinterest EN_0122_O-31M7W",
"CA_Royal Canin_Stay Curious Cat Pinterest FR_0122_O-31M7W",
"CA_Royal Canin_SOL EN_0422_O-35XT4-R3",
"CA_Royal Canin_SOL FR_0422_O-35XT4-R3",
"CA_Royal Canin_SOL EN Bonus Spend_0422_O-35XT4-R3",
"CA_Royal Canin_SOL Pinterest EN Inventory_0423_O-43270 - L*R23*REQ*14",
"CA_Royal Canin_SOL Pinterest FR Inventory_0423_O-43270 - L*R23*REQ*14",
"CA_Royal Canin_SOL Pinterest EN Cash IO_0423_O-3XNW4 - L*R23*REQ*14",
"CA_Royal Canin_SOL Pinterest FR Cash IO_0423_O-3XNW4 - L*R23*REQ*14",
"CA_Royal Canin_SOL Pinterest EN Flight 2_0923_O-3XNW4",
"CA_Royal Canin_SOL Pinterest FR Flight 2_0923_O-3XNW4",
"US_Royal Canin_Cat Health_0820_Mars_Q3_8/17 - 10/31_Pinterest_Awareness_Social",
"US_Royal Canin_Cat2Vet_0821_Mars_Q3_8/9 - 9/12_Pinterest_Brand Awareness",
"US_Royal Canin_Sol Kitten_0522_Mars_Q2/Q3_5/2-7/3_Pinterest_Reach_Social",
"Royal Canin_Q4_FY23_US_PBU Multifunction_Vet TTD_02102023_17122023",
"US_Royal Canin_RYC Royal Canin WVC_0223_VMG_Display",
"US_Royal Canin_RYC Royal Canin VMX_0123_VMG_Display",
"USE_US_Pedigree_Ipouch_0322_Mars_Q1/Q2_3/22 - 5/29_Pinterest_Conversion/Traffic_Social",
"US_American Heritage_American Heritage GM_0820_Mars_Q3_8/17 - 8/31_Pinterest_Awareness_Social_Ezine",
"US_American Heritage_American Heritage GM_0820_Mars_Q3_8/17 - 8/31_Pinterest_Traffic_Social_Ezine",
"US_American Heritage_American Heritage GM_0920_Mars_Q3_9/25 - 9/30_Pinterest_Traffic_Social_ E-Zine Flight 2",
"US_American Heritage_American Heritage GM_1020_Mars_Q4_10/21 - 11/8_Pinterest_Traffic_Social_",
"US_American Heritage_American Heritage GM_1120_Mars_Q4_11/9 - 11/30_Pinterest_Traffic_Social - Seasonal",
"US_American Heritage_American Heritage GM_1120_Mars_Q4_11/24 - 11/30_Pinterest_Traffic_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Traffic_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Awareness_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Traffic_Social - Seasonal",
"US_American Heritage_American Heritage GM_0221_Mars_Q1_2/9 - 3/31_Pinterest_Traffic_Social - Hot Cocoa",
"US_American Heritage_American Heritage GM_0221_Mars_Q1_2/9 - 2/14_Pinterest_Awareness_Social - Valentine's Day",
"US_American Heritage_American Heritage GM_0321_Mars_Q1_3/3 - 3/31_Pinterest_Awareness_Social - Recipe/FGBC",
"US_American Heritage_American Heritage GM_0421_Mars_Q2_4/5 - 5/9_Pinterest_Consideration_Social - Mothers Day",
"US_American Heritage_American Heritage GM_0421_Mars_Q2_4/5 - 5/9_Pinterest_Awareness_Social - Pancakes",
"US_American Heritage_American Heritage GM_0521_Mars_Q2_5/10 - 6/6_Pinterest_Awareness_Social - Cherry Smoothie",
"US_American Heritage_American Heritage GM_0521_Mars_Q2_5/10 - 6/27_Pinterest_Consideration_Social - Cherry Smoothie",
"US_American Heritage_American Heritage GM_0721_Mars_Q3_7/7 - 8/15_Pinterest_Consideration_Social - Tablet Bar",
"US_American Heritage_American Heritage GM_0721_Mars_Q3_7/1 - 9/26_Pinterest_Awareness_Social - Recipes",
"US_American Heritage_Valentine's Day_0222_Mars_Q1_2/10-2/14_Pinterest_Reach_Social_Valentines Day",
"Mars_FY21_Q1_US_3/9-3/28_NA_Ultra_Consideration_Pinterest_Social",
"US_Ultra_Core_0321_Mars_Q2_3/29 - 6/27_Pinterest_Consideration_Social - Flight 2",
"US_Ultra_Core_0621_Mars_Q3_6/28 - 9/26_Pinterest_Consideration_Social - Flight 3",
"US_Nutro_Core_0122_Mars_Q1_1/3 - 3/27_Pinterest_Traffic/Consideration_Social",
"US_Temptations_Purr-ee_0122_Mars_Q1_1/3 - 3/27_Pinterest_Traffic/Consideration_Promoted Pins",
"US_Cesar_Real Food_0122_Mars_Q1_1/3-5/29_Pinterest_(Traffic/Conversion)_Social",
"US_Snickers_NFL & Fantasy Football_0821",
"US_ICD_ECOSYSTEM_PN_NA_OTH_D2C_DISPLAY_CONVERSIONS_BID-ADR_0723_YAHOO-Q3-PBU-ECOSYSTEM-MA3",
"BidManager_Campaign_DO_NOT_EDIT_13671165_1695308720317",
"US_Nutro_0721_Mars_Q4_9/27 - 12/26_Reddit_Brand Awareness and Reach_Promoted Posts",
"ca_ryc_royal-canin-small-dog-breed_rc_na_dmm_brand_display_awareness_bid-nadr_0223_o-3t9x5-display-en",
"ca_ryc_royal-canin-small-dog-breed_rc_na_dmm_brand_display_awareness_bid-nadr_0223_o-3t9x5-display-fr",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46fet-display-fr",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46fet-native-display-fr",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46fet-display-en",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46fet-native-display-en",
"us_ryc_start-of-life_rc_na_sph_brand_social_conversions_bid-nadr_0423_puppy-q2-q3-4/3-7/23-ryc/rbd/014",
"us_ryc_start-of-life_rc_na_sph_brand_social_awareness_bid-nadr_0123_puppy-q1-1/9-3/31-ma3-rbd-014",
"us_ryc_start-of-life_rc_na_sph_brand_social_conversions_bid-nadr_0423_kitten-q2-q3-4/3-7/23-ryc/rbd/015",
"us_ryc_stay-curious_rc_na_cml_brand_social_awareness_bid-nadr_0823_q3/q4-8/1-10/24-bau/1pd/1pdlal-brand-lift-study-cell-2-bau/1pd",
"us_ryc_stay-curious_rc_na_cml_brand_social_awareness_bid-nadr_0823_q3/q4-8/1-10/24-bau-brand-lift-study-cell-1-bau",
"us_ryc_start-of-life_rc_na_sph_brand_social_awareness_bid-nadr_0123_kitten-q1-1/9-3/31_fb/ig_reach-ma3-rbd-015",
"us_ryc_start-of-life_rc_na_sph_brand_social_awareness_bid-nadr_0323_puppy-q1-3/2-3/15-ma3-rbd-014_fb/ig-feed-test-inventory-filter-selected-av",
"us_ryc_vet-tech_rc_na_oth_brand_social_awareness_bid-adr_1123_fb/ig-q4-11/16-to-12/24",
"us_ryc_vet_rc_na_sph_brand_social_traffic_bid-nadr_0723_vet-urinary-cat-q3-7/5-8/23",
"us_ryc_stay-curious_rc_na_cml_brand_social_awareness_bid-nadr_0723_cat-trailer-chicago-0723-q3/q4-7/31-8/20",
"us_ryc_vet_rc_na_oth_brand_social_traffic_bid-nadr_1023_vet-ce-amplification-q4-10/10-12/14",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2q",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq43",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46few",
"US_Dove_Large Promise GM_0722_Mars_Q3_7/18-9/25_Pinterest_Awareness_Native Targeting_NCS Study",
"US_Dove_Large Promise GM_0722_Mars_Q3_7/18-9/25_Pinterest_Awareness_Dove PBT_NCS Study",
"US_Dove_Large Promise GM_0922_Mars_Q3_9/1-9/25_Pinterest_Awareness_Native Targeting AV Coupon_NCS Study",
"US_Snickers_Seasonal GM_1021_Mars_Q4_10/6 - 10/31_Pinterest_Reach_Social",
"US_Snickers_Seasonal Halloween GM_1022_Mars_Q4_10/3-10/31_Pinterest_Reach_Social",
"US_Dove Ice Cream_Core GM_0620_Mars_Q2-Q3_6/22-6/30, 7/10 - 8/16_Pinterest_Awareness_Social",
"US_Dove / Galaxy_Seasonal GM_1120_Mars_Q4_11/9 - 12/20_Pinterest_Awareness_Social",
"US_Dove / Galaxy_Core GM_0321_Mars_Q1-Q2_3/1 - 4/4_Pinterest_Awareness_Social_Tastemade",
"us_ryc_vet-tech_rc_na_oth_brand_social_awareness_bid-adr_1123_fb/ig-q4-11/16-to-12/24-av",
"US_M&Ms_Album Art GM_0222_Mars_Q1_2/15-3/31_Reddit_Reach_Social_NCS Study",
" DNU - US_MMS_SUPERBOWL_MW_NA_CHO_BRAND_SOCIAL_REACH_BID-ADR_0_CAMPAIGN 3_123_REDDIT-Q1-SOCIAL-1/24-2/12-MA3",
"CA_Mars_Greenies Always On | Net Media IO: O-3K3M3  | Tech Fee IO: | _0922 | Retargeting Display English",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_YouTube Bumpers FR After a Meal Moment",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_YouTube Bumpers FR Dating Moment",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_Instream Refresh & Energize EN",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_Instream Refresh & Energize FR",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_Studio71 Premium YouTube EN",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_FourSquare  After A Meal EN",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_YouTube Bumpers FR Refresh & Energize on the Go Moment",
"CA_Mars_Excel Moments Campaign, Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_1122_YouTube Bumpers FR Styling Up Moment",
"CA_Mars_Whiskas Purr More | Net Media IO: O-3M188 | Tech Fee IO: | T_M22_WD_6 |_1022 |Youtube Skippable 15s",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_YouTube Bumpers FR",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_YouTube Bumpers EN",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_YouTube Non Skip 15s EN",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_Instream FR",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_YouTube Non Skip 15s FR",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_Bell Video PG",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_Studio71 Premium YouTube EN",
"CA_Mars_Mars Bar Taste Campaign, Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122_1122_Corus PG",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Scare (Affinity)",
"CA_Mars_Excel Post Secondary, Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_1222_Behavioral targeting English",
"CA_Mars_Excel Post Secondary, Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_1222_Behavioral targeting French",
"US_RYC_ROYAL-CANIN-YT_RC_NA_OTH_BRAND_YT_KITTEN AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"US_RYC_ROYAL-CANIN-YT_RC_NA_OTH_BRAND_YT_PUPPY AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_YT",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_YT",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_YT_Brand Lift Study #2",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_YT_Brand Lift Study #2",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_YT_Awareness",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_YT_Consideration",
"Royal Canin_Unique Abilities_YT Awareness",
"Royal Canin_Breed_YT Consideration",
"US_RYC_ROYAL-CANIN-YT_RC_NA_OTH_BRAND_YT_KITTEN AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"US_RYC_ROYAL-CANIN-OLV_RC_NA_OTH_BRAND_OLV_KITTEN AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"US_RYC_ROYAL-CANIN-YT_RC_NA_OTH_BRAND_CTV_KITTEN AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"US_RYC_ROYAL-CANIN-YT_RC_NA_OTH_BRAND_CTV_PUPPY AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"US_RYC_ROYAL-CANIN-YT_RC_NA_OTH_BRAND_YT_PUPPY AWARENESS_BID-ADR_0123_DV360-Q1-PBU-NA-MA3",
"US_RYC_ROYAL-CANIN-OLV_RC_NA_OTH_BRAND_OLV_PUPPY AWARENESS_BID-ADR_0223_DV360-Q1-PBU-NA-MA3",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_Display",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_Display",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_YT",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_Native",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_Native",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_OLV",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_CTV",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_YT",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_CTV",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_OLV",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_DOOH",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_DOOH",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Puppy PBU_YT_Brand Lift Study #2",
"Royal Canin_2023_US_Start of Life_04032023_07232023_Kitten PBU_YT_Brand Lift Study #2",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_HIU",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_Display",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_Native",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_YT_Awareness",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_YT_Consideration",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_OLV_Consideration",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_01082023_24102023_OLV_Awareness",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dml_brand_other_awareness_bid-adr_1023_o-4bqb-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dml_brand_other_awareness_bid-adr_1023_o-4bqb-fr",
"US_Royal Canin_Shopper TTD SOL Q1 _0123_RYC_RRD_Petco",
"US_Royal Canin_Shopper TTD SOL Q1 _0123_RYC_RRD_Petsmart",
"US_Royal Canin_Shopper TTD SOL Q1 _0123_RYC_RRD_Retailer-Agnostic",
"Royal Canin_2023_US_Shopper_04032023_07232023_TTD_Display",
"Royal Canin_2023_US_Shopper_04032023_07232023_TTD_HIU",
"Royal Canin_Q3_FY23_US_PBU Stay Curious_Shopper TTD_1082023_24102023",
"BidManager_Campaign_DO_NOT_EDIT_14029703_1703014519474",
"Royal Canin_2023_Q4_US_Multifunction_10022023_12152023_Vet Multi   ",
"US_Royal Canin_AKC Partnership Digital_0120",
"US_DoubleMint_Digital PBU OLV FY_0922_MA3_DMG",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_5 Gum_Digital Core Control PBU OLV FY_1022_MA3_GM5 ",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_Mediacom_Mars_Snickers Core_Q2'23_PG (SO-566557)_GAM",
"US_5 Gum_Digital Core Control PBU OLV FY_1022_MA3_GM5 ",
"US_5 Gum_Digital Core Control PBU OLV FY_1022_MA3_GM5 ",
"US_5 Gum_Digital Core Test PBU OLV FY_1022_MA3_GM5",
"US_Ice Cream_Dove Mother's Day_0422_MZS_ICE",
"US_Skittles BBL_Digital PBU OLV FY_0722_MA3_SKG ",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Yieldmo_AA_Dec 2023",
"Snickers_NFL AMA__MULTI_PG__Spotify_NA_Q1 2024-MA3-SNN-020",
"MWC_Snickers Protein_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-SNP-002",
"Dove_Valentine's Day_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-DOV-014",
"MWC_M&Ms_Super Bowl_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-MPB-004",
"MWC_Twix_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-TWX-028",
"us_org_orbit core_mw_na_spotify_brand_audio_awareness_bid-adr_0124_ttd-q1-pbu-1/1 to 1/31-MA3-ORB-019",
"us_org_orbit white_mw_na_spotify_brand_audio_awareness_bid-adr_0124_ttd-q1-pbu-1/4 to 3/31-MA3-ORB-020",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_NCS-1PD Audience",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_Arts-Crafts-Shoppers-Snack-Shoppers",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_PMP 18-34-Amazon Top Performing Audience",
'Copy of "US_MMS_SUPERBOWL_MW_NA_CHO_BRAND_SOCIAL_REACH_BID-ADR_0_CAMPAIGN 1_123_REDDIT-Q1-SOCIAL-1/24-2/12-MA3',
'Copy of "US_M&Ms_Album Art_0222_Mars_Q1_2/14-3/27_Reddit_Reach_Social',
"US_Extra Gum_MA3 Digital Extra Core_Cookieless_0921",
"US_Extra Gum_MA3 Digital Extra Core_Cookieless_0921",
"Mars_Snickers_Efficiency_TTD_SNK DCO_OLV_2021_TBD",
"BidManager_Campaign_DO_NOT_EDIT_14066855_1704383597214",
"BidManager_Campaign_DO_NOT_EDIT_9317678_1555104598826",
"US_Skittles BBL_Digital PBU OLV FY_0722_MA3_SKG ",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Yieldmo_AA_Dec 2023",
"Snickers_NFL AMA__MULTI_PG__Spotify_NA_Q1 2024-MA3-SNN-020",
"MWC_Snickers Protein_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-SNP-002",
"Dove_Valentine's Day_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-DOV-014",
"MWC_M&Ms_Super Bowl_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-MPB-004",
"MWC_Twix_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-TWX-028",
"us_org_orbit core_mw_na_spotify_brand_audio_awareness_bid-adr_0124_ttd-q1-pbu-1/1 to 1/31-MA3-ORB-019",
"us_org_orbit white_mw_na_spotify_brand_audio_awareness_bid-adr_0124_ttd-q1-pbu-1/4 to 3/31-MA3-ORB-020",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_NCS-1PD Audience",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_Arts-Crafts-Shoppers-Snack-Shoppers",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_PMP 18-34-Amazon Top Performing Audience",
"US_Nutro_Core_0121_Mars_Q1_1/4-3/28_Pinterest_Brand Awareness_Flight 1",
"US_Nutro_Core_0321_Mars_Q2_3/29-6/27_Pinterest_Brand Awareness_Flight 2",
"US_Nutro_Core_0621_Mars_Q3_6/28-9/26_Pinterest_Brand Awareness_Flight 3",
"US_Nutro_Core_0122_Mars_Q1_01/10 - 03/27_Pinterest_Brand Awareness_Non-GMO",
"US_Nutro_So Simple_0422_Mars_Q2_4/13-5/29_Pinterest_Reach_Social",
"US_Cesar_Wholesome Bowls_0321_Mars_Q2_3/29-5/30_Pinterest_Reach_Social_Wholesome Bowls Test",
"US_Cesar_Real Food_0122_Mars_Q1/Q2_1/24-5/29_Pinterest_Reach_Social",
"US_Pedigree_Adoption/Loyalty_0820_Mars_Q3_8/3 - 10/31_Pinterest_Awareness_Social",
"US_Pedigree_Rescue Doodles_0322_Mars_Q2/Q3_3/14 - 5/1_Pinterest_Awareness_Social",
"USE_ US_Pedigree_Ipouch_0322_Mars_Q2/Q3_3/22 - 5/29_Pinterest_Awareness_Social",
"US_Pedigree_Marrobites_0422_Mars_Q2_4/11 - 5/29_Pinterest_Awareness_Social",
"US_Skittles BBL_Digital PBU OLV FY_0722_MA3_SKG ",
"MWC_Twix_Core_OLV_MULTI_PG_GAM_Yieldmo_AA_Dec 2023",
"Snickers_NFL AMA__MULTI_PG__Spotify_NA_Q1 2024-MA3-SNN-020",
"MWC_Snickers Protein_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-SNP-002",
"Dove_Valentine's Day_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-DOV-014",
"MWC_M&Ms_Super Bowl_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-MPB-004",
"MWC_Twix_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q1 2024-MA3-TWX-028",
"us_org_orbit core_mw_na_spotify_brand_audio_awareness_bid-adr_0124_ttd-q1-pbu-1/1 to 1/31-MA3-ORB-019",
"us_org_orbit white_mw_na_spotify_brand_audio_awareness_bid-adr_0124_ttd-q1-pbu-1/4 to 3/31-MA3-ORB-020",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_NCS-1PD Audience",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_Arts-Crafts-Shoppers-Snack-Shoppers",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3_PMP 18-34-Amazon Top Performing Audience",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3-mms-038_NCS-1PD Audience",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3-mms-038_Arts-Crafts-Shoppers-Snack-Shoppers",
"us_mms_ibe_mw_na_cho_brand_olv_awareness_bidadr_0224_Amazon-q1-pbu-2/12-5/31-ma3-mms-038_PMP 18-34-Amazon Top Performing Audience",
"US_5 Gum_Digital Core AA PBU OLV FY_1022_MA3_GM5_PG",
"US_5 Gum_Digital Core PBU Pandora_Audio FY_1022_MA3_GM5",
"US_5 Gum_Digital Core_PBU Spotify Audio FY_1022_MA3_GM5_PG",
"US_DoubleMint_Digital Core AA PBU OLV FY_1022_MA3_DMG_PG",
"US_Extra Core Learfield OOH_Digital PBU OLV FY_1122_MA3_EXT",
"US_Extra Rideshare_Digital PBU OLV FY_0722_MA3_EXT ",
"US_M&M's Mini's_Digital PBU Audio FY_1122_MA3_MMI Programmatic Guaranteed",
"US_Orbit Core_Digital PBU DOOH FY_1222_MA3_ORB",
"US_Orbit Gum_Connatix AA PG_1022_MA3_ORB",
"US_Snickers Core Learfield_Digital PBU OLV FY_1122_MA3_SNI",
"BidManager_Campaign_DO_NOT_EDIT_9282802_1553539670510",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-youtube-skippable-en",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-youtube-non-skippable-en",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-youtube-non-skippable-fr",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-bumpers-fr",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-display-en",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-display-fr",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-native-display-fr",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-youtube-skippable-fr",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-bumpers-en",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-native-display-en",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-retargeting-en",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_olv_awareness_bid-nadr_0423_o-3xq2m-retargeting-fr",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-youtube-skippable-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-youtube-skippable-fr",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-youtube-non-skippable-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-youtube-non-skippable-fr",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-bumpers-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-bumpers-fr",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-display-fr",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-display-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-native-display-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-retargeting-en",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-retargeting-fr",
"ca_ryc_royal-canin-start-of-life-puppy_rc_na_dmm_brand_olv_awareness_bid-nadr_0423_o-3xq45-native-display-fr",
"ca_ryc_royal-canin-sol-pr-upport_rc_na_cml_brand_display_awareness_bid-adr_1023_o-466v5-native-display-en",
"ca_ryc_royal-canin-sol-pr-upport_rc_na_cml_brand_display_awareness_bid-adr_1023_o-466v5-native-display-fr",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46fet-retargeting-fr",
"ca_ryc_feline-care-nutrition_rc_na_oth_brand_display_awareness_bid-adr_0823_o-46fet-retargeting-en",
"us_ryc_vet-individualis_rc_na_oth_brand_social_awareness_bid-adr_1023_fb/ig-q4-10/24-12/22",
"us_ryc_vet-multifunction_rc_na_oth_brand_social_traffic_bid-adr_1123_fb/ig-q4-10/31-to-12/17",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_other_awareness_bid-adr_1023_o-4blg4-fr",
"ca_ryc_royal-canin-start-of-life-kitten_rc_na_cml_brand_other_awareness_bid-adr_1023_o-4blg4-en",
"2024_ca_tem_product-consideration_pn_na_cml_brand_display_engagement_bid-adr_0124_o-4lqtv_GMI_Exception",
"2024_ca_idg_product-consideration_pn_na_dmm_brand_display_engagement_bid-adr_0124_o-4lq1y_GMI_Exception",
"2024_ca_ict_product-consideration_pn_na_cml_brand_display_engagement_bid-adr_0124_o-4lqq7_GMI_Exception",
"2024_ca_grn_product-consideration_pn_na_dmm_brand_display_engagement_bid-adr_0124_o-4ls05_GMI_Exception",
"2024_ca_ces_product-consideration_pn_na_dmm_brand_display_engagement_bid-adr_0124_o-4lrt7_GMI_Exception",
"2024_ca_whi_product-consideration_pn_na_cml_brand_display_engagement_bid-adr_0124_o-4lrwg_GMI_Exception",
"2024_ca_she_product-consideration_pn_na_cml_brand_display_engagement_bid-adr_0124_o-4lrvq_GMI_Exception",
"2024_ca_dts_product-consideration_pn_na_dmm_brand_display_engagement_bid-adr_0124_o-4lrmy_GMI_Exception",
"US_American Heritage_American Heritage GM_0820_Mars_Q3_8/17 - 8/31_Pinterest_Awareness_Social_Ezine",
"US_American Heritage_American Heritage GM_0820_Mars_Q3_8/17 - 8/31_Pinterest_Traffic_Social_Ezine",
"US_American Heritage_American Heritage GM_0920_Mars_Q3_9/25 - 9/30_Pinterest_Traffic_Social_ E-Zine Flight 2",
"US_American Heritage_American Heritage GM_1020_Mars_Q4_10/21 - 11/8_Pinterest_Traffic_Social_",
"US_American Heritage_American Heritage GM_1120_Mars_Q4_11/9 - 11/30_Pinterest_Traffic_Social - Seasonal",
"US_American Heritage_American Heritage GM_1120_Mars_Q4_11/24 - 11/30_Pinterest_Traffic_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Traffic_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Awareness_Social - Holiday Ezine",
"US_American Heritage_American Heritage GM_1220_Mars_Q4_12/1 - 12/23_Pinterest_Traffic_Social - Seasonal",
"US_American Heritage_American Heritage GM_0221_Mars_Q1_2/9 - 3/31_Pinterest_Traffic_Social - Hot Cocoa",
"US_American Heritage_American Heritage GM_0221_Mars_Q1_2/9 - 2/14_Pinterest_Awareness_Social - Valentine's Day",
"US_American Heritage_American Heritage GM_0321_Mars_Q1_3/3 - 3/31_Pinterest_Awareness_Social - Recipe/FGBC",
"US_American Heritage_American Heritage GM_0421_Mars_Q2_4/5 - 5/9_Pinterest_Consideration_Social - Mothers Day",
"US_American Heritage_American Heritage GM_0421_Mars_Q2_4/5 - 5/9_Pinterest_Awareness_Social - Pancakes",
"US_American Heritage_American Heritage GM_0521_Mars_Q2_5/10 - 6/6_Pinterest_Awareness_Social - Cherry Smoothie",
"US_American Heritage_American Heritage GM_0521_Mars_Q2_5/10 - 6/27_Pinterest_Consideration_Social - Cherry Smoothie",
"US_American Heritage_American Heritage GM_0721_Mars_Q3_7/7 - 8/15_Pinterest_Consideration_Social - Tablet Bar",
"US_American Heritage_American Heritage GM_0721_Mars_Q3_7/1 - 9/26_Pinterest_Awareness_Social - Recipes",
"US_American Heritage_Valentine's Day_0222_Mars_Q1_2/10-2/14_Pinterest_Reach_Social_Valentines Day",
"BidManager_Campaign_DO_NOT_EDIT_9282802_1553539670510",
"us_euk_kiss-of-life_rc_na_oth_brand_social_awareness_bid-nadr_0523_kol-reputation-q2/q3-5/2-9/17",
"us_euk_kiss-of-life_rc_na_oth_brand_social_conversions_bid-nadr_0523_kol-support-q2/q3-5/2-9/17",
"Eukanuba_1Q23_United States_PBU Puppy Pro_06022023_03042023",
"US_Eukanuba_Brand Partnerships_0820",
"BidManager_Campaign_DO_NOT_EDIT_14077991_1704435069897",
"Q2 2024_Snickers_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q2 2024_ma3-sni-042",
"MWC_Twix_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q2 2024-MA3-TWX-028",
"MWC_Snickers Protein_Core_Audio_MULTI_PG_GAM_Spotify_NA_Q2 2024-ma3-snp-002",
"MWC_Twix_Core_Video_MULTI_PG_GAM_Yieldmo_AA_Mar 2024__Makegood",
"CA_Skittles_Target The Rainbow_EN_1121"]

# COMMAND ----------

RestOldCampaignList = ["de_exg_p4-6-refreshers_mw_eur_gum_brand_social_awareness_bid-adr_0424_tiktok-topfeed-r&f-#168506",
"DUMMY",
"TH_pdg_Collaborativeads Shopee Auction ProductCatalogSales May'24_pn_apac_SPH_Sales_social_conversions_bid-dda_0524_Remarketing_Pedigree+IAMS_2024042485",
"AU_MY DOG_Dog Taste Signals_AU-PN-20_0323",
"AU_MasterFoods_H&S Burst 4 with CPV_1021",
"AU_My Dog_Culinary Credentials_0622",
"TH_pdg_Collaborativeads Shopee Auction ProductCatalogSales May'24_pn_apac_SPH_Sales_social_conversions_bid-dda_0524_Prospecting_Pedigree+IAMS_2024042485",
"Greenies_202405_è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)_image_Reach_FB_5/20-6/8_$46,000(CPM$60)",
"Greenies_202405_è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)_image_Engagement_FB_5/20-6/8_$14,000(CPE$8)",
"th_Whiskas+Temps_whi_collaborativeads-shopee-auction-productcatalogsales May'24_pn_apac_cml_brand_social_conversions_bid-adr_0524_Retargeting_2024052725",
"th_Whiskas+Temps_whi_collaborativeads-shopee-auction-productcatalogsales May'24_pn_apac_cml_brand_social_conversions_bid-adr_0524_prospecting_2024052725",
"Greenies_202405_ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)_image_Reach_IG_5/28-6/6_$30,000(CPM$60)",
"Greenies_202405_ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)_image_Reach_FB_5/28-6/6_$30,000(CPM$60)",
"Temptation_202405_5æœˆKOL(é¤µé£Ÿäº’å‹•)_image_Reach_FB_5/23-6/1_$30,000(CPM$60)"]

# COMMAND ----------

campaignsToBeFiltered = NorthAmericaOldCampaignList + RestOldCampaignList
campaign_channel = fact_performance.select('gpd_campaign_id','channel_id').dropDuplicates()
dim_campaign23_campaigns_filtered = dim_campaign23.filter(~col('campaign_desc').isin(campaignsToBeFiltered)).join(campaign_channel, on = 'gpd_campaign_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left')


# COMMAND ----------

campaign_mismatch = dim_campaign23_campaigns_filtered.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
campaign_mismatch.display()

# COMMAND ----------

# dim_campaign23_campaigns_filtered.select('starting_month','naming_convention_flag').display()

# COMMAND ----------

# dim_campaign23.select('marketregion_code','country_desc','campaign_desc','naming_convention_flag').display()

dim_campaign23_campaigns_filtered.select('marketregion_code','country_desc','platform_desc','campaign_desc','naming_convention_flag').display()


# COMMAND ----------

if campaign_mismatch.count() > 0:
    for each in campaign_dict.keys():
        not_eval = []
        x = campaign_mismatch.select(col(each)).distinct().collect()  #.select(preproc_udf(col(each))).distinct().collect()
        unique_values = [r[0] for r in x]  # [preproc(r[0]) for r in x]

        for i in unique_values:
            if i not in campaign_dict[each]:
                not_eval.append(i)

        print(f"{each}: {not_eval}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mediabuy
# MAGIC

# COMMAND ----------

dim_mediabuy23 = dim_mediabuy23.withColumn(
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

mediabuy_mismatch = dim_mediabuy23.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
mediabuy_mismatch.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mediabuy filtered for which naming convention cannot be changed at source

# COMMAND ----------

NorthAmericaOldMediaList = ["Excel Gum_CPM_Social Multi_Facebook & Instagram_CPM_18-49_IT_National EN Feed + Instream + Stories",
"Excel Gum_CPM_Social Multi_Facebook & Instagram_CPM_18-49_IT_National EN Feed + Instream + Stories - Copy",
"EN - Cell 1- IG Only - June 15 to 29",
"EN - Cell 2- IG + FB - June 15 to 29",
"ca_ubn_canada_fr_na_mt_fbi_oth_cpm_bid-adr_demo-only_18-54_1pd_awareness_reach_newsfeed/stories_video_multiplatform_1x1_6s_pnm-na-1/13-3/26",
"ca_ubn_canada_en_na_mt_fbi_oth_cpm_bid-adr_demo-only_18-54_1pd_awareness_reach_newsfeed/stories_video_multiplatform_1x1_6s_pnm-na-1/13-3/26",
"ca_ubn_canada_en_na_mt_fbi_oth_cpm_bid-adr_demo-only_18-54_1pd_awareness_reach_newsfeed/stories_video_multiplatform_1x1_6s_pnm-na-4/3-5/28",
"ca_ubn_canada_fr_na_mt_fbi_oth_cpm_bid-adr_demo-only_18-54_1pd_awareness_reach_newsfeed/stories_video_multiplatform_1x1_6s_pnm-na-4/3-5/28",
"FR-1",
"Excel Gum_Reach_Social Multi_TikTok_CPM_18-54_IT_National EN",
"EN-1",
"Excel Gum_Reach_Social Multi_TikTok_CPM_18-24_IT_National FR",
"Excel Gum_Reach_Social Multi_TikTok_CPM_18-24_IT_National EN",
"Excel Gum_Traffic_Social Multi_TikTok_CPC_18-24_IT_National EN",
"CA_Excel Gum_2021 Excel FreshStart Influencer_EN Target",
"CA_Excel Gum_2021 Excel FreshStart_EN Target",
"CA_Excel Gum_2021 Excel Moments_EN Target",
"M&Ms_Reach_Social Multi_TikTok_CPM_18-49_IT_National FR",
"M&Ms_Reach_Social Multi_TikTok_CPM_18-49_IT_National  EN",
"M&Ms_Reach_Social Multi_TikTok_CPM_18-49_IT_National FR-1",
"M&Ms_Reach_Social Multi_TikTok_CPM_18-49_IT_National  EN-1",
"P1 - 15s - FR",
"Mars Bars EN-1",
"Mars Bars FR-1",
"Canada, All Genders, 18+",
"All Genders, 18+",
"Whiskas_Reach_Social Multi_Facebook & Instagram_CPM_25-54_CI_National_EN",
"Whiskas_Reach_Social Multi_Facebook & Instagram_CPM_25-54_CI_National_FR",
"DONT MAKE LIVE",
"DO NOT MAKE LIVE",
"2022-03-18 14:13 UTC | Ad group",
"Cesar FL2 Awareness EN 18-54",
"Cesar FL2 Awareness FR 18-54",
"Cesar FL2 Awareness EN 18-54 - Retarget",
"Cesar FL2 Awareness FR 18-54 Retarget",
"Copy 1 of Cesar FL2 Awareness EN 18-54",
"Nutro EN",
"2022-10-31 19:56 UTC | Ad group",
"Early owenership",
"Munchkins",
"Awareness Kitten",
"Awareness Health Concerns & Wellbeing",
"Awareness Adult Nutrition",
"2023-10-25 16:22 UTC | Ad group",
"CA_Iams Dog_EN_MB SK_122220",
"FY 21 - Iams Dog - Mar 4 - Apr 11 - Excl Que - Interests + Keywords - EN",
"A18-49 EN Cat Interests",
"A18-49 EN Dog Interests",
"Consideration Health Concerns & Wellbeing",
"Consideration Early Ownership",
"Consideration Munchkins",
"Consideration Adult Nutrition",
"Consideration Kitten",
"CA_Iams Cat_Couponing Pinterest Traffic_EN A18-49 Cat Interest",
"CA_Iams Cat_Couponing Pinterest Traffic_EN A18-49 Cat Interest - March 10 BROWSE Only",
"Cesar FL2 Digital Carting Awareness FR 18-54",
"Cesar FL2 Digital Carting Awareness FR 18-54 RT",
"Cesar FL2 Digital Carting Awareness EN 18-54",
"Cesar FL2 Digital Carting Awareness EN 18-54 - Retarget",
"CA_Nutro VNP_EN_A25-49_Dog Interest",
"Excel Gum_Reach_0x0_DV360_CPM_A25-54_Demo Only_InstreamVideo_After_Meal_6s_Spot_EN P270QST",
"Excel Gum_Reach_0x0_DV360_CPM_A25-54_Demo Only_InstreamVideo_Styling_Up_6s_Spot_EN P270Q6N",
"Excel Gum_Reach_0x0_DV360_CPM_18-34_Demo Only_InstreamVideo_Styling_Up_6s_Spot_FR P270SPS",
"Excel Gum_Reach_0x0_DV360_CPM_A25-54_Demo Only_FourSquare_After_Meal_6s_Spot_FR P270T6B",
"Mars_CTR_728x90_Google_CPM_18-45+_NA_Display_Demo Targeting ENG P261X8T",
"Mars_CTR_160x600_Google_CPM_18-45+_NA_Display_Demo Targeting ENG P261X8X",
"Mars_CTR_300x250_Google_CPM_18-45+_NA_Display_Demo Targeting ENG P261X8R",
"Mars_CTR_320x50_Google_CPM_18-45+_NA_Display_Demo Targeting ENG P261X9F",
"Mars_VCR_Video_DV360_CPM_A18+_NA_PMP Deal Targeting FRE V1 P262MCR",
"Skittles_VCR_0x0_DV360_CPM_18-44_A/BT_EN_InstreamVideo_Behav_3PSeg_15s_Game P25XG2T",
"Skittles_VCR_0x0_DV360_CPM_18-44_A/BT_EN_InstreamVideo_Behav_InMarket_15s_Game P25XGY3",
"Skittles_CTR_0x0_DV360_CPM_18-44_A/BT_EN_InstreamVideo_Behav_3PSeg_6s_x2 Follow P25L9X1",
"Skittles_VCR_0x0_DV360_CPM_18-44_A/BT_EN_InstreamVideo_Behav_3PSeg_15s_Scare P25XG2P",
"Skittles_CTR_0x0_DV360_CPM_18-44_A/BT_EN_Video_Posit_PMPDealTarget_6s_x3_Follow P25LJM3",
"Skittles_CTR_0x0_DV360_CPM_18-44_A/BT_EN_Video_Posit_PMPDealTarget_6s_x2_Scare P25XHF4",
"Skittles_CTR_0x0_DV360_CPM_18-44_A/BT_FR_Video_Posit_PMPDealTarget_6s_x3_Follow P25LKK3",
"Skittles_VCR_0x0_DV360_CPM_18-44_A/BT_FR_Video_Posit_PMPDealTarget_15s_x1_Game P25XHKP",
"Excel Gum_CTR_Video_DV360_CPM_A18+_NA_Geo targeting college campuses ENG V2 P26X2MF",
"Excel Gum_CTR_Video_DV360_CPM_A18+_NA_Geo targeting college campuses ENG V1 P264SRT",
"Excel Gum_CTR_Video_DV360_CPM_A18+_NA_Geo targeting college campuses FRE V1 P264VB2",
"US_Placement_2020",
"Iams Dog_CTR_Display_Google_CPM_Behavioral_A/BT_Net Media IO: O-3GN4H | Tech Fee IO: Pending | M22_IA_003 | Google Affinity",
"Iams Dog_CTR_Display_Google_CPM_Custom Affinity_A/BT_Net Media IO: O-3GN4H | Tech Fee IO: Pending | M22_IA_003 | Custom Affinity | Health Concerns & Wellbeing",
"Iams Dog_CTR_Digital Display_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3GN4H  | Triplelift_Everyday Tailored Nutrition_Chicken",
"Google Affinity",
"Custom Affinity",
"Custom Intent",
"3P",
"In-Market",
"categories",
"3P #2",
"Google In-Market",
"Life Event",
"Life Events",
"Retargeting",
"3P Behavioral",
"S71CA_GroupM_EssenceMediacomPBU_CustomShebaTargeting_Studio71_French_6s",
"S71CA_GroupM_EssenceMediacomPBU_CustomShebaTargeting_Studio71_English_6s",
"S71CA_GroupM_EssenceMediacomPBU_CustomShebaTargeting_Studio71_French_15s",
"S71CA_GroupM_EssenceMediacomPBU_CustomShebaTargeting_Studio71_English_15s",
"3P Behavioral FR",
"3P Behavioral #2",
"0 - YT CA Masthead ad CPM : Run of YouTube",
"ca_ces_core-vnp-2023_en_see_dv360_yt_auction_cpm_bid-nadr_life-event_na_awareness_views_non-skip_video_cross-device_1x1_15s_media-io-o-3y24s-detailed-demo_na",
"ca_ces_core-vnp-2023_fr_see_dv360_yt_auction_cpm_bid-nadr_life-event_na_awareness_views_non-skip_video_cross-device_1x1_15s_media-io-o-3y24s-detailed-demo_na",
"Audio Everywhere_Refresh Energize_EN ID: 1679428418211",
"Audio Everywhere_Working EN ID: 1679428418182",
"CA_Audio_Everywhere_CTA_P7 ID: 1687374503146",
"CA_Audio_Everywhere_CTA_P8 ID: 1687374503174",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-beauty-wellness_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-business-professionals_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-coffee-shop-regulars_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-food-dining_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-frequently-attends-live-events_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-frequently-visits-salons_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-grocery-store-shoppers_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-health-fitness-buffs_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-nightlife-enthusiasts_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-outdoor-enthusiasts_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-public-transit-users_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-inmarket-party-supplies_na",
"ca_ecg_p1-vnp-2023_en_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-inmarket-sports-fitness_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-beauty-wellness_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-business-professionals_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-coffee-shop-regulars_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-food-dining_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-frequently-attends-live-events_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-frequently-visits-salons_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-grocery-store-shoppers_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-health-fitness-buffs_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-nightlife-enthusiasts_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-outdoor-enthusiasts_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-affinity-public-transit-users_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-inmarket-party-supplies_na",
"ca_ecg_p1-vnp-2023_fr_see_dv36o_gog_ox_cpm_bid-adr_behavioural_na_2pd_awareness_views_instream_video_cross-device_0x0_15s_media-io-o-3smgp-google-inmarket-sports-fitness_na",
"ca_kbs_kindbar-digital-2023_en_see_dv360_gog_ox_cpm_bid-adr_int-behav_na_3pd_awareness_views_instream_video_cross-device_na_na_media-io-o-43thx-3p-behavioural-f-45+na",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Beauty (3P Segments)_step 3",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Game (3P Segments)_step 1",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Game (Affinity)_step 1",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Game (In Market)_step 1",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Game (In Market)_step 3",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Scare (3P Segments)_step 1",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Scare (3P Segments)_step 2",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Instream EN Scare (Affinity)_step 3",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Sharethrough PMP EN Beauty_step 3",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Teads PMP EN Scare_step 2",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Teads PMP EN Scare_step 3",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Teads PMP FR Game_step 1",
"CA_Mars_Skittles Target the Rainbow, Net Media IO: O-3N510, Tech Fee IO: O-3QH72, CPE: M22 SK 006_Skittles Taste the Rainbow_1122_1122_Teads PMP FR Game_step 3",
"Excel Gum_CPM_Non-Skippable Video_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 _S71CA_GroupM_MediacomPBU_Mars_ExcelGum_Q4_Moments_StylingUp_6s_",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Refresh & Energize - Google Affinity - Health & Fitness Buffs",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Refresh & Energize - Google Affinity - Public Transit Users",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Refresh & Energize - Google In Market - Sports & Fitness",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Styling Up - Google Affinity - Beauty & Wellness",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Styling Up - Google Affinity - Fashionistas",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Styling Up - Google In Market - Beauty & Personal Care",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Styling Up - Google In Market - Event Tickets",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_After A Meal - Foursquare Targeting",
"Excel Gum_Reach_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_After a Meal - Google Affinity - Food & Dining",
"Excel Gum_Reach_0x0_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Styling Up - Category Targeting",
"Excel Gum_Reach_0x0_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122_After a Meal - Category Targeting",
"Excel Gum_Reach_0x0_Google_CPM_Keyword_IT_Net Media IO: O-3M46L, Tech Fee IO: O-3PB04, CPE: M22 EB 011_Excel Moments_1122 Refresh & Energize - Keyword Targeting",
"Excel Gum_VCR_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_Category Targeting",
"Excel Gum_VCR_0x0_Google_CPM_Behavioral_A/BT_Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_Google In Market Post Secondary Education",
"Excel Gum_VCR_Instream_Google_CPM_A/BT_Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_Geo Targeting_EN",
"Excel Gum_VCR_Instream_Google_CPM_A/BT_Net Media IO: O-3PB0J Tech Fee IO: O-3PB0H CPE: M22 EB 022_Excel Post Secondary Geotagging_1122_Geo Targeting_FR",
"Mars_CPM_Non-Skippable Video_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM, CPE: M22 MR 004_Mars Bars Taste_1122 _S71CA_GroupM_MediacomPBU_Mars_MarsBars_Q4_Millennial_BIPOC_6s",
"Mars_CPM_Video_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM, CPE: M22 MR 004_Mars Bars Taste_1122 _#1 Bell Media (FR) - BFPD - ROC - All Platforms - :15 In-stream Video",
"Mars_CPM_Video_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM, CPE: M22 MR 004_Mars Bars Taste_1122 _Mars Bars - :15s Video",
"Mars_CTR_Display_Google_CPM_A18+_NA_Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM , CPE: M22 MR 004_Mars Bars Taste_1122 _18+ Demo",
"Mars_CTR_Instream_Google_CPM_18+_NA_Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM, CPE: M22 MR 004_Mars Bars Taste_1122_18+ Demo",
"Mars_VCR_Video_Google_CPM_Custom Segment Targeting_IT_Net Media IO: O-3N58J, Tech Fee IO: O-3PYJM, CPE: M22 MR 004_Mars Bars Taste_1122_Quebecor 6s Video",
"Mars - Skittles - Canada 151 - Traffic - English - Snap Ads - July 1 - Dam Fall",
"Mars - Skittles - Canada 151 - Reach - English - Promoted Story - Pre",
"Mars - Skittles - Halloween - Awareness - English - Snap Ads",
"Mars - Skittles - Canada 151 - Traffic - English - Snap Ads - July 1 - Tree",
"Mars - Skittles - Canada 151 - Reach - English - Promoted Story - Pre - No Frequency Cap",
"Mars - Skittles - Halloween - Awareness - English - Snap Ads - Heavy Up",
"Mars - Skittles - Canada 151 - Traffic - English - Snap Ads - Post - Best of Branch",
"Commercials Greenies_Reach_Social Multi_Snapchat_CPM_18-49_CA_National  EN",
"Website, Canada, All Genders, 18-49",
"Commercial Website, Canada, All Genders, 18-49",
"Lens Web View, Canada, All Genders, 18-49",
"Snap Ads Greenies_Reach_Social Multi_Snapchat_CPM_18-49_CA_National  EN",
"Unknown",
"Unallocated",
"2020-SOC - Oct 19 - Nov 29 - A18-44 - Parenting Interests + Keywords",
"2020-SOC - Oct 19 - Nov 29 - A18-44 - Healthy Food + Plantbased Interests + Keywords",
"Mars_FY2020_Q3_7/15-9/10_Seeds Of Change_Pinterest_A18-49_Homefeed/Search_Brand Lift_Smart Commerce_Broad Targeting",
"Mars_FY2020_Q3_7/30-9/18_Seeds Of Change_Pinterest_A18-49_Homefeed/Search_Brand Lift_Smart Commerce_Food Keywords/Interests",
"Seeds Of Change_Reach_Social Skippable Video_Pinterest_CPM_A18-54_BRDO_Mars_FY2022_Q3_8/1-9/30_Core_GM_Feed_",
"Seeds of Change_Reach_Social Skippable Video_Pinterest_CPM_18-54_BRDO_Mars_FY2022_Q4_12/14-12/25_Seeds of Change_Pinterest_18-54_Promoted Pins_NA",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Dog Keywords_Adoption/Loyalty",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Adoption Keywords_Adoption/Loyalty",
"Mars_FY20_Q2_7/13-8/23_NA_Pinterest_Nutro_Awareness_Social_A18-54 Dog Interests/Keywords",
"Nutro_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q3_6/28-9/26_Promoted Pin_Keywords/Interests",
"Nutro_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2022_Q1_01/10-03/27_Promoted Pin_Keywords/Interests",
"Nutro_Reach_Social Skippable Video_Pinterest_CPM_A18-54_BRDO_Mars _FY2022_Q2_4/13-6/26_So Simple_GM_Promoted Pin",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q2_3/29-5/30_Cesar GM_Promoted Pin_Keywords/Interests",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q2_3/29-5/30_Cesar GM_Promoted Pin_PBT",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-65+_DI_Mars_FY2022_Q1/Q2_1/10-6/26_Cesar GM_Promoted Pin_Keywords/Interests",
"Mars_FY2020_Q4_11/9 - 12/27_Temptations_Pinterest_A18-54_Promoted Pins_Holiday",
"Mars_FY20_Q3_US_7/15-9/27_NA_Pinterest_Greenies Dog_Awareness_Social_A18-54 Dog Interests/Keywords",
"Mars_FY20_Q4_US_10/1 - 12/27_Pinterest_Greenies Dog_Doggy IQ_Awareness_Social_A18-54 Dog Interests/Keywords",
"Greenies Cat_Reach_Social Skippable Video_Pinterest_CPM_18-54_CI_Mars_FY2022_Q1_1/3 - 2/27_Feline_Pinterest_Promoted Pins_Cat Keywords and Interests",
"Mars_FY2020_Q3_7/27-9/27_IAMS Dog_Pinterest_A18-54_Homefeed/Search_Dog Interests/Keywords",
"Mars_FY2020_Q4_9/28-12/27_IAMS Dog_Pinterest_A18-54_Homefeed/Search_Dog Interests/Keywords",
"Mars_FY2022_Q1_01/11-03/27_IAMS Dog_Pinterest_A18-54_Swim/Run_Interests/Keywords",
"Mars_FY2022_Q2_06/09-06/26_IAMS Dog_Pinterest_A18-54_Swim/Run_Interests/Keywords",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_A18-54 Cat Keywords/Interests_Homefeed/Search_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_A18-54 PBT Cat Dry Food Buyers_Homefeed/Search_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_A18-54 PBT Wet Cat Food Buyers_Homefeed/Search_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_A18-54 PBT Sheba Brand Buyers_Homefeed/Search_NCS Study",
"Sheba_Reach_Social Multi_Pinterest_CPM_18-54_CI_Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_A18-54_Promoted Pin_Keywords/Interests",
"Sheba_Reach_Social Multi_Pinterest_CPM_18-54_CI_Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_A18-54_Promoted Pin_PBT",
"2023-11-23 14:10 UTC | Ad group",
"M&M_FR_P13_Nov28-Dec25",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Dog Interests_Adoption/Loyalty",
"Pedigree Dry_Reach_Social Skippable Video_Pinterest_CPM_18-54+_DI_Mars_FY2022_Q2_4/11 - 6/26_Keywords_Promoted Pins_Marrobites",
"Mars_Reach_Social Multi_Pinterest_CPM_18-49_CHOI_EN",
"Mars_Reach_Social Multi_Pinterest_CPM_18-49_CHOI_FR",
"EN_AmazonWalmart_Audience",
"FR_AmazonWalmart_Audience",
"Mars_FY20_Q2_7/13-8/23_NA_Pinterest_Nutro_Awareness_Social_A18-54 Dog Interests/Keywords",
"Nutro_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q3_6/28-9/26_Promoted Pin_Keywords/Interests",
"Nutro_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2022_Q1_01/10-03/27_Promoted Pin_Keywords/Interests",
"Nutro_Reach_Social Skippable Video_Pinterest_CPM_A18-54_BRDO_Mars _FY2022_Q2_4/13-6/26_So Simple_GM_Promoted Pin",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q2_3/29-5/30_Cesar GM_Promoted Pin_Keywords/Interests",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q2_3/29-5/30_Cesar GM_Promoted Pin_PBT",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-65+_DI_Mars_FY2022_Q1/Q2_1/10-6/26_Cesar GM_Promoted Pin_Keywords/Interests",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Dog Interests_Adoption/Loyalty",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Dog Keywords_Adoption/Loyalty",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Adoption Keywords_Adoption/Loyalty",
"Pedigree Dry_Reach_Social Skippable Video_Pinterest_CPM_18-54+_DI_Mars_FY2022_Q2_4/11 - 6/26_Keywords_Promoted Pins_Marrobites",
"ca_grn_greenies-dog-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1023_en-o-42evk",
"ca_grf_greenies-cat-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-490xp",
"ca_tem_temptations-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-3wlc5-use",
"ca_ces_cesar-awareness-education_pn_na_dmm_brand_olv_awareness_bid-adr_1123_o-3wpe7-video-en",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time BBDO",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time Pin Edits",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Great Lengths",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Puppies Puppies",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time BBDO",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time Pin Edits",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Great Lengths",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Puppies Puppies",
"Mars_FY2021_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Great Lengths",
"Mars_FY2021_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Puppies' Puppies",
"Mars_FY2021_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Holding Breath",
"Mars_FY2022_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Multipacks",
"Mars_FY2022_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Smells Good",
"Mars_FY2022_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Worthy",
"Mars_FY2022_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Jealousy",
"Mars_FY2022_Q1_1/3-3/27_Cesar_Pinterest_Conversion_Promoted Pins_Static_Real Food_Flavor",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Conditional_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Laundry_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Halves_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Snaps_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Laundry_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Halves_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Conditional_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Laundry_NCS Study",
"Mars_FY2020_Q3_7/27-9/25_Sheba_Pinterest_Awareness_Homefeed/Search_6s_Snaps_NCS Study",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Static_Chicken Alfredo",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 14s_Chicken Alfredo",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 6s_Heres Your Tip",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 14s_Salmon in Creamy Sauce",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Static_Seafood Scampi",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Static_Salmon in Creamy Sauce",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 6s_Table for Two",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 14s_Seafood Scampi",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 6s_Waiter",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Static_Chicken Alfredo",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 14s_Chicken Alfredo",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 6s_Heres Your Tip",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 14s_Salmon in Creamy Sauce",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Static_Salmon in Creamy Sauce",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 6s_Table for Two",
"Mars_FY2021_Q2_3/29-5/30_Sheba_Pinterest_Awareness_Promoted Pins_Video 6s_Waiter",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness__NCS Study_Cat Owners_Promoted Pin_Video 6s_Camping",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness_NCS Study_Native + Interests_Promoted Pin_Video 6s_Camping",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness_NCS Study_Native + Keywords_Promoted Pin_Video 6s_Camping",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness_NCS Study_Cat Food Category Buyers_Promoted Pin_Video 15s_Camping",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness_NCS Study_Native + Interests_Promoted Pin_Video 15s_Camping",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness_NCS Study_Native + Keywords_Promoted Pin_Video 15s_Camping",
"FINAL - Mars_FY2020_Q3_8/3 - 9/27_Temptations_Pinterest_Awareness_NCS Study_Cat Owners_Promoted Pin_Video 15s_Camping",
"Mars_FY2020_Q4_11/9-12/27_Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Holiday",
"Mars_FY2020_Q4_11/9-12/27_Temptations_Pinterest_Awareness_Promoted Pin_Video 15s_Holiday",
"Mars_FY2021_Q1_3/1-3/28_Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Meaty Bites",
"Mars_FY2021_Q1_3/1-3/28_Temptations_Pinterest_Awareness_Promoted Pin_Video 15s_Meaty Bites",
"Mars_FY2021_Q1_3/1-3/28_Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Purr-ee",
"Mars_FY2021_Q2_3/29-6-27_Temptations_Pinterest_Awareness_Promoted Pin_Video 15s_Purr-ee",
"Mars_FY2021_Q2_3/29-6/27_Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Meaty Bites",
"Mars_FY2021_Q2_3/29-6/27_Temptations_Pinterest_Awareness_Promoted Pin_Video 15s_Meaty Bites",
"Mars_FY2021_Q2_3/29-6/27Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Purr-ee",
"Mars_FY2021_Q3_6/28 - 9/26_Temptations_Pinterest_Awareness_Promoted Pin_Video 15s_Purr-ee",
"Mars_FY2021_Q3_6/28 - 9/26_Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Meaty Bites",
"Mars_FY2021_Q3_6/28 - 9/26_Temptations_Pinterest_Awareness_Promoted Pin_Video 15s_Meaty Bites",
"Mars_FY2022_Q1_1/3 - 3/27_Temptations_Pinterest_Awareness_Promoted Pin_Video 6s_Purr-ee",
"Mars_FY2022_Q1_1/3 - 3/27_Temptations_Pinterest_Consideration_Promoted Pin_Video 6s_Hero_V2",
"Mars_FY2022_Q1_1/3 - 3/27_Temptations_Pinterest_Consideration_Promoted Pin_Video 6s_Hero_V1",
"Mars_FY2020_Q3_7/27-9/27_IAMS Dog_Pinterest_Awareness_Video_8s_Couch Potato",
"Mars_FY2020_Q3_7/27-9/27_IAMS Dog_Pinterest_Awareness_Video_9s_Conga Line",
"Mars_FY2020_Q3_7/27-9/27_IAMS Dog_Pinterest_Awareness_Video_8s_Couch Potato V2",
"Mars_FY2020_Q4_9/28-12/27_IAMS Dog_Pinterest_Awareness_Video_9s_Conga Line",
"Mars_FY2020_Q4_9/28-12/27_IAMS Dog_Pinterest_Awareness_Video_8s_Couch Potato V2",
"DNU*** Mars_FY2022_Q1_01/11-03/27_IAMS Dog_Pinterest_Awareness_Video_6s_Run",
"Mars_FY2022_Q1_01/11-03/27_IAMS Dog_Pinterest_Awareness_Video_6s_Swim",
"Mars_FY2022_Q1_6/13-6/26_IAMS Dog_Pinterest_Awareness_Video_6s_Swim",
"Mars_FY2020_Q3_7/15-9/10_Seeds of Change_Pinterest_Awareness_Broad Targeting_Smart Commerce_Video 6s_Cauliflower Bowl J",
"Mars_FY2020_Q3_7/15-9/10_Seeds of Change_Pinterest_Awareness_Broad Targeting_Smart Commerce_Video 6s_Super Food Bowl J",
"Mars_FY2020_Q3_7/15-9/10_Seeds of Change_Pinterest_Awareness_Broad Targeting_Smart Commerce_Video 6s_Super Food Bowl I",
"Mars_FY2020_Q3_7/15-9/10_Seeds of Change_Pinterest_Awareness_Broad Targeting_Smart Commerce_Video 6s_Cauliflower Bowl I",
"Mars_FY2020_Q3_7/15-9/10_Seeds of Change_Pinterest_Awareness_Broad Targeting_Smart Commerce_Video 6s_Super Food Bowl K",
"Mars_FY2020_Q3_7/15-9/10_Seeds of Change_Pinterest_Awareness_Broad Targeting_Smart Commerce_Video 6s_Cauliflower Bowl K",
"Mars_FY2020_Q3_7/30-9/10_Seeds of Change_Pinterest_Awareness_Food KW/Interests_Smart Commerce_Video 6s_Cauliflower Bowl J",
"Mars_FY2020_Q3_9/2-9/10_Seeds of Change_Pinterest_Awareness_Food KW/Interests_Smart Commerce_Video 6s_VidMob Cauliflower Bowl J",
"us_ubn_us_en_na_mt_fbi_oth_cpm_bid-adr_demo-only_18-54_1pd_awareness_reach_stories_video_multiplatform_1x1_6s_pnm-na-3/29-6/18",
"us_ubn_us_en_na_mt_fbi_oth_cpm_bid-adr_demo-only_18-54_1pd_awareness_reach_newsfeed/explore_video_multiplatform_1x1_6s_pnm-na-3/29-6/18",
"[1180121] Jan 24, 2024 - Jan 24, 2024 - FVTO - RD",
"us_mms_superbowl_en_na_rd_rdt_auction_cpm_bid-adr_demo-only_18-49_2pd_awareness_reach_feed_video_multiplatform_1x1_6s_csm-na-1/24-2/12",
"[1180121] Jan 24, 2024 - Jan 24, 2024 - FVTO - R2",
"M&Ms_Reach_Social Skippable Video_Reddit_CPM_A13-49_BRDO_Mars_FY2022_Q1_2/14-3/27_Album Art_GM_Feed_Branded Assets",
"us_mcb_caramel-cold-brew_en_na_rd_rdt_auction_cpm_bid-adr_interest_13-49_2pd_awareness_reach_feed_video_multiplatform_1x1_6s_csm-na-4/3-to-5/14-feed-coffee-desktop-mobile-web",
"[1180121] Jan 24, 2024 - Jan 24, 2024 - FVTO - Mobile",
"M&Ms_Reach_Social Skippable Video_Reddit_CPM_A13-49_BRDO_Mars_FY2022_Q1_2/14-3/27_Album Art_GM_Feed_Branded Assets",
"Mars_FY20_Q2_7/13-8/23_NA_Pinterest_Nutro_Awareness_Social_A18-54 Dog Interests/Keywords",
"Nutro_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q3_6/28-9/26_Promoted Pin_Keywords/Interests",
"Nutro_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2022_Q1_01/10-03/27_Promoted Pin_Keywords/Interests",
"Nutro_Reach_Social Skippable Video_Pinterest_CPM_A18-54_BRDO_Mars _FY2022_Q2_4/13-6/26_So Simple_GM_Promoted Pin",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q2_3/29-5/30_Cesar GM_Promoted Pin_Keywords/Interests",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-54_DI_Mars_FY2021_Q2_3/29-5/30_Cesar GM_Promoted Pin_PBT",
"Cesar_CPM_Social Skippable Video_Pinterest_CPM_18-65+_DI_Mars_FY2022_Q1/Q2_1/10-6/26_Cesar GM_Promoted Pin_Keywords/Interests",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Dog Interests_Adoption/Loyalty",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Dog Keywords_Adoption/Loyalty",
"Mars_FY20_Q3_8/3-10/31_Pinterest_Pedigree_Awareness_Social_A18-54 Adoption Keywords_Adoption/Loyalty",
"Pedigree Dry_Reach_Social Skippable Video_Pinterest_CPM_18-54+_DI_Mars_FY2022_Q2_4/11 - 6/26_Keywords_Promoted Pins_Marrobites",
"ca_grn_greenies-dog-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1023_en-o-42evk",
"ca_grf_greenies-cat-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-490xp",
"ca_tem_temptations-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-3wlc5-use",
"ca_grn_greenies-dog-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1023_en-o-42evk",
"ca_grf_greenies-cat-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-490xp",
"ca_tem_temptations-always-on-amazon-awareness-education_pn_na_dmm_brand_video_awareness_bid-nadr_1223_en-o-3wlc5-use",
"2023_US_EN_Beat 4_Core_V1_MM_Beat4_Lentil-Change_Programmatic_15sec_16x9",
"P2846TT_MA3_DOV_012_OLV_YOUTUBE.COM_us_dov_demo_en_na_dv360_dv360_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1920x1080_6s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_NV_NA",
"P2BK76D_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_demo-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_6s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_NV_NA",
"P2BK76N_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_college-women-&-recent-grads-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_6s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_",
"P2BK76N_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_college-women-&-recent-grads-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_6s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_",
"P2BK76L_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_super-moms-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_6s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_NV_NA",
"P2BK76L_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_super-moms-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_6s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_NV_NA",
"P2BK76F_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_demo-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_15s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_NV_NA",
"P2BK76P_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_college-women-&-recent-grads-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_15s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served",
"P2BK76M_MA3_DOV_013_OLV_YOUTUBE.COM_us_dov_super-moms-mother's-day/core_en_na_dv360_yt_ox_cpm_bid-dda_behavioural_18-49_2pd_awareness_reach_preroll_video_multiplatform_1x1_15s_pnm-na-na_Fixed Placement_Behavioral_P18-49_1 x 1_Site-served_NV_NA",
"us_ryc_other_en_na_meta_fbi_auction_cpm_bid-adr_interest_25-54_2pd_awareness_reach_feed_video_multiplatform_na_na_1/18-to-8/25_interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_feed_video_multiplatform_na_na_na_pet-interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_feed_video_multiplatform_na_na_na_breed-interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_video_multiplatform_na_na_na_1pd-maxi",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_video_multiplatform_na_na_na_1pd-all",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_video_multiplatform_na_na_na_1pd-X-Small",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_video_multiplatform_na_na_na_1pd-mini",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-pet-interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-breed-interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_video_multiplatform_na_na_na_1pd-medium",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-all",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-mini",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-medium",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-maxi"]

# COMMAND ----------

RestOldMediaList = ["fr_she_laws-vnp-p4_fr_see_meta_fbi_auction_cpm_bid-dda_interest_ cgenx4154_2pd_awareness_reach_hybrid_video_cross-device_1x1_na_34715",
"pl_she_laws-non-vnp-p4_pl_do_meta_fbi_auction_cpc_bid-dda_interest_chvyo55_2pd_traffic_clicks_hybrid_video_cross-device_na_na_34218",
"pl_she_laws-non-vnp-p4_pl_do_meta_fbi_auction_cpc_bid-dda_interest_ clte2840_2pd_traffic_clicks_hybrid_video_cross-device_na_na_34218",
"pl_she_laws-non-vnp-p4_pl_do_meta_fbi_auction_cpc_bid-nadr_rtg_ vvlalrtg_2pd_traffic_clicks_hybrid_video_cross-device_na_na_34218",
"pl_she_laws-non-vnp-p4_pl_do_meta_fbi_auction_cpc_bid-dda_interest_cgenx4154_2pd_traffic_clicks_hybrid_video_cross-device_na_na_34218",
"fr_she_laws-promo-non-vnp-p4_fr_see_meta_fbi_auction_cpm_bid-dda_interest_chvyo55_2pd_awareness_reach_hybrid_static_cross-device_na_na_34710",
"fr_she_laws-promo-non-vnp-p4_fr_see_meta_fbi_auction_cpm_bid-dda_interest_clte2840_2pd_awareness_reach_hybrid_static_cross-device_na_na_34710",
"fr_she_laws-vnp-p4_fr_see_meta_fbi_auction_cpm_bid-dda_interest_ chvyo55_2pd_awareness_reach_hybrid_video_cross-device_1x1_na_34715",
"fr_she_laws-promo-non-vnp-p4_fr_see_meta_fbi_auction_cpm_bid-dda_interest_cgenx4154_2pd_awareness_reach_hybrid_static_cross-device_na_na_34710",
"fr_she_laws-non-vnp-p4_fr_do_meta_fbi_auction_cpc_bid-adr_interest_clte2840_2pd_traffic_clicks_hybrid_video_cross-device_1x1_10s_34715",
"fr_she_laws-non-vnp-p4_fr_do_meta_fbi_auction_cpc_bid-dda_lal_vvlalrtg_2pd_traffic_clicks_hybrid_video_cross-device_1x1_10s_34715",
"fr_she_laws-non-vnp-p4_fr_do_meta_fbi_auction_cpc_bid-adr_interest_chvyo55_2pd_traffic_clicks_hybrid_video_cross-device_1x1_10s_34715",
"fr_she_laws-vnp-p4_fr_see_meta_fbi_auction_cpm_bid-dda_interest_clte2840_2pd_awareness_reach_hybrid_video_cross-device_1x1_na_34715",
"fr_she_laws-non-vnp-p4_fr_do_meta_fbi_auction_cpc_bid-adr_interest_cgenx4154_2pd_traffic_clicks_hybrid_video_cross-device_1x1_10s_34715",
"pl_she_laws-vnp-p4_pl_see_meta_fbi_auction_cpm_bid-dda_interest_cgenx4154_2pd_awareness_reach_hybrid_video_cross-device_na_na_34216",
"pl_she_laws-vnp-p4_pl_see_meta_fbi_auction_cpm_bid-dda_interest_clte2840_2pd_awareness_reach_hybrid_video_cross-device_na_na_34216",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-breed-interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-mini",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-maxi",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_other_video_multiplatform_na_na_1pd-all",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-all",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_other_video_multiplatform_na_na_1pd-mini",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_other_video_multiplatform_na_na_breed-interest",
"us_ryc_other_en_na_meta_fbi_auction_cpm_bid-adr_interest_25-54_2pd_awareness_reach_feed_video_multiplatform_na_na_1/18-to-8/25_interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_other_video_multiplatform_na_na_1pd-X-Small",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_other_video_multiplatform_na_na_pet-interest",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-X-Small",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_other_video_multiplatform_na_na_1pd-maxi",
"us_ryc_other_en_na_meta_fbi_auction_cpm_bid-adr_lal_25-54_1pd_awareness_reach_feed_video_multiplatform_na_na_5/20-to-8/25_websitevisit/lal",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-1pd-medium",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_lal_18plus_1pd_traffic_clicks_other_video_multiplatform_na_na_1pd-medium",
"us_ryc_breed_en_na_meta_fbi_auction_cpc_bid-adr_interest_18plus_2pd_traffic_clicks_feed_static_multiplatform_na_na_na_carousel-pet-interest",
"pl_she_laws-non-vnp-p4_pl_do_tt_tik_auction_cpc_bid-dda_lal_ vvlalrtg_2pd_traffic_clicks_hybrid_video_cross-device_na_na_na_34218",
"pl_she_laws-non-vnp-p4_pl_do_tt_tik_auction_cpc_bid-dda_interest_ccmbaff_2pd_traffic_clicks_hybrid_video_cross-device_na_na_na_34218",
"New Sales ad set",
"25-44 All_RT_Fanspage engager",
"25-44 All_LAL_Fanspage engager",
"25-44 All_Competitor",
"25-44 All_Interest",
"25-44 All_RT_Fanspage engager",
"25-44 All_LAL_Fanspage engager",
"25-44 All_Interest",
"25-44 All_Competitor",
"fr_pdg_alien-p6_fr_na_meta_fbi_auction_cpm_bid-dda_interest_dcmbaff_2pd_awareness_reach_hybrid_video_cross-device_na_na_dheavyo50-dlightu50-32533-3freq",
"fr_pdg_alien-p6_fr_na_meta_fbi_auction_cpm_bid-dda_interest_dadop_2pd_awareness_reach_hybrid_video_cross-device_na_na_dog-adopters-32533-3freq",
"fr_pff_wbh-p6_fr_see_meta_fbi_auction_cpm_bid-dda_interest_clteheau50_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_34688",
"fr_pdg_dentastix_fr_think_meta_fbi_auction_cpc_bid-dda_interest_dconsc_2pd_traffic_clicks_hybrid_video_cross-device_1x1_15s_conscious_32351",
"fr_pdg_dentastix_fr_think_meta_fbi_auction_cpc_bid-dda_lalrtg_vvlalrtg_2pd_traffic_clicks_hybrid_video_cross-device_1x1_15s_na_32351",
"fr_pdg_dentastix_fr_think_meta_fbi_auction_cpc_bid-dda_interest_dlteu50_2pd_traffic_clicks_hybrid_video_cross-device_1x1_15s_millenial_32351",
"pl_whi_playfulness-p7_pl_think_meta_fbi_auction_cpc_bid-dda_interest_chvyo50_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_na_32529",
"fr_ces_cul-cred-p7_fr_see_meta_fbi_auction_cpm_bid-dda_interest_dprmweto50_2pd_awareness_reach_hybrid_video_cross-device_na_na_na_34126",
"18+ All_RT_Fanspage engager",
"fr_pdg_dentastix_fr_see_meta_fbi_auction_cpm_bid-dda_interest_dlteu50_2pd_awareness_reach_hybrid_video_cross-device_1x1_na_millenial-32351",
"fr_pdg_dentastix_fr_see_meta_fbi_auction_cpm_bid-dda_interest_dconsc_2pd_awareness_reach_hybrid_video_cross-device_1x1_na_conscious-32351",
"25-44 All_RT_Fanspage engager",
"25-44 All_Interest",
"25-44 All_LAL_Fanspage engager",
"25-44 All_Competitor",
"25-44 All_Competitor",
"25-44 All_Interest",
"18+ All_LAL_Fanspage engager",
"18+ All_Interest",
"25-44 All_LAL_Fanspage engager",
"25-44 All_RT_Fanspage engager",
"pl_pdg_multivitamins_pl_see_meta_fbi_auction_cpm_bid-dda_interest_daff_2pd_awareness_reach_hybrid_video_cross-device_na_na_non-supplements-buyers_32305",
"pl_pdg_multivitamins_pl_see_meta_fbi_auction_cpm_bid-dda_interest_devdlfemom_2pd_awareness_reach_hybrid_video_cross-device_na_na_oral-care_32305",
"pl_pff_wbh-p6_pl_see_meta_fbi_auction_cpm_bid-dda_interest_chvyheao50_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_34413",
"pl_pff_wbh-p6_pl_see_meta_fbi_auction_cpm_bid-dda_interest_clteheau50_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_34413",
"pl_pdg_dentastix_pl_think_meta_fbi_auction_cpc_bid-dda_interest_dconsc_2pd_traffic_clicks_hybrid_video_cross-device_1x1_6s_conscious_32156",
"pl_pdg_multivitamins_pl_see_meta_fbi_auction_cpm_bid-dda_interest_dconsc_2pd_awareness_reach_hybrid_video_cross-device_na_na_supplements-buyers_32305",
"pl_pff_total-5-p6_pl_do_meta_fbi_auction_cpc_bid-dda_rtg_vvrtg_2pd_traffic_clicks_hybrid_static_cross-device_1x1_36124",
"pl_pff_total-5-p6_pl_do_meta_fbi_auction_cpc_bid-dda_interest_chvyheao50_2pd_traffic_clicks_hybrid_static_cross-device_1x1_36124",
"pl_pff_total-5-p6_pl_do_meta_fbi_auction_cpc_bid-dda_interest_clteheau50_2pd_traffic_clicks_hybrid_static_cross-device_1x1_36124",
"pl_pdg_dentastix_pl_think_meta_fbi_auction_cpc_bid-dda_interest_dlteu50_2pd_traffic_clicks_hybrid_video_cross-device_1x1_6s_millenial_32156",
"pl_she_laws-vnp-p4_pl_see_meta_fbi_auction_cpm_bid-dda_interest_chvyo55_2pd_awareness_reach_hybrid_video_cross-device_na_na_34216",
"pl_she_laws-non-vnp-p4_pl_see_tt_tik_auction_cpv_bid-dda_interest_ccmbaff_2pd_awareness_views_hybrid_video_cross-device_na_na_na_34218",
"pl_she_laws-non-vnp-p4_pl_see_tt_tik_auction_cpm_bid-dda_interest_ccmbaff_2pd_awareness_reach_hybrid_video_cross-device_na_na_na_34218",
"fr_whi_playfulness-p7_fr_think_meta_fbi_auction_cpc_bid-dda_rtg_vvrtg_2pd_traffic_clicks_hybrid_video_cross-device_1x1_6s_na_33353",
"fr_dre_feeling_fr_do_meta_fbi_auction_cpc_bid-dda_interest_cnonbuytrt_2pd_traffic_clicks_hybrid_video_cross-device_na_na_ccnonbuytreats_33399",
"fr_whi_playfulness-p7_fr_see_meta_fbi_auction_cpm_bid-dda_interest_chvyo50_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_na_33353",
"fr_dre_feeling_fr_do_meta_fbi_auction_cpc_bid-dda_interest_chvyo50_2pd_traffic_clicks_hybrid_video_cross-device_na_na_cheavyo50_33399",
"fr_pff_wbh-p6_fr_see_meta_fbi_auction_cpm_bid-dda_interest_chvyheao50_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_34688",
"fr_dre_feeling_fr_do_meta_fbi_auction_cpc_bid-dda_interest_clight2840_2pd_traffic_clicks_hybrid_video_cross-device_na_na_clight_33399",
"fr_pff_wbh-p6_fr_see_meta_fbi_auction_cpm_bid-dda_lal_vvlal_2pd_awareness_reach_hybrid_video_cross-device_1x1_6s_34688",
"fr_whi_playfulness-p7_fr_think_meta_fbi_auction_cpc_bid-dda_lal_vvlal_2pd_traffic_clicks_hybrid_video_cross-device_1x1_6s_na_33353"]

# COMMAND ----------

MediabuyToBeFiltered = NorthAmericaOldMediaList + RestOldMediaList
media_channel = fact_performance.select('media_buy_id','channel_id').dropDuplicates()
dim_mediabuy23_mediabuy_filtered = dim_mediabuy23.filter(~col('media_buy_desc').isin(MediabuyToBeFiltered)).join(media_channel, on = 'media_buy_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left')

# COMMAND ----------

mediabuy_mismatch = dim_mediabuy23_mediabuy_filtered.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
mediabuy_mismatch.display()

# COMMAND ----------

# dim_mediabuy23.select('marketregion_code','country_desc','media_buy_desc','naming_convention_flag').display()
dim_mediabuy23_mediabuy_filtered.select('marketregion_code','country_desc','platform_desc','media_buy_desc','naming_convention_flag').display()

# COMMAND ----------

if mediabuy_mismatch.count() > 0:
    for each in mediaBuy_dict.keys():
        not_eval = []
        x = mediabuy_mismatch.select(col(each)).distinct().collect()
        unique_values = [r[0] for r in x]   # [preproc(r[0]) for r in x]

        for i in unique_values:
            if i not in mediaBuy_dict[each]:
                not_eval.append(i)

        print(f"{each}: {not_eval}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creative

# COMMAND ----------

dim_creative23 = dim_creative23.withColumn(
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

creative_mismatch = dim_creative23.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
creative_mismatch.display()

# COMMAND ----------

dim_creative23.select('marketregion_code','country_desc','creative_desc','naming_convention_flag').display()

# COMMAND ----------

df2 = fact_performance.select('creative_id','channel_id').dropDuplicates()
# just for a check
dim_creative23.join(df2, on = 'creative_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').filter(col('platform_desc').isNull()).display()

# .select("platform_desc").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### only these filterd platforms are showing in dashboard for content health

# COMMAND ----------

# only these filterd platforms are showing in dashboard for content health
dim_creative23_platform_filtered = dim_creative23.join(df2, on = 'creative_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').filter(col('platform_Desc').isin(['GOOGLE ADWORDS', 'DV360-YOUTUBE', 'FACEBOOK ADS', 'PINTEREST ADS', 'SNAPCHAT', 'SNAPCHAT ADS', 'TIKTOK ADS', 'REDDIT ADS']))

# dim_creative23_platform_filtered.select('marketregion_code','country_desc','platform_desc','creative_desc','naming_convention_flag').display()

# SNAPCHAT ADS
# DV360-YOUTUBE
# REDDIT ADS
# GOOGLE ADWORDS
# FACEBOOK ADS
# TIKTOK ADS
# PINTEREST ADS

# COMMAND ----------

dim_creative23_platform_filtered.select('platform_desc').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creative filtered for which naming convention cannot be changed at source

# COMMAND ----------

NorthAmericaOldCreativeList = ["Feed Clean 15s 9x16",
"Feed Clean 6s 9x16",
"Awareness | Nutroâ„¢ Feed Clean",
"Awareness | Nutroâ„¢ Feed Clean",
"Awareness | IAMSâ„¢ Healthy Digestion",
"Awareness | IAMSâ„¢ Healthy Weight",
"Awareness | IAMSâ„¢ Healthy Aging",
"Awareness | IAMSâ„¢ Healthy Digestion",
"PH_PUPPY_PinterestStaticPin 1000x1500",
"IAMS Proactive Health Puppy Carousel",
"Awareness | PH_MINICHUNKS_PinterestStaticP",
"Awareness | IAMSâ„¢ Minichunks Lamb & Rice",
"Awareness | IAMSâ„¢ Minichunks Lamb & Rice",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"SOC - Static - EN - COTA",
"SOC - Static - EN - CTG",
"SOC - Video - EN - COTA",
"SOC - Video - EN - Brand Relaunch",
"SOC - Static - EN - COTA",
"SOC - Static - EN - CTG",
"SOC - Video - EN - COTA",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3.mp4",
"HoldingBreath_1080x1920.png",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3.mp4",
"Flavor_1080x1920.png",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3.mp4",
"RealJealousy_1080x1920.png",
"SmellGood__1080x1920.png",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"Real Ingredients",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3.mp4",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3.mp4",
"HoldingBreath_1080x1920.png",
"Flavor_1080x1920.png",
"RealJealousy_1080x1920.png",
"SmellGood__1080x1920.png",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAfr_v3",
"Idea Pin Dog Food So Good EN",
"Idea Pin Smells Good EN",
"Idea Pin Dog Food So Good EN",
"Idea Pin Smells Good EN",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAfr_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAfr_v3",
"Idea Pin Smells Good FR",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3",
"Greenies Holiday Lens",
"Temptations Lens",
"Cesar_Smells Good_FR_02440048_1000x1500_08_Rev1.jpg",
"Cesar_Flavour_FR_02440048_1000x1500_05_Rev1.jpg",
"Cesar_Smells Good_FR_02440048_1000x1500_08_Rev1.jpg",
"Cesar_Flavour_EN_02440048_1000x1500_01_Rev1.jpg",
"Cesar_Holding Breath_EN_02440048_1000x1500_02_Rev1.jpg",
"Cesar_Jealousy_EN_02440048_1000x1500_03_Rev1.jpg",
"Cesar_Smells Good_EN_02440048_1000x1500_04_Rev1.jpg",
"Cesar_Flavour_EN_02440048_1000x1500_01_Rev1.jpg",
"Cesar_Jealousy_EN_02440048_1000x1500_03_Rev1.jpg",
"Cesar_Smells Good_EN_02440048_1000x1500_04_Rev1.jpg",
"Awareness | Halloween with DENTASTIXâ„¢",
"Awareness | Halloween with DENTASTIXâ„¢",
"IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Conga Line 1x1 - 6s",
"IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Couch Potato Iap Dog Opt2 - 6s",
"IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Couch Potato Big Mood Opt 1 - 6s",
"NEW - IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Couch Potato Opt 2 1x1 - 6s",
"Iams Cat Static V7",
"Iams Cat Static V1",
"Iams Cat Static V2",
"Iams Cat Static V6",
"Iams Cat Static V9",
"Iams Cat Static V5",
"Iams Cat Static V4",
"Iams Cat Static V10",
"Iams Cat Static V3",
"Iams Cat Static V8",
"Iams Cat Static V11",
"PH_MINICHUNKS2_PinterestStaticPin 1000x1500",
"PH_MINICHUNKS_PinterestStaticPin 1000x1500",
"PH_HEALTHY WEIGHT_PinterestStaticPin 1000x1500",
"PH_HEALTHY AGING_PinterestStaticPin 1000x1500",
"AH_HealthyDigestion_PinterestStaticPin 1000x1500",
"IAMSâ„¢ Healthy Digestion Carousel",
"IAMSâ„¢ Healthy Weight",
"IAMSâ„¢ Healthy Aging",
"IAMSâ„¢ Healthy Digestion",
"PH_HEALTHY AGING_PinterestStaticPin 1000x1500NEW",
"PH_HEALTHY WEIGHT_PinterestStaticPin 1000x1500NEW",
"PH_MINICHUNKS_PinterestStaticPin 1000x1500NEW",
"PH_PUPPY_PinterestStaticPin 1000x1500NEW",
"PH_MINICHUNKS2_PinterestStaticPin 1000x1500NEW",
"AH_HealthyDigestion_PinterestStaticPin 1000x1500NEW",
"Consideration | IAMSâ„¢- Tailored Nutrition to k - Copy7",
"Consideration | IAMSâ„¢- Tailored Nutrition to k - Copy9",
"Consideration | IAMSâ„¢- Tailored Nutrition to k - Copy11",
"Consideration | IAMSâ„¢- Tailored Nutrition to k - Copy7",
"Consideration | IAMSâ„¢- Tailored Nutrition to k - copy9",
"Consideration | IAMSâ„¢- Tailored Nutrition to k  -Copy11",
"nutro-global-2022-kale-2x3-6s_CAen_v2_766117-1",
"nutro-global-2022-kale-9x16-6s_CAen_v2_766117-1",
"nutro-global-2022-kale-4x5-6s_CAen_v2_766117-1",
"nutro-global-2022-overarching-2x3-15s_CAen_v2_766117-1",
"nutro-global-2022-overarching-4x5-15s_CAen_v2_766117-1",
"nutro-global-2022-overarching-9x16-15s_CAen_v2_766117-1",
"P2F4NB9_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NB6_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NBB_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563N_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563N_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563N_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563N_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563Q_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563Q_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563Q_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P27563Q_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NB7_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NB8_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NBC_MA3_IDG_030_OLV_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HL7_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HL7_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HL9_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HL9_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271HL7_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271HL9_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271HLT_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLT_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLT_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271HLV_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLV_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLV_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271HL4_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271HL6_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271HLR_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLR_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLR_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271HLS_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLS_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P271HLS_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_con_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271HLL_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271HLM_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271JJ3_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271JJ4_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Cool Dog Moms_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271HLJ_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271HLK_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_1pd_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"MARS_GREENIES_BALLOON_015_CW_Spec",
"P271JJ1_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_15s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Greenies_Balloon_Social_16x9_09",
"P271JJ2_MA3_GRN_BLANK_OLV_YOUTUBE.COM_US_GRN_Globally Minded Dogs_en_NA_DV360_YT_OX_CPM_bid-adr_cs_hvc_2P/3P_Awareness_reach_trueview_video_Cross Device_1x1_6s_UNM-NA-NA_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-Climber_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-Climber_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JLX_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_equity-dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JLX_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_equity-dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"ia015_CLIMBER_sf14_2398fps_cut2354_20221205_mp4_youtube_stereo",
"IAMS_FOR_LIFE_2022_CLIMBER_FOR_LIFE_6SEC_16X9_2398FPS_TITLED_REV1_mp4",
"P2750RK_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RK_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RK_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2750RK_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_globally-minded-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2856FM_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_equity-dog-owners-+-health_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQF_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQF_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JLZ_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JLZ_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQB_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQB_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_celebrations-&-connections_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQG_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQG_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JM0_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JM0_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQC_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQC_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_dog-owners/dog-interest_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQH_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JM1_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F5JM1_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-seeker_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQD_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2F4NQD_MA3_IDG_032_Display Companion_YOUTUBE.COM_us_idg_plugged-in-&-sporty_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P270N8J_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_active-seniors_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P270N8J_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_active-seniors_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P270N8J_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_active-seniors_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P270N8N_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_do-it-all dog-moms_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P270N8N_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_do-it-all dog-moms_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P270N8N_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_do-it-all dog-moms_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Pedigree_Pup_lete_TINY_SNEAKERS_16x9_mp4_15s",
"Pedigree_Pup_lete_TINY_SNEAKERS_16x9_mp4_15s",
"Pedigree_Pup_lete_TINY_SNEAKERS_16x9_mp4_15s",
"P270N8Q_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_ush-base_en_na_dv360_yt_ox_dcpm_bid-adr_demo-only_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Demo_P18+_1 x 1_Site-served_NV_NA",
"P270N8Q_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_ush-base_en_na_dv360_yt_ox_dcpm_bid-adr_demo-only_hvc_2pd_awareness_reach_trueview_video_multiplatform_1x1_15s_unm-na-na_Run of Network_Demo_P18+_1 x 1_Site-served_NV_NA",
"P270N8R_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_ush-base_en_na_dv360_yt_ox_dcpm_bid-adr_demo-only_hvc_2pd_awareness_reach_bumpers_video_multiplatform_1x1_6s_unm-na-na_Run of Network_Demo_P18+_1 x 1_Site-served_NV_NA",
"P270N8R_MA3_PDG_028_OLV_YOUTUBE.COM_us_pdg_ush-base_en_na_dv360_yt_ox_dcpm_bid-adr_demo-only_hvc_2pd_awareness_reach_bumpers_video_multiplatform_1x1_6s_unm-na-na_Run of Network_Demo_P18+_1 x 1_Site-served_NV_NA",
"15_temptationsPurree_AEDMRTT180015_2021_02_17_1453_YouTube",
"P274HXW_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HXW_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"6_ temptationsPurree_AEDMRTT177006_2021_02_17_1453_YouTube",
"P274HXX_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HXX_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HXX_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D7NBS_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_Driven and Industrious_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D7NBT_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_Driven and Industrious_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D7NBT_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_Driven and Industrious_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-conquesting_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D7NBV_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_Home-a-holics_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D7NBW_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_Home-a-holics_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D7NBW_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_Home-a-holics_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HXY_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HXZ_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HXZ_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"15_temptationsPurree_AEDMRTT180015_2021_02_17_1453_YouTube",
"P274HY0_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_fun-fashion-and-fmily_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HY0_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_fun-fashion-and-fmily_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"6_ temptationsPurree_AEDMRTT177006_2021_02_17_1453_YouTube",
"P274HY1_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_fun-fashion-and-fmily_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HY1_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_fun-fashion-and-fmily_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P274HY1_MA3_TEM_041_OLV_YOUTUBE.COM_us_tem_fun-fashion-and-fmily_en_na_dv360_yt_ox_cpv_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_csm-pce-expansion_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P28QW4B_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_15s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4B_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_15s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4B_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_15s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28TJNG_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_30s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4D_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4D_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4D_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4D_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4D_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW4D_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_polished-pooch-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW46_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_15s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW46_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_15s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28TJNB_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_30s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW49_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW49_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW49_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW49_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW49_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P28QW49_MA3_NUA_011_OLV_YOUTUBE.COM_us_nut_socially-conscious-dog-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_other_video_multiplatform_1x1_6s_unm-na-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P29LL5Y_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P29LL5Y_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P29LL5W_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-",
"P29LL5W_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-",
"P29LL5X_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product_Run of Network_Behavioral_P18-34_1 x",
"P29LL5X_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_do-it-all-adventure-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product_Run of Network_Behavioral_P18-34_1 x",
"P29LL5V_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_",
"P29LL5V_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_",
"P29LL5S_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_N",
"P29LL5S_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_N",
"P29LL5T_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Site-",
"P29LL5T_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Site-",
"P2D7ZZQ_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_Driven and industrious_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-serv",
"P2J4C4G_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_driven-and-industrious_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_",
"P2D7ZZP_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_Driven and industrious_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-serve",
"P2D7ZZR_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_Driven and industrious_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Si",
"P2J4C4D_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_driven-and-industrious_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_S",
"P2J4C4F_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_driven-and-industrious_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product-(no-new)_Run of Network_Behavioral_P18-34",
"P29LL61_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-s",
"P29LL61_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-s",
"P2J4C48_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-expansion-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x",
"P29LL5Z_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-se",
"P29LL5Z_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-se",
"P29LL60_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1",
"P29LL60_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1",
"P2J4C46_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x",
"P2J4C47_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_freewheeling-young-families_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-product-(no-new)_Run of Network_Behavioral_P18",
"P29LL64_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served",
"P29LL64_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served",
"P2J4C4C_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-expansion-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_Si",
"P29LL62_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_",
"P29LL62_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_",
"P29LL63_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P29LL63_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P2J4C49_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_Sit",
"P2J4C4B_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_fun,-fashion-and-fmily_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-expansion-artist-cat-product-(no-new)_Run of Network_Behavioral_P18-34_1",
"P2D8003_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_Home-a-holics_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2J4C4K_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_home-a-holics_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_Site-serv",
"P2D8001_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_Home-a-holics_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2D8002_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_Home-a-holics_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Site-served",
"P2J4C4H_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_home-a-holics_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_Site-serve",
"P2J4C4J_MA3_TEM_042_OLV_YOUTUBE.COM_us_tem_home-a-holics_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-artist-cat-product-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_Si",
"P2F8MP5_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_1pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY4_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY4_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY5_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY5_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP0_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY6_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY6_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY7_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY7_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY8_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY8_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_cool-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP6_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_1pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXZ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXZ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY0_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY0_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY0_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP1_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY1_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY1_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY2_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY2_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY3_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHY3_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_deal-hunting-dog-moms-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFD_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFD_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFF_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFF_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFT_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFT_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFV_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFV_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFW_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFW_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_digital-first-dog-dads-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP7_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_1pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXL_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXL_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXM_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXM_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP2_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXN_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXN_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXP_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXP_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXQ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXQ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_globally-minded-men-exp_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP8_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_1pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXF_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXF_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXG_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXG_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP3_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXH_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXH_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXJ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXJ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXK_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHXK_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_leaders-of-the-pack-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP9_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_1pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFX_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFX_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-gamer_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFY_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFY_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_unm-na-home-office_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P2F8MP4_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_15s_pouch-good-stuff_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFZ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHFZ_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-nose_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHG0_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHG0_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-smile_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHG1_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28SHG1_MA3_PDG_034_OLV_YOUTUBE.COM_us_pdw_time-and-money-savers-cs_en_na_dv360_yt_ox_dcpm_bid-adr_con_hvc_2pd/3pd_awareness_reach_instream_video_cross-device_1x1_6s_unm-na-wag_Run of Network_Demo_P18-64_1 x 1_Standard_NV_NA",
"P28R188_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_traditional-seniors-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-conquesting-core_Run of Network_Behavioral_P13+_1 x 1_Site-se",
"P28R188_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_traditional-seniors-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-conquesting-core_Run of Network_Behavioral_P13+_1 x 1_Site-se",
"P28R18C_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_traditional-seniors-all-day_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-ser",
"P28R18C_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_traditional-seniors-all-day_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-ser",
"P28ZXB0_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-core_Run of Network_Behavioral_P13+_1 x 1_Site-serve",
"P28ZXB0_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-core_Run of Network_Behavioral_P13+_1 x 1_Site-serve",
"P28ZXB2_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-wrong-bowl_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-core_Run of Network_Behavioral_P13+_1 x 1_Site-served_NV",
"P28ZXB2_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-wrong-bowl_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-core_Run of Network_Behavioral_P13+_1 x 1_Site-served_NV",
"P28ZXBV_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-puppy-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-se",
"P28ZXBV_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-puppy-dogs_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-se",
"P28ZXBX_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-remember_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-serv",
"P28ZXBX_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_traditional-seniors-remember_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-serv",
"P28ZXB6_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-cross-sell-core_Run of Network_Behavioral_P13+_1 x 1_Site-serve",
"P28ZXB6_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-cross-sell-core_Run of Network_Behavioral_P13+_1 x 1_Site-serve",
"P28R189_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-core_Run of Network_Behavioral_P13+_1 x 1_Site-serv",
"P28R189_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-mouth-breather_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-core_Run of Network_Behavioral_P13+_1 x 1_Site-serv",
"P28R18D_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-puppy-dog-eyes_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Si",
"P28R18D_MA3_DTX_BLANK_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-puppy-dog-eyes_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Si",
"P28ZXB4_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-wrong-bowl_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-core_Run of Network_Behavioral_P13+_1 x 1_Site-served_NV_",
"P28ZXB4_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-wrong-bowl_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-core_Run of Network_Behavioral_P13+_1 x 1_Site-served_NV_",
"P28ZXBZ_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-remember_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-serve",
"P28ZXBZ_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-remember_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-serve",
"P28ZXC1_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-all-day_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-served",
"P28ZXC1_MA3_DTX_012_OLV_YOUTUBE.COM_us_den_fun-loving-dog-dads-all-day_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-cross-sell-chewy-chunx_Run of Network_Behavioral_P13+_1 x 1_Site-served",
"P2BG7BC_MA3_SHE_009_OLV_YOUTUBE.COM_us_she_cat-owners-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_conversions_reach_trueview_video_cross-device_1x1_15s_unm-na-combined-salmon_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2BG7BD_MA3_SHE_009_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_conversions_reach_trueview_video_cross-device_1x1_15s_unm-na-devonte-chicken_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2BG7BF_MA3_SHE_009_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_conversions_reach_trueview_video_cross-device_1x1_15s_unm-na-eleanor-salmon_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"Sheba-US-2023-KittenLaunch-Devont-Chicken-OLV-15s-16x9_2_2",
"Sheba-US-2023-KittenLaunch-Eleanor-Salmon-OLV-15s-16x9_2",
"P2BG7BG_MA3_SHE_009_OLV_YOUTUBE.COM_us_she_expecting-kitten-owner_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_conversions_reach_trueview_video_cross-device_1x1_15s_unm-na-combined-salmon_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2BG7BH_MA3_SHE_009_OLV_YOUTUBE.COM_us_she_expecting-kitten-owner_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_conversions_reach_trueview_video_cross-device_1x1_15s_unm-na-devonte-chicken_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2BG7BJ_MA3_SHE_009_OLV_YOUTUBE.COM_us_she_expecting-kitten-owner_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_conversions_reach_trueview_video_cross-device_1x1_15s_unm-na-eleanor-salmon_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"Sheba-US-2023-KittenLaunch-Devont-Chicken-OLV-15s-16x9_2_2",
"Sheba-US-2023-KittenLaunch-Eleanor-Salmon-OLV-15s-16x9_2",
"P2BKYXB_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_cat-owners/cat-interest-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-reach-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served",
"P2GD09P_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_cat-owners/cat-interest-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-reach-french-cat-(no-new)_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P2BKYX8_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_cat-owners/cat-interest-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-french-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P2BKYX9_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_cat-owners/cat-interest-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served_",
"P2GD09Y_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_cat-owners/cat-interest-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-main-mealfrench-cat-(no-new)_Run of Network_Behavioral_P18-34_1",
"P2GD0B6_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_cat-owners/cat-interest-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-main-mealfrench-cat-product-(no-new)_Run of Network_Behavioral_P",
"P2BKYWX_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-serve",
"P2GD09L_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting-main-meal-french-cat-(no-new)_Run of Network_Behavioral_P18-34",
"P2BKYWV_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-french-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Sit",
"P2BKYWW_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-served",
"P2GD09V_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-main-mealfrench-cat-(no-new)_Run of Network_Behavioral_P18-34_1",
"P2GD0B3_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-main-mealfrench-cat-product-(no-new)_Run of Network_Behavioral_",
"P2GD0B3_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_down-home-cool-cats_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-main-mealfrench-cat-product-(no-new)_Run of Network_Behavioral_",
"P2BKYX7_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_footloose-cat-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-cross-sell-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-serv",
"P2GD09N_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_footloose-cat-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-cross-sell-main-mealfrench-cat-(no-new)_Run of Network_Behavioral_P18-34",
"P2BKYX5_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_footloose-cat-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-french-cat-product_Run of Network_Behavioral_P18-34_1 x 1_Si",
"P2BKYX6_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_footloose-cat-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-serve",
"P2GD09X_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_footloose-cat-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-main-mealfrench-cat-(no-new)_Run of Network_Behavioral_P18-34_",
"P2GD0B5_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_footloose-cat-parents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-main-mealfrench-cat-product-(no-new)_Run of Network_Behavioral",
"P2BKYX4_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_fun-loving-grandparents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-cross-sell-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-se",
"P2GD09M_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_fun-loving-grandparents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-cross-sell-main-meal-french-cat-(no-new)_Run of Network_Behavioral_P18",
"P2BKYWY_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_fun-loving-grandparents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-french-cat-product_Run of Network_Behavioral_P18-34_1 x 1_",
"P2BKYWZ_MA3_TEM_BLANK_OLV_YOUTUBE.COM_us_tem_fun-loving-grandparents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-french-cat_Run of Network_Behavioral_P18-34_1 x 1_Site-ser",
"P2GD09W_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_fun-loving-grandparents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-main-mealfrench-cat-(no-new)_Run of Network_Behavioral_P18-3",
"P2GD0B4_MA3_TEM_043_OLV_YOUTUBE.COM_us_tem_fun-loving-grandparents_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-cross-sell-main-mealfrench-cat-product-(no-new)_Run of Network_Behavior",
"P2F5B37_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-wcw-owner_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2F5B38_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-wcw-no-owner_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2F5B39_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-wcw-combined_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2F5B4M_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-wcw-owner_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2F5B4N_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-wcw-no-owner_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2F5B4P_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach-wcw-combined_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2CMQ2Z_MA3_SHE_BLANK_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-reach_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Other_NA",
"P2F5B4Q_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_work-hard-play-hard-parents-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-wcw-owner_Run of Network_Behavioral_P18-34_1 x 1_Site-se",
"P2F5B4R_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_work-hard-play-hard-parents-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-wcw-no-owner_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P2F5B4S_MA3_SHE_011_OLV_YOUTUBE.COM_us_she_work-hard-play-hard-parents-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-conquesting-wcw-combined_Run of Network_Behavioral_P18-34_1 x 1_Site",
"P2CMQ31_MA3_SHE_BLANK_OLV_YOUTUBE.COM_us_she_work-hard-play-hard-parents-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-conquesting_Run of Network_Behavioral_P18-34_1 x 1_Site-served_Ot",
"P2D08L1_MA3_CES_BLANK_OLV_YOUTUBE.COM_us_ces_dog-office-workers-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_reach_reach_trueview_video_cross-device_1x1_15s_unm-na-rhino_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2D08L1_MA3_CES_BLANK_OLV_YOUTUBE.COM_us_ces_dog-office-workers-_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_reach_reach_trueview_video_cross-device_1x1_15s_unm-na-rhino_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2FP19W_MA3_GRN_BLANK_OLV_YOUTUBE.COM_us_grn_cool-dog-moms_en_na_ot_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-sb_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2FP19V_MA3_GRN_BLANK_OLV_YOUTUBE.COM_us_grn_globally-minded-dogs_en_na_ot_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-conquesting-sb_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2G5Z65_MA3_SHE_012_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_15s_unm-na-olv-reach_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2FSQYN_MA3_SHE_012_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_30splus_unm-na-olv-reach_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2FSQYN_MA3_SHE_012_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_30splus_unm-na-olv-reach_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2FSQYH_MA3_SHE_012_OLV_YOUTUBE.COM_us_she_cat-owners_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-reach_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2G7WT9_MA3_GRN_BLANK_OLV_YOUTUBE.COM_us_grn_cool-dog-moms_en_na_ot_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-a_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2G7XLW_MA3_GRN_BLANK_OLV_YOUTUBE.COM_us_grn_cool-dog-moms_en_na_ot_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na--olv-b_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2G7WT6_MA3_GRN_BLANK_OLV_YOUTUBE.COM_us_grn_globally-minded-dogs_en_na_ot_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na--olv-a_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2G7XLV_MA3_GRN_BLANK_OLV_YOUTUBE.COM_us_grn_globally-minded-dogs_en_na_ot_yt_ox_dcpm_bid-adr_behavioural_hvc_2pd/3pd_awareness_reach_trueview_video_cross-device_1x1_6s_unm-na-olv-b_Run of Network_Behavioral_P18-34_1 x 1_Site-served_NV_NA",
"P2HTXDH_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_audienceexpansion_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-advhealth_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXDH_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_audienceexpansion_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-advhealth_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXDJ_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_audienceexpansion_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-puppy_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXDK_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_audienceexpansion_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_1pd_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-core_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXR1_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_dogownerscustomaffinity_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-advhealth_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXR2_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_dogownerscustomaffinity_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-puppy_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXR3_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_dogownerscustomaffinity_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-core_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXDL_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_entertainmentcustomaffinity_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-advhealth_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXNC_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_entertainmentcustomaffinity_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-puppy_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXND_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_entertainmentcustomaffinity_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-core_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXNF_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_in-markethouseholdcleaning_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-advhealth_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXNG_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_in-markethouseholdcleaning_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-puppy_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"P2HTXQN_MA3_IDG_043_OLV_YOUTUBE.COM_us_idg_in-markethouseholdcleaning_en_na_dv360_yt_ox_dcpm_bid-adr_behavioural_hvc_2p/3p_awareness_reach_non-skip_video_cross-device_1x1_15s_unm-na-core_Run of Network_Behavioral_P18+_1 x 1_Site-served_NV_NA",
"Bens_Original_15sec_CknFriedRice_Family_Dinner_16x9",
"Bens_Original_15sec_BlkBeanTacoSalad_Family_Lunch_16x9",
"Bens_Original_15sec_Everyday_CknFriedRice_NonFamily_Dinner_16x9",
"Bens_Original_15sec_BlkBeanTacoSalad_NonFamily_Dinner_16x9",
"Bens_Original_15sec_BlkBeanTacoSalad_Family_Dinner_16x9",
"Bens_Original_15sec_BlkBeanTacoSalad_NonFamily_Dinner_16x9",
"Bens_Original_15sec_TomatoGarlic_Family_Lunch_16x9",
"Bens_Original_15sec_TomatoGarlic_Family_Dinner_16x9",
"Bens_Original_15sec_TomatoGarlic_NonFamily_Lunch_16x9",
"Bens_Original_15sec_TomatoGarlic_Family_Lunch_16x9",
"Bens_Original_15sec_StuffedPeppers_Family_Dinner_16x9",
"Bens_Original_15sec_Refuel_CknFriedRice_NonFamily_Dinner_16x9",
"Bens_Original_15sec_SpicyCknKebabs_Family_Dinner_16x9",
"Bens_Original_15sec_KoreanBeef_NonFamily_Dinner_16x9",
"Bens_Flatmates_guide_15s_25fps_16x9_760738_1_CAen_v1",
"Bens_HERO_family_guide_15s_16x9_760738_1_CAen_v1",
"Bens_Launch_15s_guide_16x9_760738_1_CAen_v1",
"Bens_Flatmates_15s_24LKFS_25fps_16x9_760738_1_CAfr_v2",
"Bens_HERO_family_15s_24LKFS_16x9_760738_1_CAfr_v2",
"Bens_Launch_15s_24LKFS_16x9_760738_1_CAfr_v2",
"Bens_Flatmates_guide_15s_25fps_16x9_760738_1_CAen_v1",
"Bens_HERO_family_guide_15s_16x9_760738_1_CAen_v1",
"Bens_Launch_15s_guide_16x9_760738_1_CAen_v1",
"Bens_Flatmates_15s_24LKFS_25fps_16x9_760738_1_CAfr_v2",
"Bens_HERO_family_15s_24LKFS_16x9_760738_1_CAfr_v2",
"Bens_Launch_15s_24LKFS_16x9_760738_1_CAfr_v2",
"SNICKERS ICE CREAM_6sec_AVOIDING SPOILERS_1920x1080",
"SNICKERS ICE CREAM_6sec_SHOW BINGE_1920x1080",
"SNICKERS ICE CREAM_15sec_AVOIDING SPOILERS_1920x1080",
"SNICKERS ICE CREAM_15sec_SHOW BINGE_1920x1080",
"MA3_2023_TAB_Channa_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_06s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Channa_YT_15s_Eng_BR_PROD_VID_1x1",
"MA3_2023_TAB_Madras_YT_15s_Eng_BR_PROD_VID_1x1",
"us_ubn_grandpa-everyday-dinner-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_meals-make-everyone-go_na_mikmak_7/17---10/8",
"us_ubn_workout-everyday-dinner-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_moment-to-enjoy_na_mikmak_7/17---10/8",
"us_ubn_grandpa-everyday-dinner-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_meals-make-everyone-go_na_mikmak_7-17---10-8",
"us_ubn_grandpa-everyday-dinner-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_fun-day-with-grandad_na_mikmak_7-17---10-8",
"us_ubn_workout-everyday-dinner-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_moment-to-enjoy_na_mikmak_7-17---10-8",
"us_ubn_workout-everyday-dinner-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_zen-stress-free-dinner_na_mikmak_7-17---10-8",
"us_ubn_grandpa-exercise-adventure-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_no-artificial-flavors_na_mikmak_7-17---10-8",
"us_ubn_grandpa-exercise-adventure-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_tasty-lunch-no-artificial-flavors_na_mikmak_7-17---10-8",
"us_ubn_workout-exercise-adventure-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_zero-artificial-flavors_na_mikmak_7-17---10-8",
"us_ubn_workout-exercise-adventure-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_healthy-way-to-refuel_na_mikmak_7-17---10-8",
"us_ubn_grandpa-flexitarian-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_max-delight_na_mikmak_7-17---10-8",
"us_ubn_grandpa-flexitarian-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_veggie-meal-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_workout-flexitarian-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_hectic-work-day_na_mikmak_7-17---10-8",
"us_ubn_workout-flexitarian-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_veggie-meal-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_grandpa-workday-lunch-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_lunch-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_grandpa-workday-lunch-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_lunch-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_workout-workday-lunch-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_tasty-lunch_na_mikmak_7-17---10-8",
"us_ubn_workout-workday-lunch-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_tasty-lunch_na_mikmak_7-17---10-8",
"us_ubn_grandpa-everyday-dinner-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_meals-make-everyone-go_na_mikmak_7/17---10/8",
"us_ubn_workout-everyday-dinner-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_moment-to-enjoy_na_mikmak_7/17---10/8",
"us_ubn_grandpa-everyday-dinner-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_meals-make-everyone-go_na_mikmak_7-17---10-8",
"us_ubn_grandpa-everyday-dinner-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_fun-day-with-grandad_na_mikmak_7-17---10-8",
"us_ubn_workout-everyday-dinner-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_moment-to-enjoy_na_mikmak_7-17---10-8",
"us_ubn_workout-everyday-dinner-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_zen-stress-free-dinner_na_mikmak_7-17---10-8",
"us_ubn_grandpa-exercise-adventure-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_no-artificial-flavors_na_mikmak_7-17---10-8",
"us_ubn_grandpa-exercise-adventure-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_tasty-lunch-no-artificial-flavors_na_mikmak_7-17---10-8",
"us_ubn_workout-exercise-adventure-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_zero-artificial-flavors_na_mikmak_7-17---10-8",
"us_ubn_workout-exercise-adventure-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_healthy-way-to-refuel_na_mikmak_7-17---10-8",
"us_ubn_grandpa-flexitarian-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_max-delight_na_mikmak_7-17---10-8",
"us_ubn_grandpa-flexitarian-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_veggie-meal-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_workout-flexitarian-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_hectic-work-day_na_mikmak_7-17---10-8",
"us_ubn_workout-flexitarian-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_veggie-meal-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_grandpa-workday-lunch-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_lunch-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_grandpa-workday-lunch-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_lunch-in-minutes_na_mikmak_7-17---10-8",
"us_ubn_workout-workday-lunch-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_10s_tasty-lunch_na_mikmak_7-17---10-8",
"us_ubn_workout-workday-lunch-non-family_en_dv360_yt_bid-adr_interest_v1_video_1x1_6s_tasty-lunch_na_mikmak_7-17---10-8",
"Greenies_commercial_WILL_006_1X1_BM_03",
"GREENIESTM Dental Treats Holiday Ad 1",
"Greenies Commercial",
"Greenies_SnapAds_WILL_006_1X1_BM_03",
"Greenies FY21 Holidays Lens V2",
"Mars - Skittles - Canada 151 - Reach - English - Promoted Story - Pre - V4",
"Mars - Skittles - Canada 151 - Reach - English - Promoted Story - Pre - V3",
"Mars - Skittles - Canada 151 - Reach - English - Promoted Story - Pre - V4",
"Mars - Skittles - Canada 151 - Traffic - English - Snap Ads - July 1 - Dam Fall",
"Mars - Skittles - Canada 151 - Traffic - English - Snap Ads - July 1 - Tree",
"Mars - Skittles - Canada 151 - Traffic - English - Snap Ads -",
"Mars - Skittles - Halloween - Awareness - English - Snap Ads - Paint Can",
"Mars - Skittles - Halloween - Awareness - English - Snap Ads - Zombie Hand Pointing at TO",
"Mars - Skittles - Halloween - Awareness - English - Snap Ads - Paint Can",
"Mars - Skittles - Halloween - Awareness - English - Snap Ads - Zombie Hand Pointing at TO",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_9x16_6s_chicken burrito wrap 2_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_na_chicken burrito wrap carousel_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_na_vegan rice salad carousel_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_9x16_6s_vegan rice salad 1_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_9x16_6s_chicken burrito wrap 1_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_9x16_6s_vegan rice salad 2_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_6s_vegan rice salad_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_6s_vegan rice salad 2_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_6s_korean beef rice salad_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_na_jasmine rth carousel_shop-now_social_3/26-6/18",
"US_UBN_Core_en_MT_FCB_bid-adr_demo-only_v1_standard_1x1_6s_sweet potato burrito_shop-now_social_3/26-6/18",
"EN_Video_Allie and Sam",
"EN_Video_Jasmine",
"EN_Video_Michelle",
"EN_Video_Jasmine",
"EN_Video_Michelle",
"EN_Video_Allie and Sam",
"US_Creative_2020",
"Awareness | M&M'S - we share joy",
"M&Ms Baking EN",
"M&Ms Baking FR",
"6s_IN FEED_CONGA_LINE_1080x1080",
"6s_STORIES_CONGA_LINE_9:16",
"6s_STORIES_Couch Potato Sofa Opt 1_Big_Mood_9:16",
"6s_STORIES_Couch Potato Sofa Opt 2_Lap_Dog_9:16",
"6s_IN FEED_COUCH POTATO SOFA OPT 2_1080x1080",
"Mars_FY2020_Q3_7/13-8/23_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Butterfly",
"Mars_FY2020_Q3_7/13-8/23_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Cherry Blossom",
"Mars_FY21_Q1_1/4 - 3/28_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Greet",
"Mars_FY21_Q1_1/4 - 3/28_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Leap",
"Mars_FY2021_Q1_1/4-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Pre-Transition",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition A_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition B_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition A_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition B_",
"Mars_FY21_Q2_3/29 - 6/27_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Greet",
"Mars_FY21_Q2_3/29 - 6/27_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Leap",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition A_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition B_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition A_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition B_",
"Mars_FY2021_Q2_4/13-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Wet Post-Transition A",
"Non GMO Just Disappointed",
"Non GMO Whatever",
"Non GMO Wallpaper Without Claim",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Just Disapoointed",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Whatever",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Wallpaper Without Claim",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO NMN V1",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO Disappointed V1",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO Disappointed V2",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Dry Key Ingredient",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Wet Simplified",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Wet Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Mixed Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Dry Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Mixed Puzzle",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Cesar Time",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Feeding Experience",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Human Food",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Real Ingredients",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 10s_Adoption/Loyalty_Belly Rubs",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 10s_Adoption/Loyalty_Jumping",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 10s_Adoption/Loyalty_Belly Rubs",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_Mail Squirrel",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_MZMM0206000H",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_MZMM0196000H",
"Awareness | A variety of PEDIGREE® recipes",
"Mars_FY2021_Q2_4/11-6/26_Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_Squirrel",
"5GUM_Backstage_06_16x9_corus",
"Advanced Health Native 1",
"Advanced Health Native 2",
"Advanced Health Native 3",
"Advanced Health Native 4",
"Advanced Health Native 5",
"AdvancedHealth6s",
"IFL 15s",
"IFL 6s",
"us_snn_draft-room-entry-video_en_di_csp_dir-nadr_contextual_v1_standard_1x1_1x1_nfl_na_mars_8-14-to-9-17",
"us_snn_fantasy-football-targeted-sponsorship-scoreboard-gametracker-logo_en_di_csp_dir-nadr_contextual_v1_standard_1x1_1x1_nfl_na_mars_8-1-to-12-24",
"Mars 2023 Royal Canin Core  (RYC/RRD/015/014)",
"Mars 2023 Royal Canin VET (RYC/RBD/020)",
"Feed Clean 15s 9x16",
"Feed Clean 6s 9x16",
"Awareness | Nutroâ„¢ Feed Clean",
"Awareness | Nutroâ„¢ Feed Clean",
"nutro-global-2022-kale-2x3-6s_CAen_v2_766117-1",
"nutro-global-2022-kale-9x16-6s_CAen_v2_766117-1",
"nutro-global-2022-kale-4x5-6s_CAen_v2_766117-1",
"nutro-global-2022-overarching-2x3-15s_CAen_v2_766117-1",
"nutro-global-2022-overarching-4x5-15s_CAen_v2_766117-1",
"nutro-global-2022-overarching-9x16-15s_CAen_v2_766117-1",
"Awareness | Shop Greenies at Amazon.ca",
"Awareness | Shop Greenies at Amazon.ca",
"Awareness | Halloween with DENTASTIXâ„¢",
"Awareness | Halloween with DENTASTIXâ„¢",
"Awareness | Mars Bar - You're Drooling",
"Awareness | Mars Bar - You're Drooling",
"Awareness | Make Life Sweeter with a Mars",
"Awareness | Make Life Sweeter with a Mars",
"Awareness | Barre Mars - C'est toi qui sal",
"Awareness | Rendez la vie plus agrÃ©able av",
"Awareness | Barre Mars - C'est toi qui sal",
"Awareness | Rendez la vie plus agrÃ©able av",
"Awareness | Mars Bar - You're Drooling",
"Awareness | Mars Bar - You're Drooling",
"Awareness | Make Life Sweeter with a Mars",
"Awareness | Make Life Sweeter with a Mars",
"Awareness | Barre Mars - C'est toi qui sal",
"Awareness | Rendez la vie plus agrÃ©able av",
"Awareness | Barre Mars - C'est toi qui sal",
"Awareness | Rendez la vie plus agrÃ©able av",
"Awareness | Holidays are better with M&M'S - 1",
"Awareness | Holidays are better with M&M'S - 2",
"Awareness | Holidays are better with M&M'S - 2",
"Awareness | Holidays are better with M&M'S - 1",
"Awareness | Make your holiday baking more - 1",
"Awareness | Make your holiday baking more - 1",
"Awareness | Make your holiday baking more - 2",
"Awareness | Make your holiday baking more - 2",
"Awareness | Rendez la cuisine des fÃªtes pl - 1",
"Awareness | Rendez la cuisine des fÃªtes pl - 1",
"Awareness | Rendez la cuisine des fÃªtes pl - 2",
"Awareness | Rendez la cuisine des fÃªtes pl - 2",
"Awareness | M&Mâ€™S. For all funkind.",
"Awareness | M&Mâ€™S. Du fun pour tout le mon",
"Awareness | M&Mâ€™S. For all funkind.",
"Awareness | M&Mâ€™S. Du fun pour tout le mon",
"Awareness | M&Mâ€™S. For all funkind.",
"Awareness | M&M'S. For All Funkind.",
"Awareness | M&M'S. Du Fun Pour Tout Le Mon",
"Awareness | M&M'S. For All Funkind.",
"Awareness | M&M'S. Du Fun Pour Tout Le Mon",
"Awareness | M&M'S - we share joy",
"M&Ms Baking EN",
"M&Ms Baking FR",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3.mp4",
"HoldingBreath_1080x1920.png",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3.mp4",
"Flavor_1080x1920.png",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3.mp4",
"RealJealousy_1080x1920.png",
"SmellGood__1080x1920.png",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3.mp4",
"Real Ingredients",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3.mp4",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3.mp4",
"HoldingBreath_1080x1920.png",
"Flavor_1080x1920.png",
"RealJealousy_1080x1920.png",
"SmellGood__1080x1920.png",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3",
"759212-1_16x9_-1DBFS_Cesar_Wholesome_Bowls_FeedingExperience_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAfr_v3",
"Idea Pin Dog Food So Good EN",
"Idea Pin Smells Good EN",
"Idea Pin Dog Food So Good EN",
"Idea Pin Smells Good EN",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAen_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAfr_v3",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_RealIngredients_6s_CAfr_v3",
"Idea Pin Smells Good FR",
"759212-1_9x16_-1DBFS_Cesar_Wholesome_Bowls_15s_CAen_v3",
"Cesar_Smells Good_FR_02440048_1000x1500_08_Rev1.jpg",
"Cesar_Flavour_FR_02440048_1000x1500_05_Rev1.jpg",
"Cesar_Smells Good_FR_02440048_1000x1500_08_Rev1.jpg",
"Cesar_Flavour_EN_02440048_1000x1500_01_Rev1.jpg",
"Cesar_Holding Breath_EN_02440048_1000x1500_02_Rev1.jpg",
"Cesar_Jealousy_EN_02440048_1000x1500_03_Rev1.jpg",
"Cesar_Smells Good_EN_02440048_1000x1500_04_Rev1.jpg",
"Cesar_Flavour_EN_02440048_1000x1500_01_Rev1.jpg",
"Cesar_Jealousy_EN_02440048_1000x1500_03_Rev1.jpg",
"Cesar_Smells Good_EN_02440048_1000x1500_04_Rev1.jpg",
"IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Conga Line 1x1 - 6s",
"IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Couch Potato Iap Dog Opt2 - 6s",
"IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Couch Potato Big Mood Opt 1 - 6s",
"NEW - IAMS - 2020 - Dec Q4 - Pinterest - Traffic - EN - Couch Potato Opt 2 1x1 - 6s",
"6s_IN FEED_CONGA_LINE_1080x1080",
"6s_STORIES_CONGA_LINE_9:16",
"6s_STORIES_Couch Potato Sofa Opt 1_Big_Mood_9:16",
"6s_STORIES_Couch Potato Sofa Opt 2_Lap_Dog_9:16",
"6s_IN FEED_COUCH POTATO SOFA OPT 2_1080x1080",
"Iams Cat Static V7",
"Iams Cat Static V1",
"Iams Cat Static V2",
"Iams Cat Static V6",
"Iams Cat Static V9",
"Iams Cat Static V5",
"Iams Cat Static V4",
"Iams Cat Static V10",
"Iams Cat Static V3",
"Iams Cat Static V8",
"Iams Cat Static V11",
"PH_PUPPY_PinterestStaticPin 1000x1500",
"PH_MINICHUNKS2_PinterestStaticPin 1000x1500",
"PH_MINICHUNKS_PinterestStaticPin 1000x1500",
"PH_HEALTHY WEIGHT_PinterestStaticPin 1000x1500",
"PH_HEALTHY AGING_PinterestStaticPin 1000x1500",
"AH_HealthyDigestion_PinterestStaticPin 1000x1500",
"Awareness | IAMSâ„¢ Healthy Digestion",
"Awareness | IAMSâ„¢ Healthy Weight",
"Awareness | IAMSâ„¢ Healthy Aging",
"Awareness | IAMSâ„¢ Healthy Digestion",
"PH_PUPPY_PinterestStaticPin 1000x1500",
"IAMS Proactive Health Puppy Carousel",
"Awareness | PH_MINICHUNKS_PinterestStaticP",
"Awareness | IAMSâ„¢ Minichunks Lamb & Rice",
"Awareness | IAMSâ„¢ Minichunks Lamb & Rice",
"IAMSâ„¢ Healthy Digestion Carousel",
"IAMSâ„¢ Healthy Weight",
"IAMSâ„¢ Healthy Aging",
"IAMSâ„¢ Healthy Digestion",
"PH_PUPPY_PinterestStaticPin 1000x1500",
"IAMS Proactive Health Puppy Carousel",
"Awareness | PH_MINICHUNKS_PinterestStaticP",
"Awareness | IAMSâ„¢ Minichunks Lamb & Rice",
"Awareness | IAMSâ„¢ Minichunks Lamb & Rice",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"Consideration | IAMSTM- Veterinarian Recommend",
"PH_HEALTHY AGING_PinterestStaticPin 1000x1500NEW",
"PH_HEALTHY WEIGHT_PinterestStaticPin 1000x1500NEW",
"PH_MINICHUNKS_PinterestStaticPin 1000x1500NEW",
"PH_PUPPY_PinterestStaticPin 1000x1500NEW",
"PH_MINICHUNKS2_PinterestStaticPin 1000x1500NEW",
"AH_HealthyDigestion_PinterestStaticPin 1000x1500NEW",
"Mars_FY2020_Q3_7/13-8/23_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Butterfly",
"Mars_FY2020_Q3_7/13-8/23_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Cherry Blossom",
"Mars_FY21_Q1_1/4 - 3/28_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Greet",
"Mars_FY21_Q1_1/4 - 3/28_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Leap",
"Mars_FY2021_Q1_1/4-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Pre-Transition",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition A_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition B_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition A_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition B_",
"Mars_FY21_Q2_3/29 - 6/27_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Greet",
"Mars_FY21_Q2_3/29 - 6/27_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Leap",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition A_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition B_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition A_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition B_",
"Mars_FY2021_Q2_4/13-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Wet Post-Transition A",
"Non GMO Just Disappointed",
"Non GMO Whatever",
"Non GMO Wallpaper Without Claim",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Just Disapoointed",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Whatever",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Wallpaper Without Claim",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO NMN V1",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO Disappointed V1",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO Disappointed V2",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Dry Key Ingredient",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Wet Simplified",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Wet Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Mixed Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Dry Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Mixed Puzzle",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time BBDO",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time Pin Edits",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Great Lengths",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Puppies Puppies",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time BBDO",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time Pin Edits",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Great Lengths",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Puppies Puppies",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Cesar Time",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Feeding Experience",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Human Food",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Real Ingredients",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 10s_Adoption/Loyalty_Belly Rubs",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 10s_Adoption/Loyalty_Jumping",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 10s_Adoption/Loyalty_Belly Rubs",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_Mail Squirrel",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_MZMM0206000H",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_MZMM0196000H",
"Awareness | A variety of PEDIGREE® recipes",
"Mars_FY2021_Q2_4/11-6/26_Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_Squirrel",
"SPOTIFY US ORBIT FEB SPOT 1 V2 10_02_.mp3",
"SPOTIFY US ORBIT WHITE DEC AE 1.mp3",
"MZSN8128000H_15_YouTube_MP4_1920x1080.mp4",
"mw-snickers-23rookiemistakesspicyfood-sn23000079-mzsn8187000h-16x9-6s.mp4",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_camping_v1_video_1920x1080_15s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_caramel-fold_v1_video_1920x1080_6s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_chocolate-pour_v1_video_1920x1080_6s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_cookie-break_v1_video_1920x1080_6s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_camping_v1_video_1920x1080_15s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_caramel-fold_v1_video_1920x1080_6s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_chocolate-pour_v1_video_1920x1080_6s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_cookie-break_v1_video_1920x1080_6s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_spotify-audio_v1_audio_0x0_30s_na_na_mars_na_na.mp3",
"us_mms_dco_en_tradedesk_ttd_bid-adr_behavioural_v1_video_1x1_6s_holiday_na_mars_1113-to-1225",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - RD - 3",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_cool-parent",
"us_mms_maya-lentil-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/11_learn-more_mars_beat-1",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - R2 - 1",
"us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_crossover",
"us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_twirl",
"Mars_FY2022_Q1_2/14-3/27_M&Ms Album Art_Reddit_Reach_Newsfeed_Static Image_Album Art Product",
" us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_accessory",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - RD - 2",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - Mobile- 1",
" us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_it-bag",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - R2 - 2",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_streaming-radio-heads",
"us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_accessory",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - Mobile - 3",
"us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_spinoff",
"Mars_FY2022_Q1_2/14-3/27_M&Ms Album Art_Reddit_Reach_Newsfeed_Video 15s_Album Art Product_ Copy V1",
"Mars_FY2022_Q1_2/14-3/27_M&Ms Album Art_Reddit_Reach_Newsfeed_Static Image_Album Art Product",
" us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_open-lentil",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - RD - 1",
"DNU_us_mms_core/superbowl_en_reddit_rdt_bid-adr_interest_13-49_bau-turtleneck_v1_video_1x1_6s_na_shop-now_mars_2/11-to-3/17_na",
"Mars_FY2022_Q1_2/14-3/27_M&Ms Album Art_Reddit_Reach_Newsfeed_Video 15s_Album Art Product_Copy V2",
"Mars_FY2022_Q1_2/14-3/27_M&Ms Album Art_Reddit_Reach_Newsfeed_Video 15s_Album Art Product_Copy V2",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_fashionista",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - R2 - 3",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_college-life/recent-grads",
"us_mms_maya-lentil-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/11_learn-more_mars_beat-1",
"us_mms_maya-lentil-change_en_rd_rdt_bid-adr_demo-only_v3_static_1x1_4x3_1/26-2/11_learn-more_mars_beat-1_v3",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_working-moms",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_core-(you)",
" us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_twirl",
"us_mms_maya-product-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_learn-more_mars_beat-3",
"Mars_FY2022_Q1_2/14-3/27_M&Ms Album Art_Reddit_Reach_Newsfeed_Video 15s_Album Art Product_ Copy V1",
" us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_spinoff",
"us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_open-lentil",
" us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_4/3-to-5/14_shop-now_mars_crossover",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_core-(america)",
"us_mms_maya-name-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_shop-now_mars_beat-2_big-city-parent",
"us_mms_maya-lentil-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/11_learn-more_mars_beat-1",
"us_mms_caramel-cold-brew_en_rd_rdt_bid-adr_demo-only_v1_static_4x3_na_4/3-to-5/14_shop-now_mars_crossover-yellow",
"us_mms_maya-lentil-change_en_rd_rdt_bid-adr_demo-only_v1_video_1x1_6s_1/24-2/12_learn-more_mars_beat-1",
"Jan 24, 2024 - Jan 24, 2024 - FVTO - Mobile - 2",
"us_mms_maya-lentil-change_en_rd_rdt_bid-adr_demo-only_v3_static_1x1_4x3_1/24-2/11_learn-more_mars_beat-1_v3",
"us_sns_snickers_en_tradedesk_ttd_bid-adr_na_18-34_spotify-snickers-shapes-eggs--_v1_audio_0x0_30s_na_na_mars_na_na.mp3",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_elena_v1_video_0x0_15s_-freestyles-_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_elizabeth_v1_video_0x0_15s_mashes_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_emily_v1_video_0x0_15s_-handcrafts_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_janneth_v1_video_0x0_15s_asmr_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_jason_v1_video_0x0_15s_-balloons-_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_liam_v1_video_0x0_15s_mishmashes_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_lynnea_v1_video_0x0_15s_arts-and-crafts_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nate_v1_video_0x0_15s_-cotton-candies_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nick-d-1_v1_video_0x0_15s_layers_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nick-d-2_v1_video_0x0_15s_blobs_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nick-t_v1_video_0x0_15s_-stacks_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_trinh_v1_video_0x0_15s_-fondues_na_mars_2-26-to-3-31_na.mp4",
"Mars_FY2020_Q3_7/13-8/23_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Butterfly",
"Mars_FY2020_Q3_7/13-8/23_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Cherry Blossom",
"Mars_FY21_Q1_1/4 - 3/28_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Greet",
"Mars_FY21_Q1_1/4 - 3/28_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Leap",
"Mars_FY2021_Q1_1/4-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Pre-Transition",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition A_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition B_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition A_",
"Mars_FY2021_Q1_2/1-3/28_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition B_",
"Mars_FY21_Q2_3/29 - 6/27_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Greet",
"Mars_FY21_Q2_3/29 - 6/27_Nutro_Pinterest_Awareness_Dog Interest_Video 6s_Leap",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition A_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Static_Post-Transition B_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition A_",
"Mars_FY2021_Q2_3/29-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Post-Transition B_",
"Mars_FY2021_Q2_4/13-6/27_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Wet Post-Transition A",
"Non GMO Just Disappointed",
"Non GMO Whatever",
"Non GMO Wallpaper Without Claim",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Just Disapoointed",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Whatever",
"Mars _FY2021_Q3_6/28 - 9/26_Nutro_Pinterest_Awareness_Promoted Pins_Video 6s_Non- GMO_Wallpaper Without Claim",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO NMN V1",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO Disappointed V1",
"Mars_FY2022_Q1_1/3 - 3/27_Nutro Core_Pinterest_Awareness_Promoted Pins_Video 6s_Non-GMO Disappointed V2",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Dry Key Ingredient",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Wet Simplified",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Wet Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Mixed Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Static Image_Dry Static",
"Mars_FY2022_Q1/Q2_4/13-6/26_Nutro_Pinterest_Awareness_Promoted Pin_Video 6s_Mixed Puzzle",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time BBDO",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time Pin Edits",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Great Lengths",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Puppies Puppies",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time BBDO",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Video 6s_Wholesome Bowls Time Pin Edits",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Great Lengths",
"Mars_FY2021_Q2_3/29-5/30_Cesar_Pinterest_Awareness_Promoted Pins_Static_Wholesome Bowls Puppies Puppies",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Cesar Time",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Feeding Experience",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Human Food",
"Mars_FY2022_Q1_1/3 - 3/27_Cesar_Pinterest_Awareness_Promoted Pin_Video 6s_Real Ingredients",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 10s_Adoption/Loyalty_Belly Rubs",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 10s_Adoption/Loyalty_Jumping",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Interests_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Dog Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 10s_Adoption/Loyalty_Belly Rubs",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards A",
"Mars_FY2020_Q3_8/6-10/31_Pedigree_Pinterest_Awareness_Adoption Keywords_Promoted Pin_Video 6s_Adoption/Loyalty_Rewards B",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_Mail Squirrel",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_MZMM0206000H",
"Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_MZMM0196000H",
"Awareness | A variety of PEDIGREE® recipes",
"Mars_FY2021_Q2_4/11-6/26_Pedigree_Pinterest_Awareness_Promoted Pins_Video 6s_Squirrel",
"SPOTIFY US ORBIT FEB SPOT 1 V2 10_02_.mp3",
"SPOTIFY US ORBIT WHITE DEC AE 1.mp3",
"MZSN8128000H_15_YouTube_MP4_1920x1080.mp4",
"mw-snickers-23rookiemistakesspicyfood-sn23000079-mzsn8187000h-16x9-6s.mp4",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_camping_v1_video_1920x1080_15s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_caramel-fold_v1_video_1920x1080_6s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_chocolate-pour_v1_video_1920x1080_6s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_dv360_yt_bid-adr_behavioural_13-34_cookie-break_v1_video_1920x1080_6s_na_na_mars_na_na",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_camping_v1_video_1920x1080_15s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_caramel-fold_v1_video_1920x1080_6s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_chocolate-pour_v1_video_1920x1080_6s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_cookie-break_v1_video_1920x1080_6s_na_na_mars_na_na.mp4",
"us_twx_twix-core-q1_24_en_tradedesk_ttd_bid-adr_behavioural_13-34_spotify-audio_v1_audio_0x0_30s_na_na_mars_na_na.mp3",
"us_mms_dco_en_tradedesk_ttd_bid-adr_behavioural_v1_video_1x1_6s_holiday_na_mars_1113-to-1225",
"us_sns_snickers_en_tradedesk_ttd_bid-adr_na_18-34_spotify-snickers-shapes-eggs--_v1_audio_0x0_30s_na_na_mars_na_na.mp3",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_elena_v1_video_0x0_15s_-freestyles-_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_elizabeth_v1_video_0x0_15s_mashes_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_emily_v1_video_0x0_15s_-handcrafts_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_janneth_v1_video_0x0_15s_asmr_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_jason_v1_video_0x0_15s_-balloons-_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_liam_v1_video_0x0_15s_mishmashes_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_lynnea_v1_video_0x0_15s_arts-and-crafts_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nate_v1_video_0x0_15s_-cotton-candies_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nick-d-1_v1_video_0x0_15s_layers_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nick-d-2_v1_video_0x0_15s_blobs_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_nick-t_v1_video_0x0_15s_-stacks_na_mars_2-26-to-3-31_na.mp4",
"us_stb_easter_en_tradedesk_ttd_bid-adr_na_na_trinh_v1_video_0x0_15s_-fondues_na_mars_2-26-to-3-31_na.mp4",
"MA3_2023_DOV_Spotify Audio_30s_Eng_BR_CNSD_DIS_0x0 V2.mp3",
"us_dsg_Mothers Day_en_tradedesk_ttd_bid-adr_demo_int_18-49_Super Moms Indulge_v1_video_0x0_6s_NA_na_mars_4-15 to 5-12_.mp4",
"us_mpmi_pb-minis_en_dv360_yt_bid-adr_demo-int_18-49_taste-bud_v1_video_1x1_6s_na_na_mars_3-18-to-5-12_na",
"us_mpmi_pb-minis_en_tradedesk_spf_bid-adr_demo-int_18-49_poppable_v1_audio_0x0_30s_na_na_mars_4-8-to-5-12_na.mp3",
"us_mpmi_pb-minis_en_tradedesk_spf_bid-adr_demo-int_18-49_poppable_v2_audio_0x0_30s_na_na_mars_4-8-to-5-12_na1.mp3",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_bits-red-pack_v1_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_bits-yellow-pack_v1_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_confetti-red-pack_v2_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_confetti-yellow-pack_v2_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_confetti_v1_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_downsize-red-pack_v1_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_downsize-yellow-pack_v1_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_hypnosis_v1_video_0x0_15s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_taste-bud-red-pack_v2_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_taste-bud-yellow-pack_v2_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_tradedesk_ttd_bid-adr_demo-int_18-49_taste-bud_v1_video_0x0_6s_na_na_mars_3-18-to-5-12_na.mp4",
"us_mpmi_pb-minis_en_wunderkind_ttd_bid-adr_demo-int_18-49_placeholder_v1_video_1x1_6s_na_na_mars_3-18-to-5-12_na",
"Tracker",
"Pedigree_MouthBreather_15s_16x9_HD_WEB_MASTER.mov",
"Wag_YAHOO_HD.mp4",
"Smile_YAHOO_HD.mp4",
"Nose_YAHOO_HD.mp4",
"Sheba_Bandaid_15s_16x9.mp4",
"mars-pedigree-pouch-goodstuff-15sec-16-9_YAHOO.mp4",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral",
"Anytime Bites OLV B 16x9.mp4",
"greeniesMakesHealthATreat_15_CAMOUFLAGE_OLV_16X9_2023_03_09_1018_1.mp4",
"Anytimebites Banner Ad A 1200x627",
"Anytimebites Banner Ad B 1200x627",
"P2LWYVW_Iams Dog Native",
"P2LWYW0_Iams Dog Native",
"P2LWYVX_Iams Dog Native",
"greeniesMakesHealthATreat_6_STASH_OLV_16X9_2023_03_09_1018_1 3.mp4",
"What Cats Want Split",
"P2LWYVZ_Iams Dog Native",
"P2LWYVY_Iams Dog Native",
"Advanced Health Native 4",
"PuppyHub Route 4",
"PetConnect Route1",
"Kitten Gen 1",
"HWD Gen 1",
"PetConnect Route3",
"Kitten Gen 4",
"PuppyHub Route 1",
"PuppyHub Route 3",
"PetConnect Route4",
"Kitten Gen 2",
"HWD Gen 2",
"PuppyHub Route 2",
"Kitten Gen 3",
"Advanced Health Native 1",
"Advanced Health Native 3",
"Advanced Health Native 2",
"Anytime Bites OLV A 16x9_2.mp4",
"HWD Gen 3",
"greeniesMakesHealthATreat_15_STASH_OLV_16X9_2023_03_09_1018_1 3.mp4",
"03579001_Mars_Native_1200x627",
"Yum Up Pedigree Pouch 3",
"P2LWYW1_Iams Dog Native",
"P2M3N1D_Smile_YAHOO_HD.mp4",
"P2M3N1C_Wag_YAHOO_HD.mp4",
"P2M3N1C_Nose_YAHOO_HD.mp4",
"P2M3N1F_Wag_YAHOO_HD.mp4",
"P2M3N1G_Wag_YAHOO_HD.mp4",
"P2M3N1C_Smile_YAHOO_HD.mp4",
"P2M3N1D_Nose_YAHOO_HD.mp4",
"P2M3N1D_Wag_YAHOO_HD.mp4",
"P2M3N1F_Nose_YAHOO_HD.mp4",
"P2M3N1F_Smile_YAHOO_HD.mp4",
"Iams Global AdvancedHealth-15s-16x9.mp4",
"Advanced Health Native 5",
"PetConnect Route2",
"HWD Gen 4",
"greeniesMakesHealthATreat_6_STASH_OLV_16X9_2023_03_09_1018_1 2.mp4",
"ShebaUS2023WCWDigitalStatic-300x50-catparent.png",
"What Cats Want V2",
"ShebaUS2023WCWDigitalStatic-300x50-split.png",
"ShebaUS2023WCWDigitalStatic-160x600-split.png",
"ShebaUS2023WCWDigitalStatic-728x90-catparent.png",
"ShebaUS2023WCWDigitalStatic-320x480-catparent.png",
"ShebaUS2023WCWDigitalStatic-160x600-catparent.png",
"ShebaUS2023WCWDigitalStatic-300x600-catparent.png",
"ShebaUS2023WCWDigitalStatic-970x250-split.png",
"greeniesMakesHealthATreat_6_STASH_OLV_16X9_2023_03_09_1018_1 1.mp4",
"greeniesMakesHealthATreat_6_STASH_OLV_16X9_2023_03_09_1018_1 3 1.mp4",
"What Cats Want V2 1",
"Sheba-US-2023-Hope2-CFC-OLV-Hawaii-15s-16x9_AMEND_V3_23976fps.mp4",
"Yum Up Pedigree Pouch 2",
"Yum Up Pedigree Pouch 1",
"Yum Up Pedigree Pouch 4",
"P2M3N1L_mars-pedigree-pouch-goodstuff-15sec-16-9_YAHOO.mp4",
"P2M3N3V_mars-pedigree-pouch-goodstuff-15sec-16-9_YAHOO.mp4",
"P2M3N1J_mars-pedigree-pouch-goodstuff-15sec-16-9_YAHOO.mp4",
"P2M3N1K_mars-pedigree-pouch-goodstuff-15sec-16-9_YAHOO.mp4",
"ShebaUS2023WCWDigitalStatic-320x480-split.png",
"ShebaUS2023WCWDigitalStatic-728x90-split.png",
"Sheba-US-2023-WCW-DigitalStatic-300x250_splitpsd.png",
"Sheba-US-2023-WCW-DigitalStatic-300x250psd.png",
"ShebaUS2023WCWDigitalStatic-320x50-catparent.png",
"ShebaUS2023WCWDigitalStatic-320x50-split.png",
"ShebaUS2023WCWDigitalStatic-300x600-split.png",
"ShebaUS2023WCWDigitalStatic-970x250-catparent.png",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability 2",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability 3",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral 3",
"What Cats Want Split 1",
"What Cats Want V2 2",
"What Cats Want Split 2",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability",
"HW Cat 1",
"HW Cat 4",
"HW Cat 2",
"HW Cat 3",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral 1",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-300x250.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-320x50.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-320x50.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-160x600.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-970x250.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-320x480.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-728x90.png",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-320x480.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability 1",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral 2",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-300x250.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-300x600.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-970x250.jpg",
"Anytime Bites OLV A 16x9_3.mp4",
"Pedigree_WrongBowl_6s_16x9_WEB_MASTER3.mov",
"Pedigree_MouthBreather_15s_16x9_HD_WEB_MASTER2.mov",
"Pedigree_MouthBreather_15s_16x9_HD_WEB_MASTER4.mov",
"Pedigree_MouthBreather_6s_16x9_HD_WEB_MASTER1.mov",
"Pedigree_MouthBreather_6s_16x9_HD_WEB_MASTER2.mov",
"Pedigree_WrongBowl_6s_16x9_WEB_MASTER2.mov",
"Pedigree_MouthBreather_15s_16x9_HD_WEB_MASTER1.mov",
"Pedigree_WrongBowl_6s_16x9_WEB_MASTER1.mov",
"Evergreen_A1",
"Evergreen_A2",
"Evergreen_B2",
"Evergreen_B3",
"Pedigree_MouthBreather_15s_16x9_HD_WEB_MASTER3.mov",
"Pedigree_MouthBreather_6s_16x9_HD_WEB_MASTER3.mov",
"Pedigree_MouthBreather_6s_16x9_HD_WEB_MASTER4.mov",
"Pedigree_WrongBowl_6s_16x9_WEB_MASTER4.mov",
"sheba-global-2023-hope-cfc-static-displaybanner-sustainability-300x50.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-160x600.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-300x50.jpg",
"sheba-global-2023-hope-cfc-static-displaybanner-restorecoral-300x600.jpg",
"Evergreen_B1",
"Greenies-US-2024-WBMW-Camouflage-6s-DV360-16x9-2398fps2.mp4",
"Greenies-US-2024-WBMW-Camouflage-15s-OLV-16x9-2398fps-DV3603.mov",
"Greenies-US-2024-WBMW-Camouflage-6s-DV360-16x9-2398fps1.mp4",
"Greenies-US-2024-WBMW-Stash-15s-Social-16x9-2398fps-DV3601.mov",
"Greenies-US-2024-WBMW-Stash-15s-Social-16x9-2398fps-DV3602.mov",
"Yieldmo-Hyperscroller-Unit_Static_Anytime Bites",
"Greenies-US-2024-WBMW-Stash-6s-DV360-16x9-2398fps1.mp4",
"Anytime Bites OLV B 16x9 3.mp4",
"Yieldmo-Window-Display-Unit_Static_Anytime Bites",
"Greenies-US-2024-WBMW-Camouflage-6s-DV360-16x9-2398fps3.mp4",
"Evergreen_A3",
"Iams_For_Life_3",
"Iams_For_Life_1",
"Iams_For_Life_4",
"Iams_For_Life_5",
"Iams_For_Life_6",
"Yieldmo-Hyperscroller-Unit_Static_Dental Treats",
"Anytime Bites OLV A 16x9 1.mp4",
"Greenies-US-2024-WBMW-Stash-6s-DV360-16x9-2398fps2.mp4",
"Anytime Bites OLV A 16x9 3.mp4",
"Greenies-US-2024-WBMW-Camouflage-15s-OLV-16x9-2398fps-DV3601.mov",
"Greenies-US-2024-WBMW-Stash-15s-Social-16x9-2398fps-DV3603.mov",
"Yieldmo-Window-Display-Unit_Static_Dental Treats",
"Anytime Bites OLV B 16x9 1.mp4",
"Anytime Bites OLV A 16x9 2.mp4",
"Anytime Bites OLV B 16x9 2.mp4",
"Greenies-US-2024-WBMW-Camouflage-15s-OLV-16x9-2398fps-DV3602.mov",
"Greenies-US-2024-WBMW-Stash-6s-DV360-16x9-2398fps3.mp4",
"Iams_For_Life_2",
"Digestive Supplements160x600HP.png",
"Skin and CoatDFO NativeHP",
"Calming SupplementsDFO NativeHP",
"Digestive Supplements320x50HP.png",
"Hip and Joint Supplement 30-ct320x50HP.png",
"Calming Supplements728x90HP.png",
"Digestive Supplements728x90HP.png",
"Greenies Digestion Supplement728x90HP.png",
"Calming Supplements300x600HP.png",
"Skin and Coat300x600HP.png",
"Greenies Digestion Supplement320x480HP.png",
"Hip and Joint Supplement 30 ctDFO NativeHP",
"Digestive SupplementsDFO NativeHP",
"Calming SupplementsDFO NativeCDSB",
"Greenies Digestion SupplementDFO NativeCDSB",
"Skin and Coat300x250HP.png",
"Greenies Digestion Supplement300x600HP.png",
"Digestive Supplements970x250HP.png",
"Skin and Coat970x250HP.png",
"Hip and Joint Supplement 30-ct320x480HP.png",
"Digestive Supplements300x250CDSB.gif",
"Calming Supplements300x600CDSB.png",
"Hip and Joint Supplement 30-ct300x600CDSB.png",
"Skin and Coat300x600CDSB.png",
"Hip and Joint Supplement 30-ct970x250CDSB.png",
"Skin and CoatDFO NativeCDSB",
"Skin and CoatDFO NativeGS1",
"Greenies Digestion SupplementDFO NativeGS1",
"Digestive Supplements300x250HP.gif",
"Greenies Digestion Supplement320x50HP.png",
"Skin and Coat728x90HP.png",
"Calming Supplements320x50CDSB.png",
"Digestive Supplements300x50CDSB.png",
"Greenies Digestion Supplement300x50CDSB.png",
"Hip and Joint Supplement 30-ct300x50CDSB.png",
"Digestive Supplements728x90CDSB.png",
"Greenies Digestion Supplement728x90CDSB.png",
"Hip and Joint Supplement 30-ct728x90CDSB.png",
"Digestive Supplements970x250CDSB.png",
"Skin and Coat320x480CDSB.png",
"Digestive Supplements300x250GS1.gif",
"Hip and Joint Supplement 30-ct728x90GS1.png",
"Greenies Digestion Supplement160x600CDSB.png",
"Skin and Coat970x250CDSB.png",
"Hip and Joint Supplement 30-ct320x480CDSB.png",
"Greenies Digestion Supplement300x250GS1.png",
"Skin and Coat320x50GS1.png",
"Digestive Supplements300x50GS1.png",
"Skin and Coat300x50GS1.png",
"Skin and Coat160x600GS1.png",
"Skin and Coat300x600GS1.png",
"Skin and Coat320x480GS1.png",
"Hip and Joint Supplement 30 ctDFO NativeCDSB",
"Digestive SupplementsDFO NativeGS1",
"Greenies Digestion Supplement300x250HP.png",
"Calming Supplements320x50HP.png",
"Calming Supplements300x50HP.png",
"Calming Supplements300x250HP.png",
"Skin and Coat320x50HP.png",
"Hip and Joint Supplement 30-ct728x90HP.png",
"Hip and Joint Supplement 30-ct970x250HP.png",
"Hip and Joint Supplement 30-ct300x250CDSB.png",
"Skin and Coat160x600CDSB.png",
"Greenies Digestion Supplement970x250CDSB.png",
"Hip and Joint Supplement 30-ct300x250GS1.png",
"Calming Supplements300x50GS1.png",
"Digestive Supplements160x600GS1.png",
"Greenies Digestion Supplement160x600GS1.png",
"Calming Supplements728x90GS1.png",
"Calming Supplements970x250GS1.png",
"Skin and Coat970x250GS1.png",
"Hip and Joint Supplement 30-ct320x480GS1.png",
"Hip and Joint Supplement 30-ct160x600GS1.png",
"Hip and Joint Supplement 30 ctDFO NativeGS1",
"Calming SupplementsDFO NativeGS1",
"Hip and Joint Supplement 30-ct300x250HP.png",
"Greenies Digestion Supplement300x50HP.png",
"Hip and Joint Supplement 30-ct300x50HP.png",
"Calming Supplements160x600HP.png",
"Greenies Digestion Supplement160x600HP.png",
"Hip and Joint Supplement 30-ct160x600HP.png",
"Calming Supplements970x250HP.png",
"Digestive Supplements320x480HP.png",
"Greenies Digestion Supplement300x250CDSB.png",
"Greenies Digestion Supplement320x50CDSB.png",
"Skin and Coat300x50CDSB.png",
"Calming Supplements160x600CDSB.png",
"Skin and Coat728x90CDSB.png",
"Digestive Supplements300x600CDSB.png",
"Calming Supplements320x480CDSB.png",
"Greenies Digestion Supplement320x480CDSB.png",
"Calming Supplements300x250GS1.png",
"Skin and Coat300x250GS1.png",
"Hip and Joint Supplement 30-ct320x50GS1.png",
"Hip and Joint Supplement 30-ct300x50GS1.png",
"Greenies Digestion Supplement300x600GS1.png",
"Hip and Joint Supplement 30-ct970x250GS1.png",
"Digestive Supplements320x480GS1.png",
"Digestive Supplements728x90GS1.png",
"Greenies Digestion Supplement728x90GS1.png",
"Digestive Supplements970x250GS1.png",
"Greenies Digestion SupplementDFO NativeHP",
"Digestive SupplementsDFO NativeCDSB",
"Digestive Supplements300x50HP.png",
"Skin and Coat300x50HP.png",
"Hip and Joint Supplement 30-ct300x600HP.png",
"Greenies Digestion Supplement970x250HP.png",
"Calming Supplements320x480HP.png",
"Skin and Coat320x480HP.png",
"Calming Supplements300x250CDSB.png",
"Skin and Coat300x250CDSB.png",
"Calming Supplements300x50CDSB.png",
"Digestive Supplements160x600CDSB.png",
"Greenies Digestion Supplement300x600CDSB.png",
"Skin and Coat160x600HP.png",
"Digestive Supplements300x600HP.png",
"Digestive Supplements320x50CDSB.png",
"Hip and Joint Supplement 30-ct320x50CDSB.png",
"Skin and Coat320x50CDSB.png",
"Hip and Joint Supplement 30-ct160x600CDSB.png",
"Calming Supplements728x90CDSB.png",
"Calming Supplements970x250CDSB.png",
"Digestive Supplements320x480CDSB.png",
"Digestive Supplements320x50GS1.png",
"Greenies Digestion Supplement300x50GS1.png",
"Calming Supplements300x600GS1.png",
"Hip and Joint Supplement 30-ct300x600GS1.png",
"Greenies Digestion Supplement970x250GS1.png",
"Calming Supplements320x480GS1.png",
"Greenies Digestion Supplement320x480GS1.png",
"Calming Supplements320x50GS1.png",
"Greenies Digestion Supplement320x50GS1.png",
"Calming Supplements160x600GS1.png",
"Skin and Coat728x90GS1.png",
"Digestive Supplements300x600GS1.png",
"us_nut_nutro-lghp-dv360-ctv-reach_en_dv360_dv360_bid-adr_na_na_overarching-15s-16x9_v1_video_0x0_15s_na_learn-more_mars_4-1-24-to-9-30-24_na.mp4",
"us_nut_nutro-lghp-dv360-ctv-reach_en_dv360_dv360_bid-adr_na_na_skin-and-coat-15s-16x9_v1_video_0x0_15s_na_learn-more_mars_4-1-24-to-9-30-24_na.mp4",
"Pedigree_MouthBreather_15s_16x9_HD_WEB_MASTER.mov",
"Pedigree_MouthBreather_6s_16x9_HD_WEB_MASTER.mov",
"Pedigree_WrongBowl_6s_16x9_WEB_MASTER.mov",
"Nutro Non_GMO Disappointed Dogs V1 Optimization 16x9",
"MA3_IDG_Large Breed_Conga_Eng_BR_CNSD_1x1_15s_NA mp4",
"ALL-DAY_760786-2_guide_16x9_6s_US_v3.mp4",
"Pedigree_Pup_lete_TINY_SNEAKERS_16x9_15s.mp4",
"Woe is Me 06s 16x9.mp4",
"us_grn_native_stream_static_en_ot_yah_bid_dda_behavioural_v1_standard_1x1_na_na_na_mars_na",
"V3-ImmuneHealth-ProductOnly-200x200",
"us_den_fun-loving-dog-dads-stream_en_na_ot_yah_ox_dcpm_bid-adr_behavioural_hvc_1pd_conversions_reach_native_carousel_multiplatform_1x1_1x1_unm-na-cross-sell",
"Wag_YAHOO_HD.mp4",
"us_nut_native-moments_en_ot_yah_bid-dda_behavioural_v1_carousel_1x1_1x1_na_na_mars_na",
"us_nut_native-stream_en_ot_yah_bid-dda_behavioural_v1_carousel_1x1_1x1_na_na_mars_na",
"Smile_YAHOO_HD.mp4",
"us_pdw_native-stream_en_ot_yah_bid-dda_behavioural_v1_standard_1x1_1x1_na_na_mars_na",
"Nose_YAHOO_HD.mp4",
"Ia015_GRAVITY_BALL_not_eating_sf14_25fps_cut2050_20221205.mp4",
"IAMS_FOR_LIFE_2022_GRAVITY_DEFIER_FOR_LIFE_6SEC_16X9_25FPS_TITLED_REV1_mp4.mp4",
"15s_TemptationsTenderfills_ArtistCat .mov",
"6s_TemptationsTenderfills_ArtistCat.mov",
"Puppy_300x250",
"Consideration",
"Plugged Sporty ",
"Puppy_728x90",
"Puppy_970x250",
"Puppy_300x50",
"Puppy_160x600",
"Puppy_300x600",
"Trade Down",
"Puppy_320x50",
"Puppy_320x480",
"SOV13272_IAMS_Socials_Master_Wisdom_Boards_Indoor_15s_29-97p_V01.mp4",
"humanhealth",
"SOV13272_IAMS_Socials_Master_Wisdom_Boards_Healthy_Digest_06s_29-97p_V01.mp4",
"transitioning-rompuppytoadult",
"pluggedsporty",
"pluggedsporty 1P",
"celebrationsconnections 1P",
"buydownsuperpremiumset",
"healthproblems",
"buyuplowerpriceset",
"dogownersinterest",
"us_she_work-hard-play-hard-parents-1p-static-1_en_ot_yah_bid-adr_behavioural_v1_static_1x1_na_afs_na_mars_na",
"us_she_work-hard-play-hard-parents-static-2_en_ot_yah_bid-adr_behavioural_v1_static_1x1_na_afs_na_mars_na",
"Sheba_Bandaid_15s_16x9.mp4",
"us_she_cat-owners-static-1_en_ot_yah_bid-adr_behavioural_v1_static_1x1_na_afs_na_mars_na",
"healthsolutions",
"StreamStatic B",
"StreamStatic A",
"StreamStatic C",
"Stream Carousel A",
"Digestive Probiotic OLV B 16x9 v60 20mbps.mp4",
"mars-pedigree-pouch-goodstuff-15sec-16-9_YAHOO.mp4",
"Native Stream Carousel - Pet Connect",
"Native Stream Carousel - Pet Connect 2",
"Native Stream Carousel - Breed Selector",
"Native Stream Carousel - Puppy Hub",
"Smartbites OLV Reach B 16x9 v7-0.mp4",
"Sheba-US-2023-Hope2-CFC-OLV-Hawaii-15s-16x9-Titled-V2.mov",
"Anytime Bites OLV A 16x9.mp4",
"Static 4",
"Anytime Bites OLV B 16x9.mp4",
"SOV13923_IAMS_OLV_Cat_Lifestyle_Healthy_Enjoyment_Copy_Version_B_opt_01.mp4",
"SOV13923_IAMS_OLV_Cat_Lifestyle_Healthy_Enjoyment_Copy_Version_A_opt_01.mp4",
"Adylic-DCO_Nutro_US_970x250",
"Adylic-DCO_Nutro_US_728x90",
"Adylic-DCO_Nutro_US_300x600",
"Adylic-DCO_Nutro_US_300x250",
"Adylic-DCO_Nutro_US_160x600",
"Doing It All",
"TEMP2300002_Temptations_Parade_15s_Video_Spa2.mov",
"Family Fun Time",
"Cat Owners",
"Human HealthWellness",
"Doing It All 1P",
"Family Fun Time 1P",
"CMI Health Solutions",
"Health Problems",
"Kitten to Cat",
"CatOwners_Static",
"ConveniencedFocusedParents_Static",
"CrossSell Banner B",
"doggeneralcarousel",
"dogexercisecarousel",
"healthyweightdogbanner1",
"healthyweightdogbanner3",
"healthyweightdogbanner2",
"healthyweightdogbanner4",
"doglearncarousel",
"CrosssSell Banner A",
"dogfeedcarousel",
"Equity 160x600",
"Equity 320x50",
"Equity 320x480",
"Equity 300x600",
"Equity 728x90",
"Equity 6s",
"greeniesMakesHealthATreat_15_CAMOUFLAGE_OLV_16X9_2023_03_09_1018_1.mp4",
"AdvancedHealth320x480",
"Temptations-US-2023-CatsLoseTheirCool-Tenderfills-ArtCriticCat-Product-6s-16x9-Titled.mp4",
"greeniesMakesHealthATreat_6_STASH_OLV_16X9_2023_03_09_1018_1 3.mp4",
"Equity 970x250",
"Equity 300x250",
"Equity 300x50",
"AdvancedHealth320x50",
"AdvancedHealth300x50",
"AdvancedHealth160x600",
"AdvancedHealth300x600",
"AdvancedHealth728x90",
"Temptations-US-2023-CatsLoseTheirCool-Tenderfills-ArtCriticCat-6s-16x9-Titled.mp4",
"Main Meal French Cat 6s no new.mp4",
"Main Meal French Cat 6s Product no new.mp4",
"Main Meal French Cat 15s no new.mp4",
"Anytime Bites OLV A 16x9_2.mp4",
"greeniesMakesHealthATreat_15_STASH_OLV_16X9_2023_03_09_1018_1 3.mp4",
"AdvancedHealth300x250",
"AdvancedHealth970x250",
"AdvancedHealth6s",
"temptationsTreatsPureeMeal_Tenderfills_02521_REF_AED_MRTT0053_006_no_new_2023_03_09_1743.mp4",
"15s_TemptationsPurrrree_TeenageCat.mov",
"temptationsTreatsPureeMeal_Tenderfills_02521_REF_AED_MRTT0052_006_2023_02_15_1255.mov",
"15s - Temptations-US-2023-CatsLoseTheirCool-Tenderfills-ArtCriticCat-15s-16x9-Titled.mp4",
"TEMP2300005_Temptations_Parade_06s_Video_with_call_Spa.mov",
"P2MHRGS_SXM_Pedigree_15s.mp3",
"P2MHRHK_SXM_Pedigree_30s.mp3",
"P2MHRHL_SXM_Pedigree_30s.mp3",
"P2MHRGQ_SXM_Pedigree_15s.mp3",
"TEMP2300012_Temptations_Misifus_06s_Video_without_call_Spa.mov",
"P2MHRGR_SXM_Pedigree_15s.mp3",
"P2MHRGT_SXM_Pedigree_15s.mp3",
"P2MHRGV_SXM_Pedigree_15s.mp3",
"P2MHRGW_SXM_Pedigree_15s.mp3",
"P2MHRHM_SXM_Pedigree_30s.mp3",
"P2MHRHP_SXM_Pedigree_30s.mp3",
"TEMP2300004_Temptations_Misifus_15s_Video_Spa.mov",
"Sheba-US-2023-Hope2-CFC-OLV-Hawaii-15s-16x9_AMEND_V3_23976fps.mp4",
"MZPY0028000H_TinySneakers_15_Yahoo_2997.mp4",
"P2MHRHN_SXM_Pedigree_30s.mp3",
"P2MHRHQ_SXM_Pedigree_30s.mp3",
"M_Sheba-US-2023-WCW-NoOwner-Social-6s-16x9-TitledV2 .mp4",
"M_Sheba-US-2023-WCW-Owner-Social-6s-16x9-TitledV2 .mp4",
"M_Sheba-US-2023-WCW-Combined-Social-6s-16x9-TitledV2 .mp4",
"P2N6JF4_SXM_Pedigree_15smp3.mp3",
"P2N6JFC_SXM_Pedigree_30smp3.mp3",
"P2N6JFD_SXM_Pedigree_30smp3.mp3",
"P2N6JF6_SXM_Pedigree_15smp3.mp3",
"P2N6JFB_SXM_Pedigree_30smp3.mp3",
"Sheba-US-2023-Hope2-CFC-VA-OLV-Hawaii-6s-16x9_AMEND_23976fps.mp4",
"P2N6JF9_SXM_Pedigree_30smp3.mp3",
"P2N6JF1_SXM_Pedigree_15smp3.mp3",
"P2N6JF0_SXM_Pedigree_15smp3.mp3",
"P2N6JF8_SXM_Pedigree_30smp3.mp3",
"P2N6JF2_SXM_Pedigree_15smp3.mp3",
"P2N6JF5_SXM_Pedigree_15smp3.mp3",
"P2N6JF7_SXM_Pedigree_30smp3.mp3",
"SPOTIFY US TEMPTATIONS FEB AE 1",
"sheba-us-2023-kittenlaunch-combined-salmon-olv-vo-15s-16x9_2398.mp4",
"sheba-us-2023-kittenlaunch-eleanor-salmon-olv-vo-15s-16x9_2398.mp4",
"Sheba-US-2023-KittenLaunch-Devonte-Chicken-OLV-6s-16x9-Titled.mp4",
"Greenies-US-2024-WBMW-Stash-6s-OLV-16x9-2997fps-YAHOO_High_Bitrate.mp4",
"Greenies-US-2024-WBMW-Camouflage-6s-OLV-16x9-2997fps-YAHOO_High_Bitrate.mp4",
"Greenies-US-2024-WBMW-Stash-15s-Social-16x9-2997fps-YAHOO_High_Bitrate.mp4",
"sheba-us-2023-kittenlaunch-devonte-chicken-olv-vo-15s-16x9_2398.mp4",
"ShebaUS2023WCWDigitalStatic-320x50-splitnova1",
"ShebaUS2023WCWDigitalStatic-160x600-splitnova3",
"ShebaUS2023WCWDigitalStatic-320x50-catparentnova1",
"ShebaUS2023WCWDigitalStatic-728x90-catparentnova1",
"ShebaUS2023WCWDigitalStatic-160x600-catparentnova2",
"ShebaUS2023WCWDigitalStatic-728x90-catparentnova2",
"ShebaUS2023WCWDigitalStatic-300x50-splitnova2",
"Sheba-US-2023-WCW-DigitalStatic-300x250_splitnova-V2psd3",
"ShebaUS2023WCWDigitalStatic-970x250-splitnovapng3",
"ShebaUS2023WCWDigitalStatic-300x50-catparentnova1",
"Sheba-US-2023-WCW-DigitalStatic-300x250-V2psdnova2",
"ShebaUS2023WCWDigitalStatic-300x50-catparentnova3",
"ShebaUS2023WCWDigitalStatic-970x250-splitnovapng1",
"Sheba-US-2023-WCW-DigitalStatic-300x250_splitnova-V2psd2",
"ShebaUS2023WCWDigitalStatic-320x480-splitnova2",
"ShebaUS2023WCWDigitalStatic-160x600-catparentnova1",
"ShebaUS2023WCWDigitalStatic-300x600-catparentnova2",
"ShebaUS2023WCWDigitalStatic-320x480-catparentnova2",
"Sheba-US-2023-WCW-DigitalStatic-300x250-V2psdnova3",
"ShebaUS2023WCWDigitalStatic-320x50-catparentnova3",
"ShebaUS2023WCWDigitalStatic-160x600-catparentnova3",
"ShebaUS2023WCWDigitalStatic-728x90-splitnova1",
"ShebaUS2023WCWDigitalStatic-160x600-splitnova2",
"ShebaUS2023WCWDigitalStatic-320x480-splitnova3",
"ShebaUS2023WCWDigitalStatic-970x250-catparentnova1",
"ShebaUS2023WCWDigitalStatic-300x600-catparentnova3",
"Sheba-US-2023-KittenLaunch-Eleanor-Salmon-OLV-6s-16x9-Titled.mp4",
"Greenies-US-2024-WBMW-Camouflage-15s-OLV-16x9-2997fps-YAHOO_High_Bitrate.mp4",
"Sheba-US-2023-WCW-DigitalStatic-300x250_splitnova-V2psd1",
"ShebaUS2023WCWDigitalStatic-320x50-splitnova2",
"ShebaUS2023WCWDigitalStatic-300x600-splitnova2",
"ShebaUS2023WCWDigitalStatic-970x250-splitnovapng2",
"ShebaUS2023WCWDigitalStatic-320x50-splitnova3",
"ShebaUS2023WCWDigitalStatic-300x600-catparentnova1",
"ShebaUS2023WCWDigitalStatic-728x90-catparentnova3",
"ShebaUS2023WCWDigitalStatic-300x600-splitnova1",
"ShebaUS2023WCWDigitalStatic-320x480-splitnova1",
"ShebaUS2023WCWDigitalStatic-728x90-splitnova2",
"ShebaUS2023WCWDigitalStatic-300x50-splitnova3",
"Sheba-US-2023-WCW-DigitalStatic-300x250-V2psdnova1",
"ShebaUS2023WCWDigitalStatic-320x480-catparentnova1",
"ShebaUS2023WCWDigitalStatic-970x250-catparentnova2",
"ShebaUS2023WCWDigitalStatic-970x250-catparentnova3",
"ShebaUS2023WCWDigitalStatic-300x50-splitnova1",
"ShebaUS2023WCWDigitalStatic-160x600-splitnova1",
"ShebaUS2023WCWDigitalStatic-728x90-splitnova3",
"ShebaUS2023WCWDigitalStatic-300x600-splitnova3",
"ShebaUS2023WCWDigitalStatic-320x50-catparentnova2",
"ShebaUS2023WCWDigitalStatic-300x50-catparentnova2",
"ShebaUS2023WCWDigitalStatic-320x480-catparentnova3",
"SPOTIFY US TEMPTATIONS FEB AE 3",
"SPOTIFY US TEMPTATIONS FEB AE 2",
"15s_TemptationsPurrrree_TeenageCat",
"greeniesMakesHealthATreat_6_STASH_OLV_16X9_2023_03_09_1018_1 ",
"greeniesMakesHealthATreat_15_STASH_OLV_16X9_2023_03_09_1018_1",
"Sheba-US-2023-WCW-AltCopy-Combined-Social-6s-16x9",
"Sheba-US-2023-WCW-AltCopy-NoOwner-Social-6s-16x9",
"Anytime Bites OLV B 16x9",
"Anytime Bites OLV A 16x9",
"Core15s",
"greeniesMakesHealthATreat_15_CAMOUFLAGE_OLV_16X9_2023_03_09_1018_1",
"Sheba_Bandaid_15s_16x9",
"IFL 6s",
"Temptations_6s_Artist_Without New_ONLINE Mix_090323_FULL SCALE",
"temptationsTreatsPureeMeal_Tenderfills_02521_REF_AED_MRTT0065_006_2023_03_10_1200",
"greeniesMakesHealthATreat_6_CAMOUFLAGE_OLV_16X9_2023_04_14_1742",
"Sheba-US-2023-WCW-AltCopy-Owner-Social-6s-16x9-V3",
"Seeker 15s",
"GoodStuff15s",
"Smile6s",
"Wag6s",
"IFL 15s",
"cut56s",
"cut115s",
"cut26s",
"cut36s",
"Main Meal French Cat 15s no new",
"Main Meal French Cat 6s no new",
"temptationsTreatsPureeMeal_Tenderfills_02521_REF_AED_MRTT0063_015_2023_03_10_1200",
"Nose6s",
"Main Meal French Cat 6s Product no new",
"cut46s",
"cut16s",
"Sheba-US-2023-Hope2-CFC-OLV-Hawaii-15s-16x9_AMEND_V3_23976fps",
"Sheba-US-2023-Hope2-CFC-VA-OLV-Hawaii-6s-16x9_AMEND_23976fps",
"Sheba-US-2023-KittenLaunch-Eleanor-Salmon-OLV-6s-16x9-Titled",
"Sheba-US-2023-KittenLaunch-Devonte-Chicken-OLV-6s-16x9-Titled",
"sheba-us-2023-kittenlaunch-devonte-chicken-olv-vo-15s-16x9_2398",
"sheba-us-2023-kittenlaunch-eleanor-salmon-olv-vo-15s-16x9_2398",
"sheba-us-2023-kittenlaunch-combined-salmon-olv-vo-15s-16x9_2398",
"Wrong Bowl 06",
"Mouthbreather 15",
"Mouthbreather 06",
"temptationsTreatsPureeMeal_Tenderfills_02521_REF_AED_MRTT0053_006_no_new_2023_03_09_1743",
"Gravity 06",
"Gravity 15",
"temptationsTreatsPureeMeal_Tenderfills_02521_REF_AED_MRTT0052_006_2023_02_15_1255",
"Healthy _VS 06",
"Healthy _VT 06",
"Greenies-US-2024-WBMW-Stash-6s-OLV-16x9-2398fps-YOUTUBE",
"Greenies-US-2024-WBMW-Camouflage-15s-OLV-16x9-2398fps-YOUTUBE",
"Greenies-US-2024-WBMW-Stash-15s-Social-16x9-2398fps-YOUTUBE",
"Greenies-US-2024-WBMW-Camouflage-6s-OLV-16x9-2398fps-YOUTUBE"]

# COMMAND ----------

RestOldCreativeList = ["fi_sicc_p5-6-party-and-work_fi_dv36o_yt_bid-nadr_demo-only_18-44_work_v1_video_None_6s_1x1_na_social_bumper (OPID-3923361)_Imp",
"be_pff_p6-p10-dry-and-wet_nl_dv36o_yt_bid-dda_int-behav_co_whole-body-health_v1_video_None_6s_cat-lovers-sport-fitness_na_mars_bumper (OPID-3959200)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_pouch-puppy_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926133)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_artificial-flavours_v1_video_None_6s_na_learn-more_mars_na (OPID-3926105)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_Thumbs_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926073)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_pouch-adult-dog_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926132)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-happiness-cut11-fit_v1_video_None_6s_bumper_learn-more_mars_dconsc (OPID-3928684)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_photobomb_v1_video_None_15s_na_learn-more_mars_na (OPID-3926021)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_bid-adr_interest_na_dentastix-thumbs-cut07-fit_v1_video_None_15s_skippable_learn-more_mars_dhvyo50 (OPID-3928695)_Imp",
"dk_pdg_p6-dtx_da_dv36o_yt_bid-adr_int-behav_hvc_dtx-credibility-proven_v1_video_None_15s_25-64_na_mars_na (OPID-3908547)_Imp",
"dk_dre_p4-p6-chill-play_da_dv36o_yt_bid-adr_int-behav_hvc_chill-play-creamy-salmon_v1_video_None_10s_25-64_na_na_nemlig (OPID-3910231)_Imp",
"dk_dre_p4-p6-chill-play_da_dv36o_yt_bid-adr_int-behav_hvc_chill-Play-creamy-chicken_v1_video_None_6s_25-64_na_na_nemlig (OPID-3910230)_Imp",
"dk_dre_p4-p6-chill-play_da_dv36o_yt_bid-adr_int-behav_hvc_chill-play-creamy-salmon_v1_video_None_6s_25-64_na_na_nemlig (OPID-3910232)_Imp",
"pl_she_ao-p3-p6_pl_meta_fcb_bid-dda_interest_cgenx4154_cat-love_v1_video_1x1_0x0_think_learn-more_mars_fbi (OPID-3943742)",
"pl_she_ao-p3-p6_pl_meta_ig_bid-dda_interest_cgenx4154_cat-love_v1_video_1x1_0x0_think_learn-more_mars_ig (OPID-3943743)",
"ie_pdg_p4-6-dtx-credibility_en_dv36o_yt_bid-dda_int-behav_hvc_gums_v1_video_None_6s_25-64-dentastix-custom-audience_na_mars_bumper (OPID-3913885)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_education-holistichealth_v1_video_None_15s_na_learn-more_mars_na (OPID-3926003)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_artificial-flavours_v1_video_None_6s_na_learn-more_mars_na (OPID-3926098)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_artificial-flavours_v1_video_None_6s_na_learn-more_mars_na (OPID-3926112)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_Thumbs_v1_video_None_15s_na_learn-more_mars_na (OPID-3926026)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_education-routine_v1_video_None_15s_na_learn-more_mars_na (OPID-3926005)_Imp",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_the-date_v1_video_None_15s_na_na_mars_nonskip (OPID-3926434)_Imp",
"fi_dre_p4-p6-chill-play_fi_dv36o_yt_bid-adr_int-behav_hvc_creamy-chicken_v1_video_None_6s_25-64-cat-parents_na_na_k-ruoka (OPID-3911908)_Imp",
"dk_she_p4-6-caturday-night_da_dv36o_yt_bid-adr_int-behav_hvc_caturday-night-core-range_v1_video_None_6s_25-64-cat-parents_na_mars_bumper (OPID-3915476)_Imp",
"de_sni_p4-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_unfiltered-girlfriend_v1_video_None_15s_na_na_mars_nonskip (OPID-3926431)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-plaque-cut09-fit_v1_video_None_6s_bumper_learn-more_mars_dhvyo50 (OPID-3928688)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-gums-cut08-fit_v1_video_None_6s_bumper_learn-more_mars_dconsc (OPID-3928681)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-6s_v1_video_None_6s_na_learn-more_mars_dhvyo50 (OPID-3930813)_Imp",
"es_org_p1-13-chew-you-good_es_dv36o_yt_bid-dda_interest_18-54_cg-sunday-scaries_v1_video_None_6s_gum-interest_na_mars_na (OPID-3874788)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_education-holistichealth_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926083)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_pouch-puppy_v1_video_None_6s_na_learn-more_mars_na (OPID-3926111)_Imp",
"ie_mal_p4-p6-look-on-the-light-side_en_dv36o_yt_bid-nadr_demo-only_18-44_look-on-the-light-side_v1_video_None_20s_1x1_na_social_non-skip (OPID-3911833)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_happiness_v1_video_None_6s_na_learn-more_mars_na (OPID-3926101)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_plaque_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926129)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_happiness_v1_video_None_6s_na_learn-more_mars_na (OPID-3926115)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_powerwasg_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926092)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_cleaning_v1_video_None_6s_na_learn-more_mars_na (OPID-3926106)_Imp",
"ie_sicc_p5-6-party-and-work_en_dv36o_yt_bid-nadr_demo-only_18-44_work_v1_video_None_6s_na_na_social_bumper (OPID-3914659)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_Proven_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926071)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-cleaning-cut06-fit_v1_video_None_6s_bumper_learn-more_mars_dhvyo50 (OPID-3928679)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_pouch-adult-dog_v1_video_None_6s_na_learn-more_mars_na (OPID-3926103)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-gums-cut08-fit_v1_video_None_6s_bumper_learn-more_mars_dhvyo50 (OPID-3928682)_Imp",
"ie_exg_p4-6-extra_en_dv36o_yt_bid-adr_int-kws_18-44_sunday-scaries_v1_video_None_6s_custom-dental-audience_na_social_bumpers (OPID-3942039)_Imp",
"es_pdg_p1-13-alien-vnp_es_dv36o_yt_bid-dda_interest_do_alien_v1_video_None_20s_30-plus_na_mars_nonskip (OPID-3862460)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-plaque-cut09-fit_v1_video_None_6s_bumper_learn-more_mars_dconsc (OPID-3928687)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-15s_v1_video_None_15s_na_learn-more_mars_dhvyo50 (OPID-3930807)_Imp",
"ie_dre_p4-p6-chill-play_en_dv36o_yt_bid-dda_int-behav_hvc_chill-play-male_v1_video_None_6s_25-64_na_na_tesco (OPID-3911858)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_credibility-professional_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926080)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_cleaning_v1_video_None_6s_na_learn-more_mars_na (OPID-3926113)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_happiness_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926128)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_bid-adr_interest_na_dentastix-thumbs-cut07-fit_v1_video_None_15s_skippable_learn-more_mars_dconsc (OPID-3928694)_Imp",
"es_org_p1-13-chew-you-good_es_dv36o_yt_bid-dda_interest_18-54_cg-library_v1_video_None_15s_gum-interest_na_mars_na (OPID-3874786)_Imp",
"ie_mms_p4-6-office-party_en_dv36o_yt_bid-nadr_demo-only_18-44_office-party_v1_video_None_20s_na_na_social_nonskip (OPID-3914477)_Imp",
"ie_pdg_p4-6-dtx-credibility_en_dv36o_yt_bid-dda_int-behav_hvc_cleaning_v1_video_None_6s_25-64-dentastix-custom-audience_na_mars_bumper (OPID-3913877)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_education-holistichealth_v1_video_None_15s_na_learn-more_mars_na (OPID-3926017)_Imp",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_the-date_v1_video_None_15s_na_na_mars_nonskip (OPID-3926434)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_credibility-professional&brand_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926078)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_happiness_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926127)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_credibility-professional_v1_video_None_15s_na_learn-more_mars_na (OPID-3926030)_Imp",
"fi_ces_p4-p6-cuddling_fi_dv36o_yt_bid-adr_int-behav_hvc_westie_v1_video_None_15s_25-64-dog-parents_na_social_non-skip (OPID-3911893)_Imp",
"es_pdg_p1-6-dentastix_es_dv36o_yt_bid-dda_interest_do_dentastix-proven_v1_video_None_15s_conversion_na_mars_na (OPID-3862592)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-6s_v1_video_None_6s_na_learn-more_mars_dlteu50 (OPID-3930814)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-artificial-flavours-cut07-fit_v1_video_None_6s_bumper_learn-more_mars_dhvyo50 (OPID-3928676)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_education-routine_v1_video_None_15s_na_learn-more_mars_na (OPID-3926033)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_credibility-professional&brand_v1_video_None_15s_na_learn-more_mars_na (OPID-3926029)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_plaque_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926130)_Imp",
"de_wav_p4-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_commute-killed-your-buzz_v1_video_None_15s_na_na_amazon_nonskip (OPID-3930049)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-artificial-flavours-cut07-fit_v1_video_None_6s_bumper_learn-more_mars_dlteu50 (OPID-3928677)_Imp",
"ie_sicc_p5-6-party-and-work_en_dv36o_yt_bid-nadr_demo-only_18-44_party_v1_video_None_6s_na_na_social_bumper (OPID-3914657)_Imp",
"fi_exg_p4-p6-gydb_fi_dv36o_yt_bid-adr_interest_18-44_mindmouth-library_v1_video_None_6s_awareness_na_social_bumpers (OPID-3921137)_Imp",
"ie_pdg_p4-6-dtx-credibility_en_dv36o_yt_bid-dda_int-behav_hvc_proven_v1_video_None_15s_25-64-dentastix-custom-audience_na_mars_nonskip (OPID-3913891)_Imp",
"ie_dol_p4-p6-tasty-new-look_en_dv36o_yt_bid-nadr_demo-only_na_tasty-new-look_v1_video_None_6s_bumper-25-64_na_mars_na (OPID-3911776)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-15s_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3930809)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_cleaning_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926123)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_Proven_v1_video_None_15s_na_learn-more_mars_na (OPID-3926025)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_education-routine_v1_video_None_15s_na_learn-more_mars_na (OPID-3926019)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_education-routine_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926087)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-20s_v1_video_None_20s_na_learn-more_mars_dhvyo50 (OPID-3930810)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_credibility-professional_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926081)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_artificial-flavours_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926121)_Imp",
"es_pdg_p1-13-alien-vnp_es_dv36o_yt_bid-dda_interest_do_alien_v1_video_None_6s_30-plus_na_mars_bumper (OPID-3862461)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_education-routine&holistichealth_v1_video_None_15s_na_learn-more_mars_na (OPID-3926032)_Imp",
"ie_exg_p4-6-extra_en_dv36o_yt_bid-adr_int-kws_18-44_library_v1_video_None_6s_custom-dental-audience_na_social_bumpers (OPID-3942037)_Imp",
"de_wav_p4-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_working-from-home-not-working_v1_video_None_15s_na_na_amazon_nonskip (OPID-3930050)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_powerwasg_v1_video_None_15s_na_learn-more_mars_na (OPID-3926022)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-artificial-flavours-cut07-fit_v1_video_None_6s_bumper_learn-more_mars_dconsc (OPID-3928675)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_Thumbs_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926072)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_pouch-puppy_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926134)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_plaque_v1_video_None_6s_na_learn-more_mars_na (OPID-3926116)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-6s_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3930815)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_education-routine_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926086)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-cleaning-cut06-fit_v1_video_None_6s_bumper_learn-more_mars_dconsc (OPID-3928678)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_education-holistichealth_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926082)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_Thumbs_v1_video_None_15s_na_learn-more_mars_na (OPID-3926012)_Imp",
"ie_she_p4-6-caturday-night_en_dv36o_yt_bid-dda_int-behav_hvc_caturday-night-nc-range_v1_video_None_10s_25-64-cat-parents_na_mars_nonskip (OPID-3915630)_Imp",
"fi_exg_p4-p6-gydb_fi_dv36o_yt_bid-adr_interest_18-44_mindmouth-sunday-scaries_v1_video_None_6s_awareness_na_social_bumpers (OPID-3921138)_Imp",
"dk_whi_p5-p6-purr-more-nova_da_dv36o_yt_bid-adr_int-behav_hvc_purr-more-nova_v1_video_None_6s_25-64-cat-parents_na_mars_bumpers (OPID-3910321)_Imp",
"fr_pff_p4-p11-wbh_fr_dv360_gog_bid-adr_interest_cpremu50_millennialxeverydaylife_6s_v1_video_1x1_na_awareness_na_mars_na_na(OPID-3930854)_Imp",
"ie_exg_p4-6-extra_en_dv36o_yt_bid-adr_int-kws_18-44_library_v1_video_None_15s_custom-dental-audience_na_social_nonskip (OPID-3942035)_Imp",
"ie_pdg_p4-6-dtx-credibility_en_dv36o_yt_bid-dda_int-behav_hvc_artificials_v1_video_None_6s_25-64-dentastix-custom-audience_na_mars_bumper (OPID-3913875)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_education-routine&holistichealth_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926084)_Imp",
"ie_sicc_p5-6-party-and-work_en_dv36o_yt_bid-nadr_demo-only_18-44_work_v1_video_None_10s_na_na_social_nonskip (OPID-3914658)_Imp",
"de_sni_p4-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_the-date_v1_video_None_15s_na_na_mars_nonskip (OPID-3926430)_Imp",
"ie_pff_p4-6-whole-body-health_en_dv36o_yt_bid-dda_int-behav_hvc_whole-body-health_v1_video_None_6s_25-64-cat-lovers-added-cat-to-household_na_social_bumper (OPID-3911831)_Imp",
"ie_ces_p4-p6-cuddling_en_dv36o_yt_bid-dda_int-behav_hvc_cuddling_v1_video_None_6s_25-64_na_social_bumper (OPID-3911829)_Imp",
"dk_she_p4-6-caturday-night_da_dv36o_yt_bid-adr_int-behav_hvc_caturday-night-core-range_v1_video_None_10s_25-64-cat-parents_na_mars_nonskip (OPID-3915475)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_artificial-flavours_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926122)_Imp",
"fi_ces_p4-p6-cuddling_fi_dv36o_yt_bid-adr_int-behav_hvc_cuddling_v1_video_None_6s_25-64-dog-parents_na_social_bumper (OPID-3911890)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-happiness-cut11-fit_v1_video_None_6s_bumper_learn-more_mars_dlteu50 (OPID-3928686)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_education-holistichealth_v1_video_None_15s_na_learn-more_mars_na (OPID-3926031)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_Proven_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926070)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_pouch-adult-dog_v1_video_None_6s_na_learn-more_mars_na (OPID-3926117)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_pouch-adult-dog_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926131)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_plaque_v1_video_None_6s_na_learn-more_mars_na (OPID-3926102)_Imp",
"ie_exg_p4-6-extra_en_dv36o_yt_bid-adr_int-kws_18-44_text_v1_video_None_15s_custom-dental-audience_na_social_nonskip (OPID-3942038)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_credibility-professional&brand_v1_video_None_15s_na_learn-more_mars_na (OPID-3926015)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_credibility-professional_v1_video_None_15s_na_learn-more_mars_na (OPID-3926002)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_cleaning_v1_video_None_6s_na_learn-more_mars_na (OPID-3926099)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_pouch-adult-dog_v1_video_None_6s_na_learn-more_mars_na (OPID-3926110)_Imp",
"fi_pff_p4-6-whole-body-health_fi_dv36o_yt_bid-adr_int-behav_hvc_whole-body-health_v1_video_None_15s_25-64-cat-dog-parents_na_mars_non-skip (OPID-3913472)_Imp",
"es_org_p1-13-chew-you-good_es_dv36o_yt_bid-dda_interest_18-54_cg-phone_v1_video_None_15s_gum-interest_na_mars_na (OPID-3874787)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-20s_v1_video_None_20s_na_learn-more_mars_dogadopmom (OPID-3930812)_Imp",
"fi_dre_p4-p6-chill-play_fi_dv36o_yt_bid-adr_int-behav_hvc_creamy-salmon_v1_video_None_6s_25-64-cat-parents_na_na_k-ruoka (OPID-3911909)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-cleaning-cut06-fit_v1_video_None_6s_bumper_learn-more_mars_dlteu50 (OPID-3928680)_Imp",
"ie_pff_p4-6-whole-body-health_en_dv36o_yt_bid-dda_int-behav_hvc_whole-body-health_v1_video_None_15s_25-64-cat-lovers-added-cat-to-household_na_social_nonskip (OPID-3911830)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_credibility-brand_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926076)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_Proven_v1_video_None_15s_na_learn-more_mars_na (OPID-3925997)_Imp",
"fi_exg_p4-p6-gydb_fi_dv36o_yt_bid-adr_interest_18-44_mindmouth-library_v1_video_None_15s_awareness_na_social_non-skip (OPID-3921184)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_pouch-puppy_v1_video_None_6s_na_learn-more_mars_na (OPID-3926104)_Imp",
"fi_dre_p4-p6-chill-play_fi_dv36o_yt_bid-adr_int-behav_hvc_creamy-salmon_v1_video_None_10s_25-64-cat-parents_na_na_k-ruoka (OPID-3911912)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_education-routine&holistichealth_v1_video_None_15s_na_learn-more_mars_na (OPID-3926004)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-gums-cut08-fit_v1_video_None_6s_bumper_learn-more_mars_dlteu50 (OPID-3928683)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_gums_v1_video_None_6s_na_learn-more_mars_na (OPID-3926114)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_gums_v1_video_None_6s_na_learn-more_mars_na (OPID-3926100)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_gums_v1_video_None_6s_na_learn-more_mars_na (OPID-3926107)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_cleaning_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926124)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_gums_v1_video_None_6s_na_learn-more_mars_dogadopmom (OPID-3926126)_Imp",
"fi_dre_p4-p6-chill-play_fi_dv36o_yt_bid-adr_int-behav_hvc_creamy-chicken_v1_video_None_10s_25-64-cat-parents_na_na_k-ruoka (OPID-3911911)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_bid-adr_interest_na_dentastix-thumbs-cut07-fit_v1_video_None_15s_skippable_learn-more_mars_dlteu50 (OPID-3928696)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_powerwasg_v1_video_None_15s_na_learn-more_mars_na (OPID-3926008)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_Thumbs_v1_video_None_15s_na_learn-more_mars_na (OPID-3925998)_Imp",
"dk_sicc_p5-6-party-and-work_da_dv36o_yt_bid-nadr_demo-only_18-44_work_v1_video_None_6s_na_na_social_bumper (OPID-3914650)_Imp",
"es_mms_p1-13-meet-the-parents_es_dv36o_yt_bid-dda_interest_cin_meet-the-parents_v1_video_None_6s_na_na_mars_25-54 (OPID-3866091)_Imp",
"es_pdg_p1-6-dentastix_es_dv36o_yt_bid-dda_interest_do_dentastix-thumbs_v1_video_None_15s_conversion_na_mars_na (OPID-3862593)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_photobomb_v1_video_None_15s_na_learn-more_mars_na (OPID-3926007)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_powerwasg_v1_video_None_15s_na_learn-more_mars_na (OPID-3926036)_Imp",
"dk_whi_p5-p6-purr-more-nova_da_dv36o_yt_bid-adr_int-behav_hvc_purr-more-nova_v2_video_None_6s_25-64-cat-parents_na_mars_bumpers (OPID-3910322)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-happiness-cut11-fit_v1_video_None_6s_bumper_learn-more_mars_dhvyo50 (OPID-3928685)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_credibility-professional&brand_v1_video_None_15s_na_learn-more_mars_na (OPID-3926001)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_photobomb_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926091)_Imp",
"fi_exg_p4-p6-gydb_fi_dv36o_yt_bid-adr_interest_18-44_mindmouth-text_v1_video_None_15s_awareness_na_social_non-skip (OPID-3921186)_Imp",
"de_twx_p4-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_camping_v1_video_None_20s_na_na_mars_nonskip (OPID-3926556)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_plaque_v1_video_None_6s_na_learn-more_mars_na (OPID-3926109)_Imp",
"ie_pdg_p4-6-dtx-credibility_en_dv36o_yt_bid-dda_int-behav_hvc_plaque_v1_video_None_6s_25-64-dentastix-custom-audience_na_mars_bumper (OPID-3913889)_Imp",
"ie_dre_p4-p6-chill-play_en_dv36o_yt_bid-dda_int-behav_hvc_chill-play-female_v1_video_None_6s_25-64_na_na_tesco (OPID-3911857)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_photobomb_v1_video_None_15s_na_learn-more_mars_channel-factory (OPID-3926090)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_pouch-puppy_v1_video_None_6s_na_learn-more_mars_na (OPID-3926118)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_credibility-professional_v1_video_None_15s_na_learn-more_mars_na (OPID-3926016)_Imp",
"ie_she_p4-6-caturday-night_en_dv36o_yt_bid-dda_int-behav_hvc_caturday-night-nc-range_v1_video_None_6s_25-64-cat-parents_na_mars_bumper (OPID-3915631)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_credibility-brand_v1_video_None_15s_na_learn-more_mars_na (OPID-3926014)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_credibility-professional&brand_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926079)_Imp",
"fi_pdg_p6-dtx_fi_dv36o_yt_bid-adr_int-behav_do_artificials_v1_video_None_6s_25-64_na_mars_dentastix-custom-audience (OPID-3916535)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dconscious_credibility-brand_v1_video_None_15s_na_learn-more_mars_na (OPID-3926000)_Imp",
"fi_pdg_p6-dtx_fi_dv36o_yt_bid-adr_int-behav_do_plaque_v1_video_None_6s_25-64_na_mars_dentastix-custom-audience (OPID-3916538)_Imp",
"es_mms_p1-13-meet-the-parents_es_dv36o_yt_bid-dda_interest_cin_meet-the-parents_v1_video_None_15s_na_na_mars_25-54 (OPID-3866090)_Imp",
"fi_pdg_p6-dtx_fi_dv36o_yt_bid-adr_int-behav_do_gums_v1_video_None_6s_25-64_na_mars_dentastix-custom-audience (OPID-3916537)_Imp",
"uk_pdg_p4-6-dentastix-credibility-ao_en_dv36o_yt_dir-adr_interest_na_dentastix-plaque-cut09-fit_v1_video_None_6s_bumper_learn-more_mars_dlteu50 (OPID-3928689)_Imp",
"ie_twx_p4-p6-camping_en_dv36o_yt_bid-nadr_demo-only_18-44_camping_v1_video_None_15s_1x1_na_social_non-skip (OPID-3911832)_Imp",
"fi_pdg_p6-dtx_fi_dv36o_yt_bid-adr_int-behav_do_proven_v1_video_None_15s_25-64_na_mars_dentastix-custom-audience (OPID-3916534)_Imp",
"fi_pdg_p6-dtx_fi_dv36o_yt_bid-adr_int-behav_do_cleaning_v1_video_None_6s_25-64_na_mars_dentastix-custom-audience (OPID-3916536)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_Proven_v1_video_None_15s_na_learn-more_mars_na (OPID-3926011)_Imp",
"dk_dre_p4-p6-chill-play_da_dv36o_yt_bid-adr_int-behav_hvc_chill-Play-creamy-chicken_v1_video_None_10s_25-64_na_na_nemlig (OPID-3910229)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_photobomb_v1_video_None_15s_na_learn-more_mars_na (OPID-3926035)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_credibility-brand_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926077)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_gums_v1_video_None_6s_na_learn-more_mars_channel-factory (OPID-3926125)_Imp",
"ie_dre_p4-p6-chill-play_en_dv36o_yt_bid-dda_int-behav_hvc_chill-play_v1_video_None_14s_25-64_na_na_tesco (OPID-3911856)_Imp",
"ie_sicc_p5-6-party-and-work_en_dv36o_yt_bid-nadr_demo-only_18-44_party_v1_video_None_10s_na_na_social_nonskip (OPID-3914656)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dlightu50_credibility-brand_v1_video_None_15s_na_learn-more_mars_na (OPID-3926028)_Imp",
"fi_exg_p4-p6-gydb_fi_dv36o_yt_bid-adr_interest_18-44_mindmouth-phone_v1_video_None_15s_awareness_na_social_non-skip (OPID-3921185)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_education-routine&holistichealth_v1_video_None_15s_na_learn-more_mars_na (OPID-3926018)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-15s_v1_video_None_15s_na_learn-more_mars_dlteu50 (OPID-3930808)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_powerwasg_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926093)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_dheavyo50_happiness_v1_video_None_6s_na_learn-more_mars_na (OPID-3926108)_Imp",
"uk_pdg_p4-alien_en_dv36o_yt_bid-dda_interest_na_alien-20s_v1_video_None_20s_na_learn-more_mars_dlteu50 (OPID-3930811)_Imp",
"ie_exg_p4-6-extra_en_dv36o_yt_bid-adr_int-kws_18-44_phone_v1_video_None_15s_custom-dental-audience_na_social_nonskip (OPID-3942036)_Imp",
"fi_pff_p4-6-whole-body-health_fi_dv36o_yt_bid-adr_int-behav_hvc_whole-body-health_v1_video_None_6s_25-64-cat-dog-parents_na_mars_bumpers (OPID-3913475)_Imp",
"uk_pdg_p4-contextual-targeting_en_dv36o_yt_bid-adr_interest_na_education-routine&holistichealth_v1_video_None_15s_na_learn-more_mars_dogadopmom (OPID-3926085)_Imp",
"ie_ces_p4-p6-cuddling_en_dv36o_yt_bid-dda_int-behav_hvc_adorable_v1_video_None_6s_25-64_na_social_bumper (OPID-3911828)_Imp",
"fr_she_laws-promo-non-vnp-p4_fr_meta_fbi_bid-dda_interest_cgenx4154_a9_v1_static_1x1_na_see_learn-more_mars_na (OPID-3961720)",
"fr_she_laws-vnp-p4_fr_meta_fbi_bid-dda_interest_chvyo55_a6_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3961724)",
"fr_she_laws-vnp-p4_fr_meta_fbi_bid-dda_interest_clte2840_a6_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3961725)",
"fr_she_laws-vnp-p4_fr_meta_fbi_bid-dda_interest_cgenx4154_a6_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3961723)",
"fr_she_laws-promo-non-vnp-p4_fr_meta_fbi_bid-dda_interest_clte2840_a9_v1_static_1x1_na_see_learn-more_mars_na (OPID-3961717)",
"fr_she_laws-promo-non-vnp-p4_fr_meta_fbi_bid-dda_interest_chvyo55_a9_v1_static_1x1_na_see_learn-more_mars_na (OPID-3961722)",
"pl_she_laws-vnp-p4_pl_meta_fbi_bid-dda_interest_cgenx4154_a6_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3961748)",
"nl_mmm_p4-minis-intro_nl_dv36o_yt_bid-dda_interest_18-54_ns_v2_video_None_15s_na_na_mars_na (OPID-3960595)_Imp",
"nl_mmm_p4-minis-intro_nl_dv36o_yt_bid-dda_interest_18-54_bumper_v1_video_None_6s_na_na_mars_na (OPID-3960594)_Imp",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-adr_interest_chvyo55_c21_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962337)",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-nadr_rtg_vvlalrtg_b26_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962666)",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-dda_interest_clte2840_b29_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962661)",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-adr_interest_clte2840_b29_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962404)",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-dda_interest_cgenx4154_a24_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962658)",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-dda_lal_vvlalrtg_c21_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962425)",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-adr_interest_cgenx4154_a24_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962319)",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-dda_lal_vvlalrtg_b26_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962423)",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-dda_lal_vvlalrtg_a24_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962421)",
"fr_she_laws-non-vnp-p4_fr_meta_fbi_bid-dda_lal_vvlalrtg_b29_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962424)",
"dk_whi_p5-p6-purr-more-nova_da_dv36o_yt_bid-adr_int-behav_hvc_purr-more-nova_v1_video_None_15s_25-64-cat-parents_na_mars_non-skip (OPID-3910451)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-french_v1_video_None_6s_party_learn-more_mars_na (OPID-3963102)_Imp",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_cgenx4154 (OPID-3969372)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clteheau50_millenial-early-ownership_v1_video_None_15s_na_na_mars_na (OPID-3975722)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clteheau50_millenial-early-ownership_v1_video_None_6s_na_na_mars_na (OPID-3975723)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clteheau50_millenial-health-concerns_v1_video_None_15s_na_na_mars_na (OPID-3975726)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clteheau50_millenial-every-day-life_v1_video_None_6s_na_na_mars_na (OPID-3975725)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clteheau50_millenial-health-concerns_v1_video_None_6s_na_na_mars_na (OPID-3975727)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clteheau50_millenial-every-day-life_v1_video_None_15s_na_na_mars_na (OPID-3975724)_Imp",
"de_exg_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_library_v1_video_None_6s_bumpers_na_amazon_na (OPID-3961017)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-nadr_demo-only_18plus_becky_v1_video_None_6s_na_learn-more_mars_na (OPID-3966449)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-nadr_demo-only_18plus_cee_v1_video_None_6s_na_learn-more_mars_na (OPID-3966451)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-dda_int-behav_hvc_zoe_v1_video_None_15s_na_learn-more_mars_na (OPID-3966492)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-dda_int-behav_hvc_becky_v1_video_None_6s_na_learn-more_mars_na (OPID-3966489)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-dda_int-behav_hvc_cee_v1_video_None_15s_na_learn-more_mars_na (OPID-3966490)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-nadr_demo-only_18plus_cee_v1_video_None_15s_na_learn-more_mars_na (OPID-3966450)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-dda_int-behav_hvc_cee_v1_video_None_6s_na_learn-more_mars_na (OPID-3966491)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-dda_int-behav_hvc_zoe_v1_video_None_6s_na_learn-more_mars_na (OPID-3966493)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-nadr_demo-only_18plus_zoe_v1_video_None_6s_na_learn-more_mars_na (OPID-3966453)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-nadr_demo-only_18plus_zoe_v1_video_None_15s_na_learn-more_mars_na (OPID-3966452)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-nadr_demo-only_18plus_becky_v1_video_None_15s_na_learn-more_mars_na (OPID-3966448)_Imp",
"uk_gal_purpose-eba_en_dv36o_yt_bid-dda_int-behav_hvc_becky_v1_video_None_15s_na_learn-more_mars_na (OPID-3966488)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-french_v1_video_None_10s_office_learn-more_mars_na (OPID-3962115)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-german_v1_video_None_6s_party_learn-more_mars_na (OPID-3963100)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-french_v1_video_None_10s_office_learn-more_mars_na (OPID-3963108)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-french_v1_video_None_6s_office_learn-more_mars_na (OPID-3962116)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-french_v1_video_None_10s_party_learn-more_mars_na (OPID-3963101)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-french_v1_video_None_10s_party_learn-more_mars_na (OPID-3962117)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-german_v1_video_None_10s_party_learn-more_mars_na (OPID-3963099)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-german_v1_video_None_6s_office_learn-more_mars_na (OPID-3962111)_Imp",
"be_mms_p5-p13-meet-the-parents_fr_dv36o_yt_bid-adr_demo-only_18-54_meet-the-parents_v2_video_None_6s_na_na_mars_na (OPID-3975448)_Imp",
"be_mms_p5-p11-m&ms-minis_fr_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-fr-6s_v1_video_None_6s_na_na_mars_efficient-reach (OPID-3966524)_Imp",
"be_mms_p5-p13-meet-the-aprents_nl_dv36o_yt_bid-adr_demo-only_18-54_meet-the-parents_v1_video_None_15s_na_na_mars_na (OPID-3975452)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-german_v1_video_None_6s_office_learn-more_mars_na (OPID-3963107)_Imp",
"be_mms_p5-p13-meet-the-aprents_nl_dv36o_yt_bid-adr_demo-only_18-54_meet-the-parents_v2_video_None_6s_na_na_mars_na (OPID-3975453)_Imp",
"be_mms_p5-p13-meet-the-parents_fr_dv36o_yt_bid-adr_demo-only_18-54_meet-the-parents_v1_video_None_15s_na_na_mars_na (OPID-3975447)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-german_v1_video_None_6s_party_learn-more_mars_na (OPID-3962107)_Imp",
"ch_mms_p5-p13-m&ms-minis_fr_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-fr-6s_v1_video_None_6s_na_na_social_efficient-reach (OPID-3966565)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp",
"ch_she_p2-p8-caturday-night_fr_dv36o_yt_bid-dda_behav-life_18plus_taste-of-rome_v2_video_None_6s_na_na_na_na (OPID-3984342)_Imp",
"ch_she_p2-p8-caturday-night_de_dv36o_yt_bid-dda_behav-life_18plus_taste-of-rome_v1_video_None_6s_na_na_na_na (OPID-3984341)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-german_v1_video_None_10s_office_learn-more_mars_na (OPID-3962110)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-french_v1_video_None_6s_office_learn-more_mars_na (OPID-3963109)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-french_v1_video_None_6s_party_learn-more_mars_na (OPID-3962118)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-german_v1_video_None_10s_party_learn-more_mars_na (OPID-3962106)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968742)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968726)_Imp",
"TH-Lazada-Pedigree-MidMonth-SingleProduct â€“ Copy",
"TH-Lazada-Pedigree-EndofMonth-MultiProduct â€“ Copy",
"TH-Shopee-Pedigree-5.5-May-carousel-image",
"TH-Shopee-Pedigree-5.5-May-carousel-image",
"TH-Shopee-Whiskas-5.5 May-collection",
"TH-Shopee-Whiskas-5.5 May-carousel",
"fr_pdg_alien-p6_fr_meta_fbi_bid-dda_interest_dcmbaff_left-town_v1_video_1x1_10s_na_learn-more_mars_na (OPID-3991230)",
"fr_pdg_alien-p6_fr_meta_fbi_bid-dda_interest_dcmbaff_no-cute-dogs_v1_video_1x1_10s_na_learn-more_mars_na (OPID-3991231)",
"TH-Shopee-Whiskas-5.5 May -collection_vdo",
"TH-Shopee-Whiskas-5.5 May-collection",
"TH-Shopee-Whiskas-5.5 May-carousel",
"TH-Shopee-Whiskas-5.5 May-collection",
"TH-Shopee-Whiskas-5.5 May-carousel_vdo",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-adr_demo-only_18-34_extra-chew-good-library-20s_v4_video_None_20s_na_na_mars_na (OPID-3972385)_Imp",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-adr_demo-only_18-34_extra-chew-good-sunday-scaries-6s_v7_video_None_6s_na_na_mars_na (OPID-3972387)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_event_v2_video_None_10s_na_learn-more_mars_non-skip (OPID-3970023)_Imp",
"uk_exg_p5-6-chew-good_en_dv36o_yt_bid-dda_int-behav_hvc_mail_v1_video_None_6s_na_na_mars_workers (OPID-3977632)_Imp",
"de_wav_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_talent-show-6_v1_video_None_6s_bumper_na_amazon_na (OPID-3964012)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_rokjesnacht_v4_video_None_10s_na_learn-more_mars_non-skip (OPID-3970035)_Imp",
"uk_exg_p5-6-chew-good_en_dv36o_yt_bid-dda_int-behav_hvc_101_v1_video_None_6s_na_na_mars_student (OPID-3977630)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_rokjesnacht_v2_video_None_10s_na_learn-more_mars_non-skip (OPID-3970031)_Imp",
"TH-Shopee-Whiskas-5.5 May-carousel_vdo",
"TH-Shopee-Whiskas-5.5 May -collection_vdo",
"TH-Shopee-Whiskas-5.5 May-carousel_vdo",
"nl_mmm_p4-minis_nl_dv36o_yt_bid-dda_interest_18-54_mms-skip_v3_video_None_15s_na_na_mars_na (OPID-3968772)_Imp",
"TH-Shopee-Whiskas-5.5 May-carousel_vdo",
"de_exg_p4-p6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_library_v1_video_None_20s_nonskip_na_amazon_na (OPID-3930500)_Imp",
"de_wav_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_talent-show_v1_video_None_15s_non-skip_na_amazon_na (OPID-3964467)_Imp",
"TH-Shopee-Whiskas-5.5 May -collection_vdo",
"TH-Shopee-Whiskas-5.5 May-carousel",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-nadr_demo-only_18plus_extra-chew-good-15s_v1_video_None_15s_na_na_mars_workers (OPID-3972379)_Imp",
"de_mms_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_meet-the-parents-peanut_v1_video_None_15s_na_na_mars_na (OPID-3961939)_Imp",
"TH-Shopee-Whiskas-5.5 May -collection_vdo",
"it_mms_p5-7-summer-of-sport_it_dv36o_yt_bid-dda_int-behav_18-54_summer-of-sport_v1_video_None_15s_general-interest_na_amazon_nonskip (OPID-3975010)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-flemish_v1_video_None_10s_office_na_mars_na (OPID-3964511)_Imp",
"de_mms_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_minis_v1_video_None_15s_na_na_mars_na (OPID-3961941)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-french_v1_video_None_6s_party_na_mars_na (OPID-3964853)_Imp",
"de_mms_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_minis_v1_video_None_6s_na_na_mars_na (OPID-3961942)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-french_v1_video_None_6s_office_na_mars_na (OPID-3964471)_Imp",
"be_mms_p5-p11-m&ms-minis_nl_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-fl-6s_v1_video_None_6s_na_na_mars_efficient-reach (OPID-3966053)_Imp",
"TH-Shopee-Pedigree-5.5-May-carousel-vdo",
"TH-Shopee-Pedigree-5.5-May-collection-image",
"be_mms_p5-p11-m&ms-minis_nl_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-fl-15s_v1_video_None_15s_na_na_mars_efficient-reach (OPID-3966052)_Imp",
"TH-Shopee-Pedigree-5.5-May-collection-vdo",
"fi_sicc_p5-6-party-and-work_fi_dv36o_yt_bid-nadr_demo-only_18-44_party_v1_video_None_10s_1x1_na_social_non-skip (OPID-3923362)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_event_v3_video_None_15s_na_learn-more_mars_non-skip (OPID-3970026)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-flemish_v1_video_None_6s_party_na_mars_na (OPID-3964855)_Imp",
"it_mms_p5-7-summer-of-sport_it_dv36o_yt_bid-dda_int-behav_18-54_summer-of-sport_v1_video_None_15s_sports-tickets_na_amazon_nonskip (OPID-3975011)_Imp",
"fi_sicc_p5-6-party-and-work_fi_dv36o_yt_bid-nadr_demo-only_18-44_work_v1_video_None_10s_1x1_na_social_non-skip (OPID-3923363)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-french_v1_video_None_10s_party_na_mars_na (OPID-3964474)_Imp",
"gr_skt_p1-6-olv_el_dv36o_yt_bid-nadr_demo-only_18-34_gaming_v1_video_None_10s_na_na_social_nonskip (OPID-3852986)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-french_v1_video_None_6s_office_na_mars_na (OPID-3965036)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-french_v1_video_None_10s_office_na_mars_na (OPID-3965035)_Imp",
"TH-Shopee-Pedigree-5.5-May-carousel-vdo",
"TH-Shopee-Pedigree-5.5-May-carousel-vdo",
"TH-Shopee-Pedigree-5.5-May-carousel-vdo",
"TH-Shopee-Pedigree-5.5-May-collection-image",
"TH-Shopee-Pedigree-5.5-May-carousel-image",
"TH-Shopee-Pedigree-5.5-May-collection-image",
"TH-Shopee-Pedigree-5.5-May-collection-vdo",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-french_v1_video_None_10s_office_na_mars_na (OPID-3964470)_Imp",
"ch_mms_p5-p13-m&ms-minis_fr_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-fr-15s_v1_video_None_15s_na_na_social_efficient-reach (OPID-3966564)_Imp",
"uk_exg_p5-6-chew-good_en_dv36o_yt_bid-dda_int-behav_hvc_backbag_v1_video_None_6s_na_na_mars_student (OPID-3977631)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-french_v1_video_None_10s_party_na_mars_na (OPID-3964852)_Imp",
"ch_sicc_p5-p6-taste-your-happy-place_de_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-german_v1_video_None_10s_office_learn-more_mars_na (OPID-3963106)_Imp",
"ch_mms_p5-p13-m&ms-minis_de_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-de-6s_v1_video_None_6s_na_na_social_efficient-reach (OPID-3966563)_Imp",
"it_mms_p5-7-summer-of-sport_it_dv36o_yt_bid-dda_int-behav_18-54_summer-of-sport_v1_video_None_15s_e-sports_na_amazon_nonskip (OPID-3975009)_Imp",
"TH-Shopee-Pedigree-5.5-May-collection-vdo",
"TH-Shopee-Pedigree-5.5-May-collection-image",
"TH-Shopee-Pedigree-5.5-May-collection-vdo",
"TH-Shopee-Whiskas-5.5 May-collection",
"TH-Shopee-Whiskas-5.5 May-carousel",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp_CP_INT",
"TH-Shopee-Whiskas-7-9 May-carousel",
"TH-Shopee-Whiskas-7-9 May-collection",
"TH-Shopee-Whiskas-7-9 May-collection",
"TH-Shopee-Whiskas-7-9 May-carousel",
"TH-Shopee-Whiskas-7-9 May-collection",
"TH-Shopee-Whiskas-7-9 May-carousel",
"TH-Shopee-Whiskas-7-9 May-collection",
"TH-Shopee-Whiskas-7-9 May-carousel",
"TH-Shopee-Whiskas-7-9 May-collection",
"TH-Shopee-Whiskas-7-9 May-carousel",
"TH-Shopee-Whiskas-7-9 May-carousel",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_videocall_v1_video_None_15s_na_na_mars_non-skip (OPID-3987713)_Imp",
"it_whi_p7-p8-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_playfulness_v1_video_None_15s_25-45_na_amazon_non-skip (OPID-3986586)_Imp",
"it_whi_p10-p11-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_inside-outside_v1_video_None_15s_25-45_na_amazon_non-skip (OPID-3986601)_Imp",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_work_v1_video_None_15s_na_na_mars_non-skip (OPID-3987714)_Imp",
"it_whi_p10-p11-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_inside-outside_v1_video_None_15s_new-kitten_na_amazon_non-skip (OPID-3986603)_Imp",
"it_whi_p7-p8-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_playfulness_v1_video_None_15s_new-kitten_na_amazon_non-skip (OPID-3986588)_Imp",
"it_whi_p7-p8-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_playfulness_v1_video_None_15s_45-64_na_amazon_non-skip (OPID-3986587)_Imp",
"pl_pff_wbh-p6_pl_meta_fbi_bid-adr_interest_clteheau50_everyday-life_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991417)",
"pl_pff_wbh-p6_pl_meta_fbi_bid-adr_interest_chvyheao50_early-ownership_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991413)",
"pl_pff_wbh-p6_pl_meta_fbi_bid-adr_interest_clteheau50_early-ownership_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991416)",
"pl_pff_wbh-p6_pl_meta_fbi_bid-adr_interest_clteheau50_health-concerns_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991418)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_superpremium-healthconcerns_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990952)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_healthconscious-healthconcerns_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990946)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_interest_clteheau50_health-concerns_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990874)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_lalrtg_vvlalrtg_thumbs_v1_video_1x1_15s_na_learn-more_mars_think (OPID-3991356)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_lalrtg_vvlalrtg_proven_v1_video_1x1_15s_na_learn-more_mars_think (OPID-3991355)",
"TH-Shopee-Whiskas-5.5 May-collection",
"TH-Shopee-Whiskas-5.5 May -collection_vdo",
"TH-Shopee-Pedigree-5.5-May-carousel-vdo",
"TH-Shopee-Pedigree-5.5-May-carousel-vdo",
"TH-Shopee-Whiskas-5.5 May-carousel_vdo",
"TH-Shopee-Pedigree-5.5-May-carousel-image",
"TH-Shopee-Whiskas-5.5 May-carousel_vdo",
"TH-Shopee-Pedigree-5.5-May-carousel-image",
"TH-Shopee-Whiskas-5.5 May -collection_vdo",
"TH-Shopee-Pedigree-5.5-May-collection-image",
"TH-Shopee-Whiskas-5.5 May-carousel",
"TH-Shopee-Pedigree-5.5-May-carousel-image",
"TH-Shopee-Whiskas-5.5 May-collection",
"TH-Shopee-Pedigree-5.5-May-collection-vdo",
"TH-Shopee-Pedigree-5.5-May-collection-image",
"TH-Shopee-Pedigree-5.5-May-collection-vdo",
"TH-Shopee-Whiskas-5.5 May-carousel",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp_CP_INT",
"TH-Shopee-Pedigree-Petclub-7-9May-collection-image",
"TH-Shopee-Pedigree-Petclub-7-9May-collection-image",
"TH-Shopee-Pedigree-Petclub-May-collection-vdo",
"TH-Shopee-Pedigree-Petclub-May-collection-vdo",
"TH-Shopee-Pedigree-Petclub-7-9May-collection-image",
"TH-Shopee-Pedigree-Petclub-May-carousel-image",
"TH-Shopee-Pedigree-Petclub-May-carousel-image",
"TH-Shopee-Pedigree-Petclub-7-9May-carousel-image",
"TH-Shopee-Pedigree-Petclub-7-9May-collection-image",
"TH-Shopee-Pedigree-Petclub-May-carousel-image",
"TH-Shopee-Pedigree-Petclub-May-carousel-image",
"TH-Shopee-Pedigree-Petclub-May-carousel-image",
"TH-Shopee-Pedigree-Petclub-May-collection-vdo",
"TH-Shopee-Pedigree-Petclub-7-9May-carousel-image",
"TH-Shopee-Pedigree-Petclub-May-collection-vdo",
"TH-Shopee-Pedigree-Petclub-7-9May-carousel-image",
"TH-Shopee-Pedigree-Petclub-7-9May-collection-image",
"TH-Shopee-Pedigree-Petclub-May-carousel-image",
"TH-Shopee-Whiskas-10-18 May-carousel",
"TH-Shopee-Whiskas-10-18 May-carousel",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_lal_vvlal_thinkdo-tasymixv1-50_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972290)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_rtg_vvrtg_thinkdo-tasymixv1-50_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972294)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_interest_chvyo50_asmr_v1_video_1x1_10s_na_learn-more_mars_na (OPID-3972278)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_rtg_vvrtg_thinkdo-corev1-18-40_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972292)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_lal_vvlal_thinkdo-tasymixv2-50_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972291)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_interest_chvyo50_purring_v1_video_1x1_10s_na_learn-more_mars_na (OPID-3972280)",
"TH-Shopee-Pedigree-Petclub-7-9May-carousel-image",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-adr_demo-only_18-34_extra-chew-good-library-15s_v5_video_None_15s_na_na_mars_na (OPID-3972384)_Imp",
"TH-Shopee-Pedigree-Petclub-May-collection-vdo",
"TH-Shopee-Pedigree-Petclub-7-9May-carousel-image",
"TH-Shopee-Pedigree-Petclub-7-9May-carousel-image",
"TH-Shopee-Pedigree-Petclub-May-collection-vdo",
"TH-Shopee-Pedigree-Petclub-7-9May-collection-image",
"TH-Shopee-Whiskas-7-9 May-collection",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_interest_chvyo50_relaxation_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972281)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_rtg_vvrtg_thinkdo-tasymixv2-50_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972295)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_rtg_vvrtg_thinkdo-tastymix18-40_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972293)",
"pl_whi_playful-p5_pl_meta_fbi_bid-dda_interest_chvyo50_duo_v1_video_1x1_6s_think_shop-now_mars_na (OPID-3972089)",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_super-premium-every-day-life_v1_video_None_15s_na_na_mars_na (OPID-3995039)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_health-conscious-every-day-life_v1_video_None_15s_na_na_mars_na (OPID-3995027)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_health-conscious-health-concerns_v1_video_None_6s_na_na_mars_na (OPID-3995030)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_super-premium-early-ownership_v1_video_None_6s_na_na_mars_na (OPID-3995038)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_super-premium-early-ownership_v1_video_None_15s_na_na_mars_na (OPID-3995037)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_health-conscious-early-ownership_v1_video_None_6s_na_na_mars_na (OPID-3995026)_Imp",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-nadr_demo-only_18plus_extra-chew-good-sunday-scaries-6s_v3_video_None_6s_na_na_mars_workers (OPID-3972381)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_health-conscious-every-day-life_v1_video_None_6s_na_na_mars_na (OPID-3995028)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_millenial-early-ownership_v1_video_None_6s_na_na_mars_na (OPID-3995032)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_super-premium-health-concerns_v1_video_None_6s_na_na_mars_na (OPID-3995042)_Imp",
"de_twx_vnp-p5-6_de_dv36o_yt_bid-nadr_demo-only_18-44_caramel-fold_v1_video_None_6s_na_na_mars_bumper (OPID-3970375)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_event_v1_video_None_10s_na_learn-more_mars_non-skip (OPID-3970021)_Imp",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-adr_demo-only_18-34_extra-chew-good-library-6s_v4_video_None_6s_na_na_mars_na (OPID-3972386)_Imp",
"de_exg_p4-p6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_phone_v1_video_None_20s_nonskip_na_amazon_na (OPID-3930501)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_rokjesnacht_v4_video_None_15s_na_learn-more_mars_non-skip (OPID-3970036)_Imp",
"fi_whi_p5-6-purr-more-nova_fi_dv36o_yt_bid-adr_int-behav_hvc_purr-more-nova_v1_video_None_6s_25-64_na_mars_na (OPID-3913695)_Imp",
"uk_whi_p13-playfulness_en_dv36o_gog_bid-dda_interest_na_purrmore_v1_video_None_20s_na_na_mars_clight18-45 (OPID-3832121)_Imp",
"uk_exg_p5-p6-chew-good_en_dv36o_yt_bid-nadr_demo-only_18plus_extra-chew-good-20s_v2_video_None_20s_na_na_mars_workers (OPID-3972380)_Imp",
"fi_whi_p5-6-purr-more-nova_fi_dv36o_yt_bid-adr_int-behav_hvc_purr-more-nova_v1_video_None_15s_25-64_na_mars_na (OPID-3913694)_Imp",
"de_mms_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_meet-the-parents-peanut_v1_video_None_6s_na_na_mars_na (OPID-3961940)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_event_v4_video_None_15s_na_learn-more_mars_non-skip (OPID-3970028)_Imp",
"nl_mms_p4-movies_nl_dv36o_yt_bid-dda_interest_mov_rokjesnacht_v3_video_None_15s_na_learn-more_mars_non-skip (OPID-3970034)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-flemish_v1_video_None_10s_office_na_mars_na (OPID-3964468)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-work-flemish_v1_video_None_6s_office_na_mars_na (OPID-3964469)_Imp",
"uk_whi_p13-playfulness_en_dv36o_gog_bid-dda_interest_na_purrmore_v1_video_None_15s_na_na_mars_cheavyo50 (OPID-3832117)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-party-flemish_v1_video_None_10s_party_na_mars_na (OPID-3964854)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-flemish_v1_video_None_10s_party_na_mars_na (OPID-3964472)_Imp",
"uk_whi_p13-playfulness_en_dv36o_gog_bid-dda_interest_na_purrmore_v1_video_None_15s_na_na_mars_clight18-45 (OPID-3832118)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_fr_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-french_v1_video_None_6s_party_na_mars_na (OPID-3964475)_Imp",
"uk_whi_p13-playfulness_en_dv36o_gog_bid-dda_interest_na_purrmore_v1_video_None_15s_na_na_mars_cpome (OPID-3832119)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_demo-only_18-34_taste-your-happy-place-work-flemish_v1_video_None_6s_office_na_mars_na (OPID-3964512)_Imp",
"ch_mms_p5-p13-m&ms-minis_de_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-de-15s_v1_video_None_15s_na_na_social_efficient-reach (OPID-3966562)_Imp",
"gr_skt_p1-6-olv_el_dv36o_yt_bid-nadr_demo-only_18-34_general_v1_video_None_10s_na_na_social_nonskip (OPID-3852987)_Imp",
"be_mms_p5-p11-m&ms-minis_fr_dv36o_yt_bid-adr_demo-only_18-54_m&ms-minis-fr-15s_v1_video_None_15s_na_na_mars_efficient-reach (OPID-3966051)_Imp",
"gr_sni_p5-8-ice-cream_el_dv36o_yt_bid-nadr_demo-only_18-44_party_v1_video_None_10s_na_na_social_na (OPID-3973111)_Imp",
"uk_whi_p13-playfulness_en_dv36o_gog_bid-dda_interest_na_purrmore_v1_video_None_20s_na_na_mars_cheavyo50 (OPID-3832120)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968736)_Imp",
"be_sicc_p5-p6-taste-your-happy-place_nl_dv36o_yt_bid-dda_int-life_hvc_taste-your-happy-place-party-flemish_v1_video_None_6s_party_na_mars_na (OPID-3964473)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968724)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_cpremu50_super-premium-every-day-life_v1_video_None_15s_na_na_mars_na (OPID-3975748)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_na_health-conscious-every-day-life_v1_video_None_6s_na_na_mars_chvyheao50 (OPID-3975755)_Imp",
"be_pdg_p3-7-supplements_nl_dv36o_yt_bid-dda_interest_18plus_supplements-think_v1_video_None_10s_dog-owners_na_mars_nonskip (OPID-3924267)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_na_health-conscious-early-ownership_v1_video_None_6s_na_na_mars_chvyheao50 (OPID-3975753)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_cpremu50_super-premium-health-concerns_v1_video_None_15s_na_na_mars_na (OPID-3975750)_Imp",
"uk_whi_p13-playfulness_en_dv36o_gog_bid-dda_interest_na_purrmore_v1_video_None_20s_na_na_mars_cpome (OPID-3832122)_Imp",
"TH-Shopee-Pedigree-MidMonth-10-18May-carousel-image",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_cpome_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_na (OPID-3969371)_Imp",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_vv-rtg (OPID-3969378)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_na_health-conscious-early-ownership_v1_video_None_15s_na_na_mars_chvyheao50 (OPID-3975752)_Imp",
"be_pdg_p3-7-supplements_nl_dv36o_yt_bid-dda_interest_18plus_supplements-photobomb_v1_video_None_15s_dog-owners_na_mars_nonskip (OPID-3924320)_Imp",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_own-goal_v1_video_None_20s_na_na_mars_nonskip (OPID-3926433)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp_CP_INT",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_lp-lal (OPID-3969375)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_na_health-conscious-health-concerns_v1_video_None_15s_na_na_mars_chvyheao50 (OPID-3975756)_Imp",
"be_pdg_p3-7-supplements_nl_dv36o_yt_bid-dda_interest_18plus_supplements-powerwash_v1_video_None_15s_dog-owners_na_mars_nonskip (OPID-3924266)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_cpremu50_super-premium-health-concerns_v1_video_None_6s_na_na_mars_na (OPID-3975751)_Imp",
"de_exg_p4-p6_de_dv36o_yt_bid-dda_interest_hvc_refresher-15-non-skip_v1_video_None_15s_na_na_mars_non-skip (OPID-3988935)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_na_health-conscious-health-concerns_v1_video_None_6s_na_na_mars_chvyheao50 (OPID-3975757)_Imp",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_lp-rtg (OPID-3969376)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_cpremu50_super-premium-every-day-life_v1_video_None_6s_na_na_mars_na (OPID-3975749)_Imp",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot_v1_video_None_15s_chvyo55_na_amazon_non-skip (OPID-3993500)_Imp",
"be_she_p5-hope_de_dv36o_yt_bid-dda_int-life_18plus_hope-15_v3_video_None_15s_na_na_mars_na (OPID-3993128)_Imp",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot_v1_video_None_15s_clte2840_na_amazon_non-skip (OPID-3993501)_Imp",
"be_pdg_p3-7-supplements_nl_dv36o_yt_bid-dda_interest_18plus_supplements-creadibility_v1_video_None_10s_dog-owners_na_mars_nonskip (OPID-3924263)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968740)_Imp",
"be_pdg_p3-7-supplements_fr_dv36o_yt_bid-dda_interest_18plus_supplements-creadibilty_v1_video_None_10s_dog-owners_na_mars_nonskip (OPID-3924253)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp_CP_INT",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_vv-lal (OPID-3969377)_Imp",
"be_pdg_p3-7-supplements_nl_dv36o_yt_bid-dda_interest_18plus_supplements-education_v1_video_None_10s_dog-owners_na_mars_nonskip (OPID-3924264)_Imp",
"at_org_p6-p7-refreshers_de_dv36o_yt_bid-nadr_demo-only_18-34_refreshers-tropical_v1_video_None_6s_na_na_mars_bumpers (OPID-3993482)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp_CP_INT",
"it_whi_p5-p6-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_what-it-takes_v1_video_None_15s_25-45_na_amazon_non-skip (OPID-3986577)_Imp",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_soccer_v1_video_None_6s_na_na_mars_bumper (OPID-3970356)_Imp",
"be_she_p5-hope_fr_dv36o_yt_bid-dda_int-life_18plus_hope-15_v1_video_None_15s_na_na_mars_na (OPID-3993126)_Imp",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot-short_v1_video_None_15s_cgenx4154_na_amazon_non-skip (OPID-3993495)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp_CP_INT",
"dk_sni_p6-unfiltered-girlfriend_da_dv36o_yt_bid-nadr_demo-only_18-44_football_v1_video_None_15s_na_na_social_non-skip (OPID-3991693)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp_CP_INT",
"it_ces_p5-p12-vnp-culinary_it_dv36o_yt_bid-dda_int-behav_na_culinary_v1_video_None_15s_gdfam01_na_amazon_nonskip (OPID-3975927)_Imp",
"be_she_p5-hope_de_dv36o_yt_bid-dda_int-life_18plus_hope-6_v4_video_None_6s_na_na_mars_na (OPID-3993129)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp_CP_INT",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_own-goal_v1_video_None_20s_na_na_mars_nonskip (OPID-3926433)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp_CP_INT",
"it_whi_p10-p11-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_inside-outside_v1_video_None_15s_45-64_na_amazon_non-skip (OPID-3986602)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_health-conscious-early-ownership_v1_video_None_15s_na_na_mars_na (OPID-3995025)_Imp",
"fi_sni_p6-unfiltered-girlfriend_fi_dv36o_yt_bid-nadr_demo-only_18-44_football_v1_video_None_15s_non-skip_na_social_na (OPID-3991708)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968728)_Imp",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_campus_v1_video_None_15s_na_na_mars_non-skip (OPID-3987711)_Imp",
"be_pdg_p3-7-supplements_fr_dv36o_yt_bid-dda_interest_18plus_supplements-photobomb_v1_video_None_15s_dog-owners_na_mars_nonskip (OPID-3924255)_Imp",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_voice-over_v1_video_None_20s_na_na_mars_nonskip (OPID-3973897)",
"de_sni_p5-6-vnp_de_dv36o_yt_bid-nadr_demo-only_18-44_voice-over_v1_video_None_20s_na_na_mars_nonskip (OPID-3973897)",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_millenial-health-concerns_v1_video_None_6s_na_na_mars_na (OPID-3995036)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_cpremu50_super-premium-early-ownership_v1_video_None_6s_na_na_mars_na (OPID-3975747)_Imp",
"it_ces_p5-p12-vnp-culinary_it_dv36o_yt_bid-dda_int-behav_na_culinary_v1_video_None_15s_gdlux03_na_amazon_nonskip (OPID-3975928)_Imp",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot_v1_video_None_15s_caculinary_na_amazon_non-skip (OPID-3993498)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp_CP_INT",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot-short_v1_video_None_15s_clte2840_na_amazon_non-skip (OPID-3993497)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp_CP_INT",
"be_she_p5-hope_fr_dv36o_yt_bid-dda_int-life_18plus_hope-6_v2_video_None_6s_na_na_mars_na (OPID-3993127)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp_CP_INT",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp_CP_INT",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_study_v1_video_None_15s_na_na_mars_non-skip (OPID-3987712)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp_CP_INT",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot_v1_video_None_15s_cgenx4154_na_amazon_non-skip (OPID-3993499)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_super-premium-health-concerns_v1_video_None_15s_na_na_mars_na (OPID-3995041)_Imp",
"it_whi_p5-p6-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_what-it-takes_v1_video_None_15s_new-kitten_na_amazon_non-skip (OPID-3986579)_Imp",
"es_org_p3-13-study_es_dv36o_yt_bid-adr_int-behav_18-24_commuting_v1_video_None_6s_reach-students_na_mars_na (OPID-3998533)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp_CP_INT",
"es_org_p3-13-study_es_dv36o_yt_bid-adr_int-behav_18-24_exam_v1_video_None_6s_reach-students_na_mars_na (OPID-3998535)_Imp",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_videocall_v1_video_None_15s_na_na_mars_non-skip (OPID-3987713)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_millenial-early-ownership_v1_video_None_15s_na_na_mars_na (OPID-3995031)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp",
"it_whi_p5-p6-vnp-playfullness_it_dv36o_yt_bid-dda_int-behav_co_what-it-takes_v1_video_None_15s_45-64_na_amazon_non-skip (OPID-3986578)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp_CP_INT",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_campus_v1_video_None_15s_na_na_mars_non-skip (OPID-3987711)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp_CP_INT",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_work_v1_video_None_15s_na_na_mars_non-skip (OPID-3987714)_Imp",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot-short_v1_video_None_15s_chvyo55_na_amazon_non-skip (OPID-3993496)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp_CP_INT",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp_CP_INT",
"es_org_p3-13-study_es_dv36o_yt_bid-adr_int-behav_18-24_commuting_v1_video_None_10s_reach-students_na_mars_na (OPID-3998532)_Imp",
"it_ces_p5-p12-vnp-culinary_it_dv36o_yt_bid-dda_int-behav_na_culinary_v1_video_None_15s_gdaff14_na_amazon_nonskip (OPID-3975926)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp_CP_INT",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp_CP_INT",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_super-premium-every-day-life_v1_video_None_6s_na_na_mars_na (OPID-3995040)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp_CP_INT",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp_CP_INT",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp_CP_INT",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp_CP_INT",
"de_wav_p5-p6_de_dv36o_yt_bid-adr_interest_hvc_study_v1_video_None_15s_na_na_mars_non-skip (OPID-3987712)_Imp",
"es_org_p3-13-study_es_dv36o_yt_bid-adr_int-behav_18-24_exam_v1_video_None_10s_reach-students_na_mars_na (OPID-3998534)_Imp",
"it_she_p6-12-pegasus_it_dv36o_yt_bid-dda_int-behav_ctb_robot-short_v1_video_None_15s_caculinary_na_amazon_non-skip (OPID-3993494)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp_CP_INT",
"be_she_p2-p11-caturday-night_nl_dv36o_yt_bid-dda_int-life_18plus_generic-tray_v5_video_None_6s_na_na_na_na (OPID-3970009)_Imp",
"be_she_p2-3-caturday-night_fr_dv36o_yt_bid-dda_int-life_18plus_generic-pouch_v1_video_None_6s_cat-owners-interest_na_na_bumper-adimo (OPID-3880187)_Imp",
"be_she_p2-3-caturday-night_nl_dv36o_yt_bid-dda_int-life_18plus_generic-tray_v1_video_None_6s_cat-owners-interest_na_na_bumper-adimo (OPID-3880192)_Imp",
"be_she_p2-p11-caturday-night_fr_dv36o_yt_bid-dda_int-life_18plus_generic-pouch_v1_video_None_6s_na_na_na_na (OPID-3970003)_Imp",
"be_she_p2-p11-caturday-night_fr_dv36o_yt_bid-dda_int-life_18plus_generic-tray_v2_video_None_6s_na_na_na_na (OPID-3970004)_Imp",
"es_mms_p1-13-meet-the-parents-minis_es_dv36o_yt_bid-dda_interest_25-54_minis_v1_video_None_15s_na_na_na_na (OPID-4003286)_Imp",
"be_she_p2-3-caturday-night_fr_dv36o_yt_bid-dda_int-life_18plus_taste-of-tokyo_v1_video_None_6s_cat-owners-interest_na_na_bumper-adimo (OPID-3880189)_Imp",
"be_she_p2-p11-caturday-night_fr_dv36o_yt_bid-dda_int-life_18plus_taste-of-tokyo_v3_video_None_6s_na_na_na_na (OPID-3970005)_Imp",
"be_she_p2-3-caturday-night_nl_dv36o_yt_bid-dda_int-life_18plus_taste-of-tokyo_v1_video_None_6s_cat-owners-interest_na_na_bumper-adimo (OPID-3880193)_Imp",
"be_pff_p6-p10-dry-and-wet_fr_dv36o_yt_bid-dda_int-behav_co_whole-body-health_v1_video_None_6s_cat-lovers-sport-fitness_na_mars_bumper (OPID-3959201)_Imp",
"be_she_p2-3-caturday-night_nl_dv36o_yt_bid-dda_int-life_18plus_generic-pouch_v1_video_None_6s_cat-owners-interest_na_na_bumper-adimo (OPID-3880191)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_millenial-every-day-life_v1_video_None_6s_na_na_mars_na (OPID-3995034)_Imp",
"be_she_p2-3-caturday-night_fr_dv36o_yt_bid-dda_int-life_18plus_generic-tray_v1_video_None_6s_cat-owners-interest_na_na_bumper-adimo (OPID-3880188)_Imp",
"be_she_p2-p11-caturday-night_nl_dv36o_yt_bid-dda_int-life_18plus_generic-pouch_v4_video_None_6s_na_na_na_na (OPID-3970008)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_health-conscious-health-concerns_v1_video_None_15s_na_na_mars_na (OPID-3995029)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_millenial-every-day-life_v1_video_None_15s_na_na_mars_na (OPID-3995033)_Imp",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_clncofco_millenial-health-concerns_v1_video_None_15s_na_na_mars_na (OPID-3995035)_Imp",
"be_she_p2-p11-caturday-night_nl_dv36o_yt_bid-dda_int-life_18plus_taste-of-tokyo_v6_video_None_6s_na_na_na_na (OPID-3970010)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp",
"TH-Shopee-Pedigree-MidMonth-10-18May-collection-image",
"TH-Shopee-Pedigree-MidMonth-10-18May-collection-image",
"TH-Shopee-Pedigree-MidMonth-10-18May-carousel-image",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"TH-Shopee-Pedigree-MidMonth-10-18May-carousel-image",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"TH-Shopee-Pedigree-MidMonth-10-18May-collection-image",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"TH-Shopee-Whiskas-10-18 May-carousel",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"TH-Shopee-Pedigree-MidMonth-10-18May-carousel-image",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"TH-Shopee-Pedigree-MidMonth-10-18May-collection-image",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"TH-Shopee-Whiskas-10-18 May-collection",
"TH-Shopee-Whiskas-10-18 May-collection",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"TH-Shopee-Whiskas-10-18 May-collection",
"TH-Shopee-Pedigree-MidMonth-10-18May-carousel-image",
"TH-Shopee-Whiskas-10-18 May-carousel",
"ç”¢å“æŽ¨å»£-åˆ©ç›Šé»ž(ç‹—æ–°ç´ æ)",
"TH-Shopee-Whiskas-10-18 May-collection",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"TH-Shopee-Whiskas-10-18 May-carousel",
"TH-Shopee-Pedigree-MidMonth-10-18May-collection-image",
"TH-Shopee-Whiskas-10-18 May-carousel",
"TH-Shopee-Pedigree-MidMonth-10-18May-carousel-image",
"TH-Shopee-Whiskas-10-18 May-collection",
"è©¦ç”¨æ–‡å¿ƒå¾—è“‹å¤§æ¨“(ç´ ææ”¶ç´ç¯‡)(ç‹—)",
"TH-Shopee-Pedigree-Petclub-19-23May-collection-image",
"TH-Shopee-Pedigree-Petclub-19-23-collection-vdo",
"TH-Shopee-Whiskas-Petclub-carousel_vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-image",
"TH-Shopee-Pedigree-Petclub-19-23May-collection-image",
"TH-Shopee-Whiskas-Petclub -collection_vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-image",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968737)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968738)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell2-bau (OPID-3968730)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968723)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cgenx41-54_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968725)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968727)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cheavyo55_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968729)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_cpome_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968735)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-zoo-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968741)_Imp",
"fr_she_p5-p6-laws-pegasus_fr_dv36o_gog_bid-dda_int-behav_vv-lalrtg_closeup-promo-bau-creative_v1_video_None_15s_engagement_shop-now_na_cell1-copilot (OPID-3968739)_Imp",
"5æœˆKOL(é¤µé£Ÿäº’å‹•)",
"5æœˆKOL(é¤µé£Ÿäº’å‹•)",
"5æœˆKOL(é¤µé£Ÿäº’å‹•)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dconsc_happiness_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991235)",
"gr_pdg_p6-7-dtx_el_dv36o_yt_bid-dda_int-behav_25-54_pedigree-min-meal-adult_v1_video_None_10s_in-market_na_mars_non-skip (OPID-3999673)_Imp",
"TH-Shopee-Pedigree-MidMonth-10-18May-collection-image",
"TH-Shopee-Whiskas-10-18 May-collection",
"be_pdg_p2-p10-megabrand_nl_dv36o_yt_bid-dda_int-behav_18plus_megabrand-adoption-alien_v2_video_None_15s_na_na_mars_p6 (OPID-4002539)_Imp",
"be_pdg_p2-p10-megabrands_fr_dv36o_yt_bid-dda_int-behav_18plus_megabrand-adoption-alien_v1_video_None_15s_na_na_mars_p6 (OPID-4002534)_Imp",
"TH-Shopee-Whiskas-Petclub -collection_vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-image",
"TH-Shopee-Whiskas-Petclub -collection_vdo",
"TH-Shopee-Whiskas-Petclub-collection",
"TH-Shopee-Whiskas-Petclub-carousel_vdo",
"TH-Shopee-Whiskas-Petclub-collection",
"TH-Shopee-Pedigree-Petclub-19-23-collection-vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-collection-image",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-image",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-image",
"TH-Shopee-Whiskas-Petclub-carousel_vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-collection-image",
"TH-Shopee-Whiskas-Petclub-carousel",
"TH-Shopee-Whiskas-Petclub-carousel",
"TH-Shopee-Whiskas-Petclub-carousel",
"TH-Shopee-Whiskas-Petclub-carousel",
"TH-Shopee-Pedigree-Petclub-19-23May-collection-image",
"TH-Shopee-Pedigree-Petclub-19-23-collection-vdo",
"TH-Shopee-Pedigree-Petclub-19-23-collection-vdo",
"TH-Shopee-Whiskas-Petclub-collection",
"TH-Shopee-Whiskas-Petclub -collection_vdo",
"TH-Shopee-Whiskas-Petclub-carousel_vdo",
"TH-Shopee-Whiskas-Petclub-carousel",
"TH-Shopee-Pedigree-Petclub-19-23-collection-vdo",
"be_pdg_p3-7-supplements_fr_dv36o_yt_bid-dda_interest_18plus_supplements-powerwash_v1_video_None_15s_dog-owners_na_mars_nonskip (OPID-3924256)_Imp",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-image",
"TH-Shopee-Whiskas-Petclub-carousel",
"TH-Shopee-Whiskas-Petclub-carousel_vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-vdo",
"TH-Shopee-Pedigree-Petclub-19-23May-collection-image",
"TH-Shopee-Whiskas-Petclub -collection_vdo",
"TH-Shopee-Whiskas-Petclub-collection",
"TH-Shopee-Whiskas-Petclub-carousel_vdo",
"TH-Shopee-Whiskas-Petclub -collection_vdo",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_cpremu50_super-premium-early-ownership_v1_video_None_15s_na_na_mars_na (OPID-3975746)_Imp",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_socialmillennial-everydaylife_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990948)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dconsc_proven_v1_video_1x1_15s_na_learn-more_mars_think (OPID-3991349)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_socialmillennial-earlyownership_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990947)",
"TH-Shopee-Pedigree-Petclub-19-23May-carousel-vdo",
"TH-Shopee-Whiskas-Petclub-collection",
"TH-Shopee-Pedigree-Petclub-19-23-collection-vdo",
"TH-Shopee-Whiskas-Petclub-collection",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_interest_clteheau50_version-a_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991845)",
"pl_pdg_multivitamins-p4-13_pl_meta_fbi_bid-dda_interest_devdlfemom_power-wash_v1_video_1x1_10s_see-p4-13_learn-more_mars_na (OPID-3925031)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_interest_chvyheao50_version-c_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991844)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dlteu50_plaque_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991251)",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_chvyo55 (OPID-3969373)_Imp",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_interest_clteheau50_everyday-life_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990873)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_interest_clteheau50_early-ownership_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990872)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_healthconscious-earlyownership_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990944)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_rtg_vvrtg_version-b_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991871)",
"pl_she_ao-content-p3-6_pl_meta_ig_bid-nadr_interest_clte2840_making-cat-happy_v1_video_1x1_10s_think-boosted-ig_learn-more_mars_na (OPID-3911855)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_rtg_vvrtg_version-c_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991872)",
"be_pdg_p3-7-supplements_fr_dv36o_yt_bid-dda_interest_18plus_supplements-think_v1_video_None_10s_dog-owners_na_mars_nonskip (OPID-3924257)_Imp",
"pl_she_ao-p3-p6_pl_meta_fcb_bid-nadr_interest_clte2840_luxury_v1_video_1x1_10s_think-p3-boosted-fb_learn-more_mars_na (OPID-3911859)",
"pl_pdg_multivitamins-p4-13_pl_meta_fbi_bid-dda_interest_daff_power-wash_v1_video_1x1_10s_see-p4-13_learn-more_mars_na (OPID-3925029)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_interest_chvyheao50_version-b_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991843)",
"pl_pdg_multivitamins-p4-13_pl_meta_fbi_bid-dda_interest_dconsc_photo-bomb_v1_video_1x1_10s_see-p4-13_learn-more_mars_na (OPID-3925032)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dconsc_thumbs_v1_video_1x1_15s_na_learn-more_mars_think (OPID-3991350)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dlteu50_gums_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991248)",
"fr_pdg_alien-p6_fr_meta_fbi_bid-dda_interest_dadop_no-cute-dogs_v1_video_1x1_10s_na_learn-more_mars_na (OPID-3991229)",
"de_pff_p5-11-wbh_de_dv36o_yt_bid-adr_interest_na_health-conscious-every-day-life_v1_video_None_15s_na_na_mars_chvyheao50 (OPID-3975754)_Imp",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_interest_chvyheao50_everyday-life_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990870)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_interest_chvyheao50_version-a_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991842)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_rtg_vvrtg_version-a_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991870)",
"pl_she_ao-content-p3-6_pl_meta_ig_bid-nadr_interest_clte2840_luxury_v1_video_1x1_10s_think-boosted-ig_learn-more_mars_na (OPID-3911854)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dlteu50_happiness_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991249)",
"de_she_p5-6-sheba-laws-pegasus_de_dv36o_yt_bid-dda_interest_na_alltheywant-olv-closeup_v1_video_None_15s_na_na_mars_clte2840 (OPID-3969374)_Imp",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-nadr_rtg_vvlalrtg_a24_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962664)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dconsc_cleaning_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991233)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_interest_clteheau50_version-b_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991846)",
"pl_pff_wbh-p6_pl_meta_fbi_bid-adr_interest_chvyheao50_health-concerns_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991415)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dlteu50_thumbs_v1_video_1x1_15s_na_learn-more_mars_think (OPID-3991354)",
"pl_pdg_multivitamins-p4-13_pl_meta_fbi_bid-dda_interest_devdlfemom_photo-bomb_v1_video_1x1_10s_see-p4-13_learn-more_mars_na (OPID-3925030)",
"pl_pdg_multivitamins-p4-13_pl_meta_fbi_bid-dda_interest_daff_photo-bomb_v1_video_1x1_10s_see-p4-13_learn-more_mars_na (OPID-3925028)",
"pl_pdg_multivitamins-p4-13_pl_meta_fbi_bid-dda_interest_dconsc_power-wash_v1_video_1x1_10s_see-p4-13_learn-more_mars_na (OPID-3925033)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_socialmillennial-healthconcerns_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990949)",
"pl_pff_total5-p6_pl_meta_fbi_bid-dda_interest_clteheau50_version-c_v1_static_1x1_na_do_shop-now_allegro_do (OPID-3991847)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dconsc_artificial-flavours_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991232)",
"pl_she_laws-vnp-p4_pl_meta_fbi_bid-dda_interest_chvyo55_a6_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3961749)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_lal_vvlal_thinkdo-tastymix18-40_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972289)",
"pl_she_laws-vnp-p4_pl_meta_fbi_bid-dda_interest_clte2840_a6_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3961750)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_interest_chvyheao50_health-concerns_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990871)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_healthconscious-everydaylife_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990945)",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-dda_interest_chvyo55_c21_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962662)",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-nadr_rtg_vvlalrtg_b29_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962667)",
"fr_pdg_alien-p6_fr_meta_fbi_bid-dda_interest_dadop_left-town_v1_video_1x1_10s_na_learn-more_mars_na (OPID-3991228)",
"pl_pff_wbh-p6_pl_meta_fbi_bid-adr_interest_chvyheao50_everyday-life_v1_video_1x1_6s_na_learn-more_mars_see (OPID-3991414)",
"fr_whi_playfulness-p5_fr_meta_fbi_bid-dda_lal_vvlal_thinkdo-corev1-18-40_v1_video_1x1_6s_na_learn-more_mars_na (OPID-3972288)",
"pl_she_laws-non-vnp-p4_pl_meta_fbi_bid-nadr_rtg_vvlalrtg_c21_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962668)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_superpremium-everydaylife_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990951)",
"pl_she_ao-p3-p6_pl_meta_fcb_bid-nadr_interest_clte2840_making-cat-happy_v1_video_1x1_10s_think-p3-boosted-fb_learn-more_mars_na (OPID-3911860)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_lal_vvlal_superpremium-earlyownership_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990950)",
"pl_she_ao-p3-p6_pl_meta_fcb_bid-dda_interest_clte2840_photoshoot_v1_video_1x1_na_think_learn-more_mars_fbi (OPID-3943749)",
"pl_she_ao-p3-p6_pl_meta_fcb_bid-dda_interest_clte2840_cat-love_v1_video_1x1_na_think_learn-more_mars_fbi (OPID-3943747)",
"fr_pff_wbh-p6_fr_meta_fbi_bid-dda_interest_chvyheao50_early-ownership_v1_video_1x1_6s_see_learn-more_mars_na (OPID-3990869)",
"pl_she_ao-p3-p6_pl_meta_ig_bid-dda_interest_cgenx4154_photoshoot_v1_video_1x1_0x0_think_learn-more_mars_fbi (OPID-3943744)",
"pl_she_laws-non-vnp-p4_pl_tt_tik_bid-nadr_lalrtg_vvlalrtg_a35_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962687)",
"pl_she_ao-p3-p6_pl_meta_ig_bid-dda_interest_clte2840_cat-love_v1_video_1x1_na_think_learn-more_mars_ig (OPID-3943748)",
"pl_she_ao-p3-p6_pl_meta_ig_bid-dda_interest_cgenx4154_photoshoot_v1_video_1x1_0x0_think_learn-more_mars_ig (OPID-3943745)",
"fr_pdg_dentastix-p6_fr_meta_fbi_bid-dda_interest_dlteu50_proven_v1_video_1x1_15s_na_learn-more_mars_think (OPID-3991353)",
"pl_she_laws-non-vnp-p4_pl_tt_tik_bid-nadr_lalrtg_vvlalrtg_b32_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962686)",
"it_whi_p6-feed-the-purr-o-clock_it_dv36o_yt_bid-dda_int-behav_na_ora-delle-fusa-inside-outside_v1_video_None_15s_chvyo50-55-64_na_amazon_wed-to-sat (OPID-3999593)_Imp",
"es_pdg_p5-6-multivitamins_es_dv36o_yt_bid-dda_interest_do_multivitamins-credibility_v1_video_None_15s_dog-lover_na_mars_nonskip (OPID-3990717)_Imp",
"it_whi_p6-feed-the-purr-o-clock_it_dv36o_yt_bid-dda_int-behav_na_ora-delle-fusa-inside-outside_v1_video_None_15s_clteu50-18-44_na_amazon_wed-to-sat (OPID-3999594)_Imp",
"pl_she_ao-p3-p6_pl_meta_ig_bid-dda_interest_clte2840_photoshoot_v1_video_1x1_na_think_learn-more_mars_ig (OPID-3943750)",
"pl_she_laws-non-vnp-p4_pl_tt_tik_bid-nadr_lalrtg_vvlalrtg_a34_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962684)",
"be_dre_p5-p6-chill-and-play_nl_dv36o_yt_bid-dda_int-behav_18plus_binge-watching_v1_video_None_14s_cat-lovers-added-adding-cat-to-household_na_mars_na (OPID-3961045)_Imp",
"gr_she_p6-11-all-they-want-vnp_el_dv36o_yt_bid-dda_int-behav_ca_robot_v1_video_None_15s_in-market_na_social_non-skip (OPID-3998432)_Imp",
"pl_she_laws-non-vnp-p4_pl_tt_tik_bid-nadr_lalrtg_vvlalrtg_a33_v1_video_1x1_10s_do_shop-now_mars_na (OPID-3962683)",
"es_whi_p6-8-feed-the-purr_es_dv36o_yt_bid-adr_interest_co_what-it-takes_v1_video_None_6s_18-44_na_amazon_bumpers (OPID-3988514)_Imp",
"es_whi_p6-8-feed-the-purr_es_dv36o_yt_bid-adr_interest_co_inside-outside_v1_video_None_15s_18-44_na_amazon_non-skip (OPID-3988512)_Imp",
"dk_pdg_p6-dtx_da_dv36o_yt_bid-adr_int-behav_hvc_dtx-credibility-plaque_v1_video_None_6s_25-64_na_mars_na (OPID-3908569)_Imp",
"es_whi_p6-8-feed-the-purr_es_dv36o_yt_bid-adr_interest_co_inside-outside_v1_video_None_15s_45-65_na_amazon_non-skip (OPID-3988513)_Imp",
"gr_pdg_p6-7-dtx_el_dv36o_yt_bid-dda_int-behav_25-54_pedigree-min-meal-adult_v1_video_None_6s_affinity_na_mars_bumpers (OPID-3999675)_Imp",
"gr_she_p6-11-all-they-want-vnp_el_dv36o_yt_bid-dda_int-behav_ca_robot_v1_video_None_15s_affinity_na_social_non-skip (OPID-3998431)_Imp",
"es_whi_p6-8-feed-the-purr_es_dv36o_yt_bid-adr_interest_co_what-it-takes_v1_video_None_6s_45-65_na_amazon_bumpers (OPID-3988515)_Imp",
"gr_pdg_p6-7-dtx_el_dv36o_yt_bid-dda_int-behav_25-54_pedigree-min-meal-adult_v1_video_None_6s_in-market_na_mars_bumpers (OPID-3999676)_Imp",
"gr_she_p6-11-all-they-want-vnp_el_dv36o_yt_bid-dda_int-behav_ca_robot_v1_video_None_15s_life-events_na_social_non-skip (OPID-3998433)_Imp",
"fi_she_p4-6-caturday-night_fi_dv36o_yt_bid-dda_int-behav_hvc_caturday-night-core-range_v1_video_None_10s_25-64-cat-lovers-added-cat-to-household_na_mars_nonskip (OPID-3925798)_Imp",
"gr_pdg_p6-7-dtx_el_dv36o_yt_bid-dda_int-behav_25-54_pedigree-min-meal-adult_v1_video_None_6s_life-events_na_mars_bumpers (OPID-3999677)_Imp",
"fi_she_p4-6-caturday-night_fi_dv36o_yt_bid-dda_int-behav_hvc_caturday-night-core-range_v1_video_None_6s_25-64-cat-lovers-added-cat-to-household_na_mars_bumper (OPID-3914628)_Imp",
"es_pdg_p5-6-multivitamins_es_dv36o_yt_bid-dda_interest_do_multivitamins-joint_v1_video_None_6s_dog-lover_na_mars_bumpers (OPID-3990723)_Imp",
"dk_pdg_p6-dtx_da_dv36o_yt_bid-adr_int-behav_hvc_dtx-credibility-artificials_v1_video_None_6s_25-64_na_mars_na (OPID-3908531)_Imp",
"it_whi_p6-feed-the-purr-o-clock_it_dv36o_yt_bid-dda_int-behav_na_ora-delle-fusa-inside-outside_v1_video_None_15s_cpome-18-54_na_amazon_sun-to-tue (OPID-3999592)_Imp",
"it_whi_p6-feed-the-purr-o-clock_it_dv36o_yt_bid-dda_int-behav_na_ora-delle-fusa-inside-outside_v1_video_None_15s_clteu50-18-44_na_amazon_sun-to-tue (OPID-3999591)_Imp",
"dk_pdg_p6-dtx_da_dv36o_yt_bid-adr_int-behav_hvc_dtx-credibility-cleaning_v1_video_None_6s_25-64_na_mars_na (OPID-3908567)_Imp",
"it_whi_p6-feed-the-purr-o-clock_it_dv36o_yt_bid-dda_int-behav_na_ora-delle-fusa-inside-outside_v1_video_None_15s_chvyo50-55-64_na_amazon_sun-to-tue (OPID-3999590)_Imp",
"es_pdg_p5-6-multivitamins_es_dv36o_yt_bid-dda_interest_do_multivitamins-range_v1_video_None_15s_dog-lover_na_mars_nonskip (OPID-3990720)_Imp",
"it_whi_p6-feed-the-purr-o-clock_it_dv36o_yt_bid-dda_int-behav_na_ora-delle-fusa-inside-outside_v1_video_None_15s_cpome-18-54_na_amazon_wed-to-sat (OPID-3999595)_Imp",
"es_pdg_p5-6-multivitamins_es_dv36o_yt_bid-dda_interest_do_multivitamins-education_v1_video_None_15s_dog-lover_na_mars_nonskip (OPID-3990718)_Imp",
"be_dre_p5-p6-chill-and-play_fr_dv36o_yt_bid-dda_int-behav_18plus_binge-watching_v1_video_None_14s_cat-lovers-added-adding-cat-to-household_na_mars_na (OPID-3961028)_Imp",
"dk_pdg_p6-dtx_da_dv36o_yt_bid-adr_int-behav_hvc_dtx-credibility-gums_v1_video_None_6s_25-64_na_mars_na (OPID-3908568)_Imp"]

# COMMAND ----------

creativeToBeFiltered = NorthAmericaOldCreativeList + RestOldCreativeList
dim_creative23_creative_platform_filtered = dim_creative23_platform_filtered.filter(~col('creative_desc').isin(creativeToBeFiltered))

# COMMAND ----------

creative_mismatch = dim_creative23_creative_platform_filtered.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
creative_mismatch.display()

# COMMAND ----------

# dim_creative23_platform_filtered.select('marketregion_code','country_desc','platform_desc','creative_desc','naming_convention_flag').display()
dim_creative23_creative_platform_filtered.select('marketregion_code','country_desc','platform_desc','creative_desc','naming_convention_flag').display()

# COMMAND ----------

if creative_mismatch.count() > 0:
    for each in creative_dict.keys():
        not_eval = []
        x = creative_mismatch.select(col(each)).distinct().collect()
        unique_values = [r[0] for r in x]   # prepoc([r[0]) for r in x]

        for i in unique_values:
            if i not in creative_dict[each]:
                not_eval.append(i)
        
        print(f"{each}: {not_eval}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Count the numbers of following vs not following

# COMMAND ----------

dim_campaign23_campaigns_filtered.groupby('marketregion_code')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# with filtered platforms
dim_creative23_creative_platform_filtered.groupby('marketregion_code')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

dim_mediabuy23_mediabuy_filtered.groupby('marketregion_code')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).withColumn('following %', round((col('following')/col('total_count'))*100,0)).withColumn('not_following %', round((col('not_following')/col('total_count'))*100,0)).display()

# COMMAND ----------

# dim_campaign23.filter(col("naming_convention_flag")==1).display()

# COMMAND ----------

# dim_creative23.filter(col("naming_convention_flag")==1).display()

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum
null_dim_campaign23 = dim_campaign23.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in dim_campaign23.columns])
null_dim_campaign23.display()

# COMMAND ----------

null_dim_creative23 = dim_creative23.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in dim_creative23.columns])
null_dim_creative23.display()

# COMMAND ----------

null_dim_mediabuy23 = dim_mediabuy23.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in dim_mediabuy23.columns])
null_dim_mediabuy23.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Count the numbers at region X platform level of following vs not following

# COMMAND ----------

# df1 = fact_performance.select('gpd_campaign_id','channel_id').dropDuplicates()
dim_campaign23_campaigns_filtered.groupby('marketregion_code','country_desc','platform_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# df2 = fact_performance.select('creative_id','channel_id').dropDuplicates() ---> we have created same df above
# with filtered platforms
dim_creative23_creative_platform_filtered.groupby('marketregion_code','country_desc','platform_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# df3 = fact_performance.select('media_buy_id','channel_id').dropDuplicates()
dim_mediabuy23_mediabuy_filtered.groupby('marketregion_code','country_desc','platform_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  4. Count the numbers at region X country level of following vs not following

# COMMAND ----------

dim_campaign23_campaigns_filtered.groupby('country_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# with filtered platforms
dim_creative23_creative_platform_filtered.groupby('country_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_mediabuy23_mediabuy_filtered.groupby('country_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summmary of filtered campaign/creative/mediabuy
# MAGIC

# COMMAND ----------

# dim_mediabuy23_mediabuy_filtered.filter(col('creative_desc').isin(RestOldCreativeList)).display()
# campaign_channel = fact_performance.select('gpd_campaign_id','channel_id').dropDuplicates()
dim_campaign23.filter(col('campaign_desc').isin(campaignsToBeFiltered)).join(campaign_channel, on = 'gpd_campaign_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').groupby('marketregion_code','country_desc','platform_desc').agg(count('campaign_desc').alias('campaign_count')).display()

# list of campaigns which are in access layer and filtered
dim_campaign23.filter(col('campaign_desc').isin(campaignsToBeFiltered)).join(campaign_channel, on = 'gpd_campaign_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').select('marketregion_code','country_desc','platform_desc','campaign_desc','naming_convention_flag').display()

# COMMAND ----------

dim_creative23_platform_filtered.filter(col('creative_desc').isin(creativeToBeFiltered)).groupby('marketregion_code','country_desc','platform_desc').agg(count('creative_desc').alias('creative_count')).display()

dim_creative23_platform_filtered.filter(col('creative_desc').isin(creativeToBeFiltered)).select('marketregion_code','country_desc','platform_desc','creative_desc','naming_convention_flag').display()

# COMMAND ----------

dim_mediabuy23.filter(col('media_buy_desc').isin(MediabuyToBeFiltered)).join(media_channel, on = 'media_buy_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').groupby('marketregion_code','country_desc','platform_desc').agg(count('media_buy_desc').alias('mediabuy_count')).display()

dim_mediabuy23.filter(col('media_buy_desc').isin(MediabuyToBeFiltered)).join(media_channel, on = 'media_buy_id',how = 'left').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'left').select('marketregion_code','country_desc','platform_desc', 'media_buy_desc','naming_convention_flag').display()
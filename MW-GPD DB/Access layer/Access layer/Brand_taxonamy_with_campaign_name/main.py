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

# Fact_performance
df_fact_performance_23_24 = df_fact_performance.withColumn("year", substring("date", 1, 4)).filter(col("year").isin(['2023','2024']))

all_col = df_fact_performance_23_24.join(dim_country.select("country_id","marketregion_code"), on = "country_id", how = "left")\
.join(dim_product.select("product_id","brand","product"), on = "product_id", how = "left")\
.join(dim_channel.select("channel_id","channel_desc","platform_desc"), on = "channel_id", how = "left")\
.join(dim_campaign.select("gpd_campaign_id","gpd_campaign_code","campaign_subproduct","media_channel",'naming_convention_flag'), on = "gpd_campaign_id" , how = "inner")\
.join(dim_creative.select("creative_id","creative_subproduct"), on = "creative_id", how= "left").select("year","marketregion_code","country_id","product","brand","channel_desc","platform_desc","gpd_campaign_id","gpd_campaign_code","campaign_desc","creative_subproduct","campaign_start_date","campaign_end_date","campaign_subproduct","media_channel",'naming_convention_flag' ).dropDuplicates().join(df_reach.select('gpd_campaign_id','in_platform_reach'), on = 'gpd_campaign_id', how = 'left')
# .join(dim_campaign.select("gpd_campaign_id","gpd_campaign_code"), on = "gpd_campaign_id" , how = "inner")
# nl_mal_core-p2_mw_eur_cho_brand_social_awareness_bid-nadr_0224_#160392

# COMMAND ----------


all_col['campaign_subproduct'] =  all_col['campaign_subproduct'].str.lower()

# COMMAND ----------

brand = pd.read_excel("brand.xlsx", 'new')
brand['Sub-Product Code'] = brand['Sub-Product Code'].str.lower()


# COMMAND ----------

# all_col[['campaign_subproduct','campaign_desc']]
subproduct_all = all_col['campaign_subproduct'].unique()
subproduct_code = brand['Sub-Product Code'].unique()
print(len(subproduct_all))
print(len(subproduct_code))
# uncommon values from both sets
uncomm = set(subproduct_code).difference(set(subproduct_all))
comm = set(subproduct_code).intersection(set(subproduct_all))
len(uncomm)

# COMMAND ----------

with open('variables.txt', 'w') as file:
    # Write the string to the file
    # file.write(uncomm)
    for value in subproduct_code:
        file.write(str(value) + '\n')

# COMMAND ----------

merged_df = pd.merge(all_col, brand, left_on='campaign_subproduct', right_on = 'Sub-Product Code',how = 'left')
merged_df.count

# COMMAND ----------

final = merged_df.copy()
final.loc[final['brand'] == final['Brand'], 'Match/NotMatch'] = 'Match'
final.loc[final['brand'] != final['Brand'], 'Match/NotMatch'] = 'Not Match'
final = final[['year','marketregion_code','country_id',"channel_desc","platform_desc",'media_channel','brand','Brand','Match/NotMatch','campaign_subproduct','campaign_desc','naming_convention_flag','in_platform_reach']]
final.columns = ['year','marketregion_code','country_id',"channel_desc","platform_desc",'media_channel','brand in access_layer','Brand_mapped','Match/NotMatch','campaign_subproduct','campaign_desc','naming_convention_flag','in_platform_reach']

final.to_csv('final.csv', index=False)

# COMMAND ----------

final_unmatched = merged_df[merged_df['brand'] != merged_df['Brand']][['year','marketregion_code','country_id','brand','Brand','campaign_subproduct','campaign_desc']]
final_unmatched.columns = ['year','marketregion_code','country_id','brand in access_layer','Brand_mapped','campaign_subproduct','campaign_desc']
final_unmatched.columns

# COMMAND ----------

final_unmatched.to_csv('final_unmatched.csv', index=False)

# COMMAND ----------

merged_df.to_csv('merged_df.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark codes
# MAGIC

# COMMAND ----------

all_col = all_col.withColumn('campaign_subproduct', lower(col('campaign_subproduct')))

# COMMAND ----------

# Modified
brand = pd.read_excel("brand.xlsx", 'new')
brand = spark.createDataFrame(brand)
brand = brand.withColumn('Sub-Product Code', lower(col('Sub-Product Code')))


# COMMAND ----------

# Modified
# all_col[['campaign_subproduct','campaign_desc']]
subproduct_all = [row['campaign_subproduct'] for row in all_col.select('campaign_subproduct').distinct().collect()]
subproduct_code = [row['Sub-Product Code'] for row in brand.select('Sub-Product Code').distinct().collect()]
print(len(subproduct_all))
print(len(subproduct_code))

# uncommon values from both sets
uncomm = set(subproduct_code).difference(set(subproduct_all))
comm = set(subproduct_code).intersection(set(subproduct_all))
len(uncomm)

# COMMAND ----------

# Modified
brand.count()

# COMMAND ----------

brand.count

# COMMAND ----------

# Modified
all_col.count()

# COMMAND ----------

all_col.count

# COMMAND ----------

print(all_col.columns)
print(brand.columns)

# COMMAND ----------

# Modified
merged_df = all_col.join(brand, all_col['campaign_subproduct'] == brand['Sub-Product Code'], how='left')
merged_df.count()

# COMMAND ----------

new_columns = ['gpd_campaign_id',
 'year',
 'marketregion_code',
 'country_id',
 'product',
 'brand_in_access_layer',
 'channel_desc',
 'platform_desc',
 'gpd_campaign_code',
 'campaign_desc',
 'creative_subproduct',
 'campaign_start_date',
 'campaign_end_date',
 'campaign_subproduct',
 'media_channel',
 'naming_convention_flag',
 'in_platform_reach',
 'Sub-Product',
 'Sub-Product Code',
 'Brand_mapped']

merged_df = merged_df.toDF(*new_columns)


# COMMAND ----------

merged_df.columns

# COMMAND ----------

# merged_df = merged_df.withColumnRenamed("brand", "brand_in_access_layer") \
#              .withColumnRenamed("Brand", "Brand_mapped")


# Modified 
merged_df = merged_df.withColumn(
    "Match/NotMatch", 
    when(merged_df.brand_in_access_layer == merged_df.Brand_mapped, "Match").otherwise("Not Match")
)

final = merged_df.select(
    "year", "marketregion_code",  "country_id",  "channel_desc",  "platform_desc",  "media_channel",  "brand_in_access_layer",  "Brand_mapped", "Match/NotMatch", "campaign_subproduct", "campaign_desc", "naming_convention_flag", "in_platform_reach"
)



final.write.csv('final.csv', header=True)

# COMMAND ----------

final.display()

# COMMAND ----------

# Modified
final_unmatched = merged_df.filter(col('brand') != col('Brand'))
final_unmatched = final_unmatched.select(
    "year", "marketregion_code", "country_id", "brand", "Brand", "campaign_subproduct", "campaign_desc"
)
final_unmatched = final_unmatched.withColumnRenamed("brand", "brand in access_layer") \
                                 .withColumnRenamed("Brand", "Brand_mapped")
final_unmatched.write.csv('final_unmatched.csv', header=True)

# COMMAND ----------

#Modified
merged_df.write.csv('merged_df.csv', header=True)

# COMMAND ----------

# Modified 
merged_df.count()

# COMMAND ----------

merged_df.count

# COMMAND ----------


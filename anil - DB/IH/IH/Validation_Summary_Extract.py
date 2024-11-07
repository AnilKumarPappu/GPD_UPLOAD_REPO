# Databricks notebook source
!pip install xlsxwriter

# COMMAND ----------

import os
from google.colab import drive
drive.mount('/content/drive')

# COMMAND ----------

!ls "/content/drive/MyDrive/APAC/all country "

# COMMAND ----------

folder = "/content/drive/MyDrive/APAC/all country "
countries = ('HK',  'ID',	'JP',  'KR',	'MY',  'NZ',	'PH',  'SG',	'TH',  'TW',	'VN', 'IN', 'AU', 'US', 'CA')


# COMMAND ----------

all_required_countries = ['USA']
#batch2_countries_files = [i for i in os.listdir(folder) if i.startswith(batch2_countries)]

# COMMAND ----------

all_required_countries

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# change file name

#writer = pd.ExcelWriter('GMD_Data_Validation_EMEA_Batch1.xlsx', engine = 'xlsxwriter')
writer = pd.ExcelWriter('GMD_Data_Summary', engine = 'xlsxwriter')

# COMMAND ----------



country = 'US'


# COMMAND ----------

os.listdir(f'/content/drive/MyDrive/APAC/all country /{country}')

# COMMAND ----------

#change the list of countries

# country_files = [i for i in batch1_countries_files if i.startswith(f"{country}_DATORAMA_ONLINE")]
country_files = [f'{folder}/{country}/{i}' for i in os.listdir(f'/content/drive/MyDrive/APAC/all country /{country}') if i.startswith('Copy of ' f"{country[:2]}_DATORAMA_ONLINE")]

# COMMAND ----------

country_files

# COMMAND ----------

df = None

for each in country_files:
  print(each)
  data = pd.read_csv(each, sep = '\t',skiprows=4)
  print(data.shape)
  if isinstance(df, pd.DataFrame):
    df = df.append(data)
  else:
    df = data

print(df.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manual upload from Local
# MAGIC

# COMMAND ----------

# Manual upload from Local
import pandas as pd

path = 'longterm_feb_extract_v3.xlsx'
df = pd.read_excel(path,skiprows=0)

# COMMAND ----------

df.head()

# COMMAND ----------

df.columns

# COMMAND ----------

df['Segment'].unique()

# COMMAND ----------

type(df[['Start Date',
       'End Date']]['Start Date'][0])

# COMMAND ----------

df['End Date'] = df['End Date'].dt.strftime('%Y%m%d')

# COMMAND ----------

df['Start Date'] = df['Start Date'].dt.strftime('%Y%m%d')

# COMMAND ----------

# Specify the file path and name
file_path = 'data.xlsx'

# Write the DataFrame to an Excel file
df.to_excel(file_path, index=False)


# COMMAND ----------

df = df[df['Segment'].isin(['Chocolate', 'Wrigley'])]

# COMMAND ----------

df2 = df.groupby(['Country',
 'Product',
 'Budget Year',
 'Plan Name',
 'Media type',
 'Start Date',
 'End Date']).agg({
    'Cost to Client USD': 'sum',
    'Cost to Client': 'sum'
})

# COMMAND ----------

df2.to_csv("Anth_granular_final.csv")

# COMMAND ----------

df2

# COMMAND ----------

# Specify the file path and name
file_path = 'data_agg.xlsx'

# Write the DataFrame to an Excel file
df2.to_excel(file_path, index=True)

# COMMAND ----------

df3 = df.groupby(['Country',
 'Budget Year']).agg({
    'Cost to Client USD': 'sum',
    'Cost to Client': 'sum'
})

# COMMAND ----------

df3.to_csv("Anth_Country*year.csv")

# COMMAND ----------

# Specify the file path and name
file_path = 'data_agg_country.xlsx'

# Write the DataFrame to an Excel file
df3.to_excel(file_path, index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SOurce_IH

# COMMAND ----------

# Manual upload from Local
import pandas as pd

path = 'Mars_MostCurrentData_by Year_21.02.22 (Local Currency)_working.xlsx'
source_IH= pd.read_excel(path,skiprows=0)

# COMMAND ----------

source_IH['Segment'].unique()

# COMMAND ----------

source_IH['Budget Year'].unique()

# COMMAND ----------

source_IH = source_IH[source_IH['Segment'].isin(['Chocolate', 'Wrigley'])]

# COMMAND ----------

source_IH['Start Date'] = source_IH['Start Date'].dt.strftime('%Y%m%d')
source_IH['End Date'] = source_IH['End Date'].dt.strftime('%Y%m%d')

# COMMAND ----------

source_IH_agg = source_IH.groupby(['Country',
 'Product',
 'Budget Year',
 'Plan Name',
 'Media type',
 'Start Date',
 'End Date']).agg({
    'Cost to Client': 'sum'
})

# COMMAND ----------

source_IH_agg.to_csv("source_IH_agg.csv")

# COMMAND ----------

source_IH_agg.count()

# COMMAND ----------

source_IH_agg_removed_date = source_IH[source_IH['Budget Year'].isin([2019,2020,2021,2022])].groupby(['Country',
 'Product',
 'Budget Year',
 'Plan Name',
 'Media type']).agg({
    'Cost to Client': 'sum'
})

# COMMAND ----------

source_IH_agg_removed_date.count()

# COMMAND ----------

source_IH_agg_removed_date.to_csv('source_start_end_not_aggregated.csv')

# COMMAND ----------

source_IH[source_IH['Budget Year'].isin(['2019','2020','2021','2022'])].count()

# COMMAND ----------

source_IH_country_year = source_IH[source_IH['Budget Year'].isin([2019,2020,2021,2022])].groupby(['Country',
 'Budget Year']).agg({
    'Cost to Client': 'sum'
})

# COMMAND ----------

source_IH_country_year.count()

# COMMAND ----------

source_IH_country_year.to_csv('source_IH_country_year.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation_Summary_Extract

# COMMAND ----------

segment = pd.read_excel('GMD_ Access_Layer_Data_Understanding_17_11_22.xlsx',sheet_name='Segment - Brand Mapping', skiprows=2)
segment = segment[segment.columns[1:]]

# COMMAND ----------

df.head()

# COMMAND ----------



# COMMAND ----------

df['brand'].unique()

# COMMAND ----------

df.loc[df['brand']== 'Dove','brand'] = 'Dove Galaxy'

# COMMAND ----------

segment[segment['Brand'] == 'Dove Galaxy'].head()

# COMMAND ----------

segment['Brand'].unique()

# COMMAND ----------

df.shape, df.drop_duplicates().shape

# COMMAND ----------

df.drop_duplicates(inplace = True)

# COMMAND ----------

df.shape

# COMMAND ----------

df.columns

# COMMAND ----------

# pdf = df[['Brand (Final) [Brand Classification Rule]','Main Channel Category ','Source Platform','Campaign Name','Clicks',
#     'Impressions','Video Views','Digital Media Spend (Original)','Video Fully Played']]

pdf = df[['country_id', 'brand', 'channel', 'platform', 'campaign_name',
       'clicks', 'impressions', 'views', 'actual_spend_local_currency',
       'video_fully_played', 'U.S. Spend', 'U.S. Impressions', 'U.S. Clicks',
       'U.S. Video Starts', 'U.S. Video Completes']]

# COMMAND ----------

pdf = pd.merge(pdf, segment, how = 'left', left_on='brand', right_on='Brand')
pdf.drop('Brand', axis=1, inplace=True)

# COMMAND ----------

pdf

# COMMAND ----------

pdf.shape

# COMMAND ----------

pdf = pdf.rename(columns={'Brand (Final) [Brand Classification Rule]': 'brand',
                        'Main Channel Category ': 'channel',
                        'Source Platform': 'platform',
                        'Campaign Name': 'campaign_name',
                        'Clicks': 'clicks',
                        'Impressions': 'impressions',
                        'Video Views': 'views',
                        'Digital Media Spend (Original)': 'actual_spend_local_currency',
                        'Video Fully Played': 'video_fully_played',
                        'Segment': 'cell'
                       })

# COMMAND ----------

pdf = pdf.rename(columns={'Segment': 'cell'})

# COMMAND ----------

pdf["clicks"] = pd.to_numeric(pdf["clicks"])
pdf["impressions"] = pd.to_numeric(pdf["impressions"])
pdf["views"] = pd.to_numeric(pdf["views"])

# COMMAND ----------

pdf.insert(0, 'country_id', country)

# COMMAND ----------

pdf.head()

# COMMAND ----------

pdf.shape

# COMMAND ----------

object_list = list(pdf.select_dtypes(['object','datetime64']).columns)
pdf[object_list] = pdf[object_list].fillna('NA')
numeric_list = list(pdf.select_dtypes(['float']).columns)
pdf[numeric_list] = pdf[numeric_list].fillna(0)

# COMMAND ----------

pdf.head()

# COMMAND ----------

pdf = pdf[pdf['platform'] != 'Campaign Manager']
pdf.shape

# COMMAND ----------

pdf['campaign_name'].nunique()

# COMMAND ----------

final = pdf.groupby(['country_id','cell','brand','channel','platform','campaign_name'])['clicks','impressions','views','actual_spend_local_currency','video_fully_played','U.S. Spend', 'U.S. Impressions', 'U.S. Clicks',
       'U.S. Video Starts', 'U.S. Video Completes'].sum().reset_index()

# COMMAND ----------

final

# COMMAND ----------

final.groupby(['campaign_name','platform'])['campaign_name'].count()

# COMMAND ----------

final.shape

# COMMAND ----------

#final.to_excel(writer, sheet_name = f'Performance Data {country}.xlsx', index=False)

# COMMAND ----------

final.to_excel(f'Performance Data {country}.xlsx', index=False)

# COMMAND ----------

# change the market

country_reach = [f'{folder}/{country}/{i}' for i in os.listdir(f'/content/drive/MyDrive/APAC/all country /{country}') if i.startswith(f"{country[:2]}_DATORAMA_REACH")]

# COMMAND ----------

# CA Reach is in US-2 file (Loading CA Reach file)
country_id = 'CA'
country_reach = [f'{folder}/{country}/{i}' for i in os.listdir(f'/content/drive/MyDrive/APAC/all country /{country}') if i.startswith(f"{'CA'}_DATORAMA_REACH")]

# COMMAND ----------

country_reach

# COMMAND ----------

rd = pd.read_csv(country_reach[0], sep = '\t',skiprows=4)


# COMMAND ----------

import pandas as pd

# COMMAND ----------

#rd = pd.read_excel('/content/HK_DATORAMA_REACH_MW_2023-04-25.xls')

# COMMAND ----------

rdf = rd[['Brand (Final) [Brand Classification Rule]','Source Platform','Campaign Name','Reach']]
rdf.head()

# COMMAND ----------

rdf.shape

# COMMAND ----------

rdf = pd.merge(rdf, segment, how = 'left', left_on='Brand (Final) [Brand Classification Rule]', right_on='Brand')
rdf.drop('Brand', axis=1, inplace=True)

# COMMAND ----------

rdf = rdf.rename(columns={'Brand (Final) [Brand Classification Rule]': 'brand',
                        'Source Platform': 'platform',
                        'Campaign Name': 'campaign_name',
                        'Segment': 'cell',
                        'Reach' : 'in_platform_reach'
                       })

# COMMAND ----------

rdf.insert(0, 'country_id', country)

# COMMAND ----------

cols = list(rdf.columns)
lc = cols.pop()
cols.insert(2,lc)

# COMMAND ----------

rdf =rdf.reindex(columns=cols)

# COMMAND ----------

rdf.head()

# COMMAND ----------

#rdf.to_excel(writer, sheet_name = f'Reach Data {country}.xlsx', index=False)

# COMMAND ----------

rdf.to_excel(f'Reach Data {country}.xlsx', index=False)

# COMMAND ----------



# COMMAND ----------


# Databricks notebook source
import pandas as pd

path = "October synthesis data.xlsx"
df = pd.read_excel(path)


# COMMAND ----------

df.shape

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

df = df[[
'Country',
'Budget Year',
'Segment',
'Brand',
'Product',
'Plan Name',

'Media',
'Media type',
    
'Start Date',
'End Date',
  
'Cost to Client',
'Cost to Client USD',
'Conversion Rate',
'Flight ID',
'Mars Fiscal Year'
  

]]

# COMMAND ----------

df.shape

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

print(df['Start Date'][0], df['Start Date'][len(df)-1])

# COMMAND ----------

df = df[df['Start Date'] > '2019-01-01']

# COMMAND ----------

df.shape

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

df.columns

# COMMAND ----------

df['Segment'].unique()

# COMMAND ----------


#df = df[(df['Segment'] == 'Chocolate') | (df['Segment'] == 'Wrigley')]

# COMMAND ----------

#df['Segment'].unique()

# COMMAND ----------

# ooh_type = []
# for i in df['Media type'].unique():
#     if 'OOH' in i:
#         ooh_type.append(i)

# COMMAND ----------

# ooh_type

# COMMAND ----------

# for i in ooh_type:
#     df = df[ df['Media type'] != i]

# COMMAND ----------

df['Media type'].unique()

# COMMAND ----------

df['Media'].unique()

# COMMAND ----------

#df = df[df['Media'] != 'OOH']

# COMMAND ----------

df["Media"].unique()

# COMMAND ----------

df['Brand'].unique()

# COMMAND ----------

df.shape

# COMMAND ----------

df = df.drop_duplicates()

# COMMAND ----------

df.shape

# COMMAND ----------

import pandas as pd

# Assuming df is your original DataFrame
#'country_id', 'budget_year', 'brand', 'segment', 'plan_name', 'media', 'media_type'

aggregation_functions = {
    
    'Cost to Client': 'sum',
    'Cost to Client USD': 'sum'
}

new_df = df.groupby(['Country', 'Budget Year', 'Brand', 'Plan Name',
                     'Media', 'Media type', 'Start Date','End Date' ]).agg(aggregation_functions).reset_index()


# COMMAND ----------

import pandas as pd

# Assuming df is your original DataFrame
#'country_id', 'budget_year', 'brand', 'segment', 'plan_name', 'media', 'media_type'

aggregation_functions = {
    
    'Cost to Client': 'sum',
    'Cost to Client USD': 'sum'
}

check_df = df[df['Start Date'] < '2023-07-01'].groupby(['Country', 'Budget Year',
                         'Brand', 'Plan Name',
                     ]).agg(aggregation_functions).reset_index()


# COMMAND ----------

check_df.shape

# COMMAND ----------

check_df.head()

# COMMAND ----------

check_df.to_csv('september_data.csv')

# COMMAND ----------

# Aggregating country wise cost to client in USD and Local data
aggregation_functions = {
    
    'Cost to Client': 'sum',
    'Cost to Client USD': 'sum'
}

country_wise_spend = new_df.groupby(['Country']).agg(aggregation_functions).reset_index()


# COMMAND ----------

new_df.shape

# COMMAND ----------

country_wise_spend.head()

# COMMAND ----------

country_wise_spend['Local/USD'] = country_wise_spend['Cost to Client']/country_wise_spend['Cost to Client USD']

# COMMAND ----------

country_wise_spend.head()

# COMMAND ----------

country_wise_spend.to_csv('country_wise_spend.csv')

# COMMAND ----------

new_df.isnull().sum()

# COMMAND ----------

new_df

# COMMAND ----------

new_df.drop_duplicates().shape

# COMMAND ----------

start_date = new_df['Start Date'].to_list()
end_date = new_df['End Date'].to_list()

# COMMAND ----------

start = []
end = []
for i in range(len(new_df)):
    s_d = str(start_date[i]).split('-')
    e_d = str(end_date[i]).split('-')
    start.append(s_d[0] + s_d[1] + s_d[2][:2])
    end.append(e_d[0] + e_d[1] + e_d[2][:2])

new_df['Start Date'] = start
new_df['End Date'] = end


# COMMAND ----------

new_df.head()

# COMMAND ----------

new_df.to_csv('Synthesis_extract.csv')

# COMMAND ----------

new_df['Country'].unique()

# COMMAND ----------

new_df['Country'].nunique()

# COMMAND ----------

access_layer = [
'AE',
'AL',
'AM',
'AR',
'AT',
'AZ',
'BR',
'BA',
'BG',
'AU',
'BY',
'TR',
'CA',
'CH',
'CL',
'CN',
'CR',
'FI',
'CZ',
'DE',
'DK',
'EC',
'DO',
'SK',
'EE',
'EG',
'ZA',
'ES',
'FR',
'GB',
'GR',
'CO',
'GT',
'HR',
'HU',
'ID',
'HK',
'IE',
'IL',
'JP',
'KE',
'IT',
'LT',
'KZ',
'LB',
'MX',
'LV',
'MK',
'ME',
'MY',
'NO',
'NL',
'NZ',
'PA',
'PE',
'PH',
'PR',
'PL',
'PT',
'RS',
'RO',
'TH',
'RU',
'SA',
'SE',
'UA',
'UG',
'TW',
'XK',
'VN',
'KR',
'SI',
'US',
'BE',
'GE',
'IN',
'UZ',

]

# COMMAND ----------

extract = [
    
'AL',
'AR',
'AU',
'AT',
'AZ',
'BE',
'BG',
#N/A,
'BR',
'BY',
'CA',
'CH',
'CL',
'CN',
'CO',
'CR',
'CZ',
'DE',
'DK',
'DO',
'EC',
'EE',
'EG',
'ES',
'FI',
'FR',
'GB',
'GE',
'GR',
'GT',
'HK',
'HR',
'HU',
'ID',
'IE',
'IL',
'IN',
'IT',
'JP',
'KE',
'KR',
'KZ',
'LT',
'LV',
#N/A,
'ME',
'MK',
'MX',
'MY',
'NL',
'NO',
'NZ',
'PA',
'PE',
'PH',
'PL',
'PR',
'PT',
'RO',
'RS',
'RU',
'SE',
'SG',
'SK',
'SI',
'TH',
'TR',
'TW',
'UA',
'UG',
'XK',
'ZA',
#N/A
#N/A
#N/A
#N/A
#N/A
'UY',
'UZ',
'VN'


]

# COMMAND ----------

set(access_layer) & set(extract)

# COMMAND ----------

len(set(access_layer) & set(extract))

# COMMAND ----------

len(access_layer)

# COMMAND ----------

len(extract)

# COMMAND ----------

set(access_layer) ^ set(extract)
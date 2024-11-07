# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### To access Synapse views

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
# MAGIC ### To access ADLS file system

# COMMAND ----------

common_path = "abfss://output@marsanalyticsprdadls01.dfs.core.windows.net/GLOBAL_XSEG_SRM_DATA_FOUNDATION/DATA_ACCESS/MW_GPD/"

# COMMAND ----------

df_fact_performance = spark.read.load(common_path + "mw_gpd_fact_performance")
df_reach = spark.read.load(common_path + "mw_gpd_fact_reach")
df_planned = spark.read.load(common_path + "mw_gpd_fact_plannedspend")
dim_campaign = spark.read.load(common_path + "mw_gpd_dim_campaign")
dim_channel = spark.read.load(common_path + "mw_gpd_dim_channel")
dim_creative = spark.read.load(common_path + "mw_gpd_dim_creative")
dim_strategy = spark.read.load(common_path + "mw_gpd_dim_strategy")
dim_mediabuy = spark.read.load(common_path + "mw_gpd_dim_mediabuy")
dim_site = spark.read.load(common_path + "mw_gpd_dim_site")
dim_country = spark.read.load(common_path + "mw_gpd_dim_country")
dim_product = spark.read.load(common_path + "mw_gpd_dim_product")
# dim_calender= spark.read.load(common_path + "vw_dim_calendar")

ih_spend = spark.read.load(common_path + "mw_gpd_fact_ih_spend")
ih_ingoing = spark.read.load(common_path + "mw_gpd_fact_ih_ingoingspend")
df_fact_creativetest = spark.read.load(common_path + "mw_gpd_fact_creativetest")
df_fact_som = spark.read.load(common_path + "mw_gpd_fact_som")


# COMMAND ----------

# MAGIC %md
# MAGIC ### To see the files in ADLS 

# COMMAND ----------

dbutils.fs.ls('abfss://output@marsanalyticsprdadls01.dfs.core.windows.net/GLOBAL_XSEG_SRM_DATA_FOUNDATION/DATA_ACCESS/MW_GPD/')


# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.secrets.list("globalxsegsrmdatafoundationprodsecretscope")

# COMMAND ----------

dbutils.fs.ls('abfss://output@marsanalyticsprdadls01.dfs.core.windows.net/')


# COMMAND ----------

# MAGIC %md
# MAGIC Access  file from blob storage

# COMMAND ----------

sas_token = "sp=racwdl&st=2024-07-08T11:37:49Z&se=2025-07-08T19:37:49Z&spr=https&sv=2022-11-02&sr=c&sig=9AjQXwTopETfuc0kc7basq4N5zTA7ICpf9uJIcejOF0%3D"
storage_account_name = "marsanalyticsprdadls01"
container_name = "report-reference-files"

# Mount the Blob storage using SAS token
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = f"/mnt/{container_name}",
  extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)


# COMMAND ----------

pip install openpyxl

# COMMAND ----------

files = dbutils.fs.ls(f"/mnt/report-reference-files")

# Display the file paths
for file in files:
    print(file.path)

# COMMAND ----------

df = spark.read.csv(f"/mnt/report-reference-files/data team testing", header=True, inferSchema=True)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC trash

# COMMAND ----------

# Copy the file from Blob Storage to DBFS
dbutils.fs.cp(f"/mnt/report-reference-files/Dim_Currency_IH.xlsx", "/tmp/local_excel_file.xlsx")


# COMMAND ----------

import pandas as pd

# Define the file path (mounted path)
xlsx_file_path = "/mnt/report-reference-files/Dim_Currency_IH.xlsx"

# Read the Excel file using pandas
excel_data = pd.read_excel(xlsx_file_path, engine='openpyxl')

# Convert the pandas DataFrame to a PySpark DataFrame
df = spark.createDataFrame(excel_data)

# Show the DataFrame
df.show()


# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### To access ADLS file system

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### To see the files in ADLS 

# COMMAND ----------

dbutils.fs.ls('abfss://output@demandlakeeus2devsa.dfs.core.windows.net/CLINCH/exploitation/dpm_clinch/global/')

# COMMAND ----------

dbutils.fs.ls('abfss://output@demandlakeeus2devsa.dfs.core.windows.net/CLINCH/exploitation/dpm_clinch/global/')

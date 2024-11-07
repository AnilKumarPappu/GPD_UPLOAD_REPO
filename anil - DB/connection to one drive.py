# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.fs.mount(source = "https://mydrive.effem.com", mount_point = "/mnt/onedrive", extra_configs = {"client_id": "anilkumar.pappu@effem.com", "client_secret": "Anil@porsche", "tenant_id": "2fc13e34-f03f-498b-982a-7cb446e25bc6"})


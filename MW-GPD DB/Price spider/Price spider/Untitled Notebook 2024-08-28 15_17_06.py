# Databricks notebook source
https://mydrive.effem.com/:x:/r/personal/anilkumar_pappu_effem_com/Documents/GPD/PriceSpider/WTB_SALES_extraction..xlsx?d=webc2f964142f4eec8951d1d70e7e0d29&csf=1&web=1&e=fC1xWS

# COMMAND ----------

import pandas as pd
import requests
from io import BytesIO

# Replace the below URL with your direct download link
onedrive_url = "https://mydrive.effem.com/:x:/r/personal/anilkumar_pappu_effem_com/Documents/GPD/PriceSpider/WTB_SALES_extraction..xlsx?download=1"

# Get the content from the OneDrive link
response = requests.get(onedrive_url)

# Check if the request was successful
if response.status_code == 200:
    # Use BytesIO to read the content as an Excel file
    excel_file = BytesIO(response.content)
    
    # Read the Excel file into a DataFrame
    df = pd.read_excel(excel_file, sheet_name=None)  # Use sheet_name=None to read all sheets
    
    # Display the DataFrame
    display(df)
else:
    print("Failed to download the file.")

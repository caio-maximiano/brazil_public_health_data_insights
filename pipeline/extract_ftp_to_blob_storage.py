# Databricks notebook source
from pyspark.sql.functions import *
from dateutil.relativedelta import *
from pyspark.sql.functions import regexp_replace
from dateutil.relativedelta import *
import datetime
from include.data_downloader import DataDownloader
from include.azure_blob_storage_handler import AzureBlobStorageHandler 


# COMMAND ----------

use_date = datetime.datetime.now() + relativedelta(months=-1)
ano_mes = use_date.strftime("%Y%m")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pulling Raw Data
# MAGIC  - Class DataDownloader to download data from FTP server into local tmp folder
# MAGIC  - Class AzureBlobStorageHandler to save downloaded file into storage account

# COMMAND ----------

# # Initialize downloader 
ftp_url = f"ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202304.ZIP"
local_download_path = "/tmp/"

downloader = DataDownloader()
downloaded_file = downloader.download_data(ftp_url,local_download_path)

# Initialize storage handler
azure_connection_string = dbutils.secrets.get("dev-secret-scope", "caiostorageaccountdev-connection-string")
container_name = "bronze"

storage_handler = AzureBlobStorageHandler(azure_connection_string, container_name)
if downloaded_file:
    storage_handler.save(downloaded_file, "sus_data/zip_files/BASE_DE_DADOS_CNES_202304.ZIP")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extracting CSV Files
# MAGIC  - With method extract_zip_in_blob() its possible to extract the csv files to another folder

# COMMAND ----------

storage_handler.extract_zip_in_blob("sus_data/zip_files/BASE_DE_DADOS_CNES_202304.ZIP", "sus_data/csv_files/202304")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading and turning names into snake case

# COMMAND ----------

import re  # Import Python's built-in regular expression library

# Reading the CSV file with specified options
df = (spark.read.option("header", "true")
                .option("delimiter", ";")
                .csv("abfss://bronze@caiostorageaccountdev.dfs.core.windows.net/dados_sus/202304/tbDadosProfissionalSus202304.csv"))

# Function to clean and convert column names to snake_case
def to_snake_case(name):
    # Remove unwanted characters
    cleaned_name = re.sub(r"[^a-zA-Z0-9.\-_ ]+", "", name)
    # Replace spaces and periods with underscores and convert to lower case
    snake_case_name = re.sub(r"[\s.]+", "_", cleaned_name).lower()
    return snake_case_name

# Renaming columns
for name in df.columns:
    snake_case_name = to_snake_case(name)
    df = df.withColumnRenamed(name, snake_case_name)

# Display the DataFrame with renamed columns in snake_case
df.display()


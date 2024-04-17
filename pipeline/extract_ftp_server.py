# Databricks notebook source
# MAGIC %md
# MAGIC ## Extraction
# MAGIC  - Brazilian Healt Data resides in a FTP server maintained by the the government

# COMMAND ----------

# MAGIC %sh
# MAGIC rm ../../../../../../../temp

# COMMAND ----------

import wget

wget.download("ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202304.ZIP", "./temp/",bar="bar_adaptive")

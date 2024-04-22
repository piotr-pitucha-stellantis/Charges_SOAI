# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re
import  toolbox_connected_vehicle as tcv
import pandas.testing as pd_testing
import datetime
# import openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC # Extraction datalab v2

# COMMAND ----------

vin = ['VR1J45GBULY057165']

message_list = [79]

# COMMAND ----------

for message in message_list:
    # one message_type only (54, 56, 57 ...)
    message_type = message
    start_date = "2022-01-01"
    end_date = "2022-02-01"
    # True for cleaned data, False for raw data
    preprocessing = True
    # Optionally a list of vin can be specified
    list_vin = vin
    # carcom or carbide or None for both
    data_collect = 'carbide' 

    tcv_df = tcv.read(
        spark, message_type, start_date, end_date, preprocessing, list_vin, data_collect
    )
    print("TCV extracted for message ", message)
    data_path_extraction = f"s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_1_VIN_midnight/Bloc_{message}"
    tcv_df.write.mode('Overwrite').parquet(data_path_extraction)
    print("TCV wrote for message ", message)

# COMMAND ----------

dfv2_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_1_VIN_midnight/Bloc_79")
                    )

# COMMAND ----------

display(dfv2_spk)

# COMMAND ----------



# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re
import  toolbox_connected_vehicle as tcv

# import openpyxl

# COMMAND ----------

#message_list = [54,74,77,79]
message_list = [68]

# COMMAND ----------

for message in message_list:
    # one message_type only (54, 56, 57 ...)
    message_type = message
    start_date = "2022-01-01"
    end_date = "2022-02-01"
    # True for cleaned data, False for raw data
    preprocessing = True
    # Optionally a list of vin can be specified
    list_vin = ['VR3F4DGZTMY505590']
    # carcom or carbide or None for both
    data_collect = 'carbide' 

    df_1_vin_true = tcv.read(
        spark, message_type, start_date, end_date, preprocessing, list_vin, data_collect
    )


# COMMAND ----------

for message in message_list:
    # one message_type only (54, 56, 57 ...)
    message_type = message
    start_date = "2022-01-01"
    end_date = "2022-02-01"
    # True for cleaned data, False for raw data
    preprocessing = False
    # Optionally a list of vin can be specified
    list_vin = ['VR3F4DGZTMY505590']
    # carcom or carbide or None for both
    data_collect = 'carbide' 

    df_1_vin_false = tcv.read(
        spark, message_type, start_date, end_date, preprocessing, list_vin, data_collect
    )


# COMMAND ----------

display(df_1_vin_true.select('HEAD_COLL_TIMS', 'HEAD_VIN').orderBy('HEAD_COLL_TIMS'))

# COMMAND ----------

display(df_1_vin_false.select('HEAD_COLL_TIMS', 'HEAD_VIN').orderBy('HEAD_COLL_TIMS'))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



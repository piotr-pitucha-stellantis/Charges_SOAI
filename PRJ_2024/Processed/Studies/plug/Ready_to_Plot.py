# Databricks notebook source
# MAGIC %md
# MAGIC # Start widgets

# COMMAND ----------

# Create a text widget for the userid definition
dbutils.widgets.text('UserID', '')
# dbutils.widgets.remove('path') # to remove the widget

# Get the input value
userid = dbutils.widgets.get('UserID')



# COMMAND ----------

# Get the input value
userid = dbutils.widgets.get('UserID')
print(userid)

# COMMAND ----------

# widget data path 
dbutils.widgets.text('Charge_Data_Path_S3', '')

# COMMAND ----------

print('Charge_Data_Path_S3 :', dbutils.widgets.get('Charge_Data_Path_S3'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import re
import  toolbox_connected_vehicle as tcv
import numpy as np
import datetime as dt
import pytz
import pandas as pd
import shutil
#import osy
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stats
import seaborn as sns
from datetime import timedelta
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.dates as mdates


# import openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC # Ready to plot datalabv2

# COMMAND ----------

df_plug = (spark.read
  .format("parquet")
  .option("header",True)
  .options(delimiter=';')
  .load(dbutils.widgets.get('Charge_Data_Path_S3') + "data/Processed/levdata/plugs/2022/01")
)

# COMMAND ----------

display(df_plug.filter((F.col('HEAVIN') == "VR3F4DGZTMY505590") & ((F.col('TSstart') < "2022-01-09 00:00:00") | (F.col('TSstop') < "2022-01-09 00:00:00"))).distinct())

# COMMAND ----------

df_spark_start = df_plug.select("TSstart","HEAVIN",'SEIDstart','HMSGstart').withColumn("EVNT", F.lit("plug"))
df_spark_stop = df_plug.select("TSstop","HEAVIN",'SEIDstop',"HMSGstop").withColumn("EVNT", F.lit("unplug"))
df_spark_stop = df_spark_stop.withColumnRenamed("TSstop", "TSstart")
df_spark_stop = df_spark_stop.withColumnRenamed("SEIDstop", "SEIDstart")
df_spark_stop = df_spark_stop.withColumnRenamed("HMSGstop", "HMSGstart")


# COMMAND ----------

df_union =df_spark_start.union(df_spark_stop)
df_union = df_union.withColumnRenamed("TSstart", "time")
df_union = df_union.orderBy("HEAVIN")

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicate drop and sort

# COMMAND ----------

df_union = df_union.dropDuplicates()
window_spec = Window.partitionBy('HEAVIN').orderBy('time')
df_union = df_union.withColumn('row_num', row_number().over(window_spec))
df_union = df_union.orderBy('HEAVIN', 'row_num').drop('row_num')


# COMMAND ----------

display(df_union.filter((F.col('HEAVIN') == "VR3F4DGZTMY505590") & ((F.col('time') < "2022-01-09 00:00:00") )).distinct())

# COMMAND ----------

df_union.write.mode("overwrite").parquet(dbutils.widgets.get('Charge_Data_Path_S3') +"data/Processed/Plot/df_ready_plot/datalabv2_id")

# COMMAND ----------

# MAGIC %md
# MAGIC # ready to plot datalabv1

# COMMAND ----------

df_from_datalabv1_spark = (spark.read
  .format("csv")
  .option("header",True)
  .options(delimiter=';')
  .load(f"file:/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/comparaison_msg.csv")
)

# COMMAND ----------

display(df_from_datalabv1_spark)

# COMMAND ----------

df_from_datalabv1_spark.write.mode("overwrite").parquet(dbutils.widgets.get('Data_path_S3') +"data/Processed/Plot/df_ready_plot/datalabv1_id")

# COMMAND ----------



# COMMAND ----------



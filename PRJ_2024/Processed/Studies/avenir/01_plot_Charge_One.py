# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re
import  toolbox_connected_vehicle as tcv
import numpy as np
import datetime as dt
import pytz
import pandas as pd
import shutil
import os
import json
from datetime import datetime
import time
import matplotlib.pyplot as plt
# import openpyxl

# COMMAND ----------

dbutils.widgets.text('S3', 's3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/')

# COMMAND ----------

path_result = dbutils.widgets.get('S3')+'Avenir/ChargeOne_test/'
path_result

# COMMAND ----------

def plot_v1(df,VIN, start_time=None, end_time=None):
    df = df[df['HEAD_VIN'] == VIN]
    df = df.sort_values(by='time')
    plt.step(df['time'], df['EVNT'], marker='o', linestyle='-', color='b', label=VIN, where='post')
    plt.legend()
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.xticks(rotation=45)
    
    if start_time is not None:
        start_time = datetime.strptime(start_time, '%Y-%m-%d')
    if end_time is not None:
        end_time = datetime.strptime(end_time, '%Y-%m-%d')
        
    if start_time is not None and end_time is not None:
        plt.xlim(start_time, end_time)

    plt.show()

# COMMAND ----------

def transform_to_plot(df_plug):
    df_spark_start = df_plug.select("CHARGE_START","HEAD_VIN",'CHARGE_SESSION').withColumn("EVNT", F.lit("plug"))
    df_spark_stop = df_plug.select("CHARGE_STOP","HEAD_VIN",'CHARGE_SESSION').withColumn("EVNT", F.lit("unplug"))
    df_union =df_spark_start.union(df_spark_stop)
    df_union = df_union.withColumnRenamed("CHARGE_START", "time")
    df_union = df_union.orderBy("HEAD_VIN")
    df_union = df_union.dropDuplicates()
    window_spec = Window.partitionBy('HEAD_VIN').orderBy('time')
    df_union = df_union.withColumn('row_num', F.row_number().over(window_spec))
    df_union = df_union.orderBy('HEAD_VIN', 'row_num').drop('row_num')
    return df_union

# COMMAND ----------


df_chargeOne = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_result)
)

# COMMAND ----------


df_plot = transform_to_plot(df_chargeOne)

# COMMAND ----------

plot_v1(df_plot.toPandas(),"VR3UHZKXZLT067992","2022-04-01", "2022-04-20")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



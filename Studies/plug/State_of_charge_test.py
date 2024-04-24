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
#import osy
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as stats
import seaborn as sns
from datetime import timedelta
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.dates as mdates
import json
from datetime import datetime

# import openpyxl

# COMMAND ----------

df_74 = (spark.read
  .format("parquet")
  .option("header",True)
  .options(delimiter=';')
  .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Dase/parquet/74/Bloc_74")
)
print("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Dase/parquet/74/Bloc_74")

# COMMAND ----------

df_79 = (spark.read
  .format("parquet")
  .option("header",True)
  .options(delimiter=';')
  .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Dase/parquet/79/Bloc_79")
)
print("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Dase/parquet/79/Bloc_79")

# COMMAND ----------

display(df_79)

# COMMAND ----------

display(df_74)

# COMMAND ----------

def plot(df,col_name,VIN, start_time=None, end_time=None):
    df_74_1_vin = df.select(col_name,'HEAD_COLL_TIMS').filter(F.col("HEAD_VIN") == VIN).orderBy('HEAD_COLL_TIMS').toPandas()
    plt.figure(figsize=(10, 6))
    plt.plot(df_74_1_vin['HEAD_COLL_TIMS'], df_74_1_vin[col_name], marker='o', linestyle='-')
    plt.title('Évolution du State of Charge de la batterie')
    plt.xlabel('Temps')
    plt.ylabel('State of Charge (%)')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    if start_time is not None:
        start_time = datetime.strptime(start_time, '%Y-%m-%d')
    if end_time is not None:
        end_time = datetime.strptime(end_time, '%Y-%m-%d')
            
    if start_time is not None and end_time is not None:
        plt.xlim(start_time, end_time)
    plt.show()

# COMMAND ----------

plot(df_74,"VEHC_STTS_CHRG_STT", "VF3M4DGZUMS015260" ,"2022-01-15", "2022-01-20" )

# COMMAND ----------

plot(df_79,"BATT_STTS_SUMM_SOC" ,"VF3M4DGZUMS015260" ,"2022-01-15", "2022-01-20" )

# COMMAND ----------



# COMMAND ----------



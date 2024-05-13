# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Start widgets

# COMMAND ----------

# widget data path 
dbutils.widgets.text('Charge_Data_Path_S3', 's3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Dase/Request/')

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

############################### Python File #############################
from Read import Read_and_plot as RaP

# import openpyxl

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
print("Chemin du notebook :", notebook_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Hyperparameters

# COMMAND ----------

with open('/Workspace/Repos/ugo.merlier@stellantis.com/CHRG00/Dase/Request/config.json', 'r') as file:
    data = json.load(file)

# Extraire les informations individuelles
vin = data["vin"]
start_periode_plot = data["start_periode_plot"]
end_periode_plot = data["end_periode_plot"]
start_date = data["start_date"]
end_date = data["end_date"]
folder_name = data["folder_name"]

# Convertir la chaîne en objet datetime
date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()

# Formater la date en une chaîne de caractères avec le format souhaité
nouvelle_start_date = date_obj.strftime("%Y/%m")

# COMMAND ----------

# MAGIC %md
# MAGIC # Start

# COMMAND ----------

# MAGIC %md
# MAGIC # Data informations

# COMMAND ----------

read_obj = RaP(dbutils.widgets.get('Charge_Data_Path_S3'),folder_name,nouvelle_start_date)

# COMMAND ----------

df_79_cor = RaP.SOC_correction(read_obj.df_79)

# COMMAND ----------

read_obj.df_plug.select('HEAVIN').toPandas().describe()[:2]

# COMMAND ----------

df_plot = RaP.transform_to_plot(read_obj.df_plug)

# COMMAND ----------

RaP.plot(df_plot.toPandas(),df_79_cor.toPandas(), "BATT_STTS_SUMM_SOC", vin )

# COMMAND ----------

RaP.plot(df_plot.toPandas(),df_79_cor.toPandas(),"BATT_STTS_SUMM_SOC",vin,start_periode_plot, end_periode_plot )

# COMMAND ----------

RaP.plot(df_plot.toPandas(),read_obj.df_74.toPandas(), "VEHC_STTS_CHRG_STT", vin)

# COMMAND ----------

RaP.plot(df_plot.toPandas(),read_obj.df_74.toPandas(),"VEHC_STTS_CHRG_STT",vin,start_periode_plot, end_periode_plot)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



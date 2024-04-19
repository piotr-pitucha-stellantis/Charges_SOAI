# Databricks notebook source
# MAGIC %md
# MAGIC ## The aim of this notebook is to compare charge data created in the datalabv2 and datalabv1 

# COMMAND ----------

# MAGIC %md
# MAGIC # Start widgets

# COMMAND ----------

# Create a text widget for the userid definition
dbutils.widgets.text('UserID', '')
# dbutils.widgets.remove('path') # to remove the widget

# Get the input value
userid = dbutils.widgets.get('UserID')



# COMMAND ----------

dbutils.widgets.text('Data_path', '')

# COMMAND ----------

# widget data path 
dbutils.widgets.text('Data_path_S3', '')

# COMMAND ----------

print('Data_path_S3 path :', dbutils.widgets.get('Data_path_S3'))

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

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
print("Chemin du notebook :", notebook_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #STARTING PROCESS

# COMMAND ----------

class PlotAnalysis():
    def __init__(self) -> None:
        self.dfv1_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load(dbutils.widgets.get('Data_path_S3') +"data/Processed/Plot/df_ready_plot/datalabv1_id")
                    )
        self.dfv2_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load(dbutils.widgets.get('Data_path_S3') +"data/Processed/Plot/df_ready_plot/datalabv2_id")
                    )
        
        temp = self.dfv1_spk.toPandas()
        temp['time'] = pd.to_datetime(temp['time'])
        temp['SEIDstart'] = temp['SEIDstart'].astype('int64')
        temp['HMSGstart'] = temp['HMSGstart'].astype('int64')
        self.dfv1_pd = temp.drop_duplicates()

        self.dfv2_pd = self.dfv2_spk.toPandas()
        
        self.liste_vin = self.dfv2_pd['HEAVIN'].unique().tolist()


    def plotDisplay(self,num_max =None ):
        if num_max is None:
                    num_max = len(self.liste_vin)

        min_time = self.dfv2_pd['time'].min()
        max_time = self.dfv2_pd['time'].max()
        if num_max > 1:
            fig, axs = plt.subplots(num_max, 1, figsize=(20, 4*num_max))
        else:
            fig, axs = plt.subplots(1, 1, figsize=(20, 4))
            axs = [axs] 

        for i, vin in enumerate(self.liste_vin):
            if i >= num_max:
                break
            df_sorted_ready_chart_v2 = self.dfv2_pd[self.dfv2_pd['HEAVIN'] == vin]
            df_sorted_ready_chart_v1 = self.dfv1_pd[self.dfv1_pd['HEAVIN'] == vin]
            axs[i].step(df_sorted_ready_chart_v2['time'], df_sorted_ready_chart_v2['EVNT'], marker='o', linestyle='-', color='b', label="datalab v2", where='post')
            axs[i].step(df_sorted_ready_chart_v1['time'], df_sorted_ready_chart_v1['EVNT'], marker='o', linestyle='-', color='red', label="datalab v1", where='post')
            axs[i].invert_yaxis()  # Inversion de l'axe y
            axs[i].legend()
            axs[i].set_title(f'Comparison of charge definition methods on one VIN during one month. VIN : {vin}', fontsize=16)
            axs[i].set_xlim(min_time, max_time)  # Définition des limites des axes x

        plt.tight_layout()
        plt.show()


    def diffRange(self,num_max =None):
        RESET = "\033[0m"
        RED = "\033[91m"

        if num_max is None:
            num_max = len(self.liste_vin)

        for i, vin in enumerate(self.liste_vin):
            if i >= num_max:
                break
            df_v2= self.dfv2_pd[self.dfv2_pd['HEAVIN'] == vin].sort_values(by="time")
            df_v1= self.dfv1_pd[self.dfv1_pd['HEAVIN'] == vin].sort_values(by="time")
            list_v1 = df_v2['time'].tolist()
            list_v2 = df_v1['time'].tolist()
            diff3 = []
            for dt1, dt2 in zip(list_v1, list_v2):
                diff = dt2 - dt1
                diff3.append(diff)
            df_temp = pd.DataFrame(diff3, columns=['diff'])
            if df_temp['diff'].mean() > dt.timedelta(days=1) or df_temp['diff'].mean() < - dt.timedelta(days=1):
                print("VIN : "+ str(vin) + " =====> The difference range between datalabv1 and datalabv2 is : " + RED + str(df_temp['diff'].mean())+ RESET)
            else:
                print("VIN : "+ str(vin) + " =====> The difference range between datalabv1 and datalabv2 is : " + str(df_temp['diff'].mean()))

    def stats(self):
        df_join = pd.merge(self.dfv1_pd, self.dfv2_pd, on=['HEAVIN', 'SEIDstart','HMSGstart'], how='left')
        df_join['time_x'] = pd.to_datetime(df_join['time_x'])
        df_join['time_y'] = pd.to_datetime(df_join['time_y'])
        df_join['diff'] = df_join['time_x'] - df_join['time_y']
        return df_join



    def histo_stat(self, df):
        df['diff_seconds'] = df['diff'].dt.total_seconds()
        plt.hist(df['diff_seconds'], bins=100, density=True)
        plt.title('Histogramme des valeurs timedelta')
        plt.xlabel('Secondes')
        plt.ylabel('Fréquence')
        plt.show()

        


    def histo(self, datalab):

        if datalab == "datalabv1":
            df= self.dfv1_pd
            color = "blue"
            label = "v1"
        elif datalab == "datalabv2":
            df = self.dfv2_pd
            color = "red"
            label = "v2"
        else:
            print("------------You have to choose between datalabv1 or datalabv2------------")
            return
        plt.figure(figsize=(10, 6))
        df['time'].hist(alpha=0.5, color=color, bins=20, label='Datalab'+label)
        plt.xlabel('Valeur')
        plt.ylabel('Fréquence')
        plt.title('Comparaison des valeurs entre les deux DataFrames')
        plt.legend()
        plt.show()
    


# COMMAND ----------

analyse = PlotAnalysis()

# COMMAND ----------

analyse.dfv1_pd[(analyse.dfv1_pd['HEAVIN'] == "VR3F4DGZTMY505590") & (analyse.dfv1_pd['time'] < "2022-01-09 00:00:00")]

# COMMAND ----------

display(analyse.dfv2_spk.filter((F.col('HEAVIN') == "VR3F4DGZTMY505590") & ((F.col('time') < "2022-01-09 00:00:00") )).distinct())

# COMMAND ----------

df_stat = analyse.stats()
print("min : ",df_stat['diff'].min())
print("max : ", df_stat['diff'].max())
print("Number of None values : ",df_stat['diff'].isnull().sum())

# COMMAND ----------

df_stat.describe()

# COMMAND ----------

df_stat[df_stat['diff'].isna()]

# COMMAND ----------

analyse.histo_stat(df_stat)

# COMMAND ----------

analyse.plotDisplay(10)

# COMMAND ----------

analyse.diffRange()

# COMMAND ----------

analyse.histo("datalabv2")

# COMMAND ----------

analyse.histo("datalabv1")

# COMMAND ----------

analyse.dfv1_pd['time'].describe()

# COMMAND ----------

analyse.dfv2_pd['time'].describe()

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC # Comparaison des deux dataframes de charges venant de Dase et Avenir

# COMMAND ----------

# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/StellantisGADP/Charges_SOAI/main/assets/powerpoint_cas_pratique.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 véhicules 
# MAGIC ## 1 mois (2022-01-01  2022-02-01)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Librairies 

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
import os
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import builtins
# import openpyxl

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
print("Chemin du notebook :", notebook_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fonctions

# COMMAND ----------

def Min_Max_date(df):
    min_date_start_plug = df.select(min("START")).first()[0]
    max_date_start_plug = df.select(max("START")).first()[0]

    min_date_stop_plug = df.select(min("STOP")).first()[0]
    max_date_stop_plug = df.select(max("STOP")).first()[0]

    print("Min date start :", min_date_start_plug)
    print("Max date start :", max_date_start_plug)
    print("Min date stop :", min_date_stop_plug)
    print("Max date stop :", max_date_stop_plug)

# COMMAND ----------

def anonymise_dataframe(df, VIN_col, VAN_col='VAN'):
    VIN_column = F.col(VIN_col)
    list_columns = df.columns
    df = df.withColumn('VAN', udf_hash(VIN_column))
    df.drop(VIN_column)
    new_list_columns = [VAN_col if col == VIN_col else col  for col in list_columns ]
    df = df.select(*new_list_columns)
    return df

# COMMAND ----------

from pyspark.sql import types as T
filename = '/Workspace/Users/ugo.merlier@stellantis.com/data/key/APRS_key'
print(filename)

import pickle
code=pickle.load(open(filename,"rb"))


import hashlib
def anonym(x):

    return hashlib.md5(x.encode('utf-8')+code.encode('utf-8')).hexdigest()

print('Verif  : 7dc519c5b42887df1ba5a0d398719717')
print('anonym : ' + anonym('VR3xxxxxxxxxxxxxx'))

# udf spark  function :
udf_hash = F.udf(anonym, T.StringType())

# COMMAND ----------

def vin_pas_en_commun(list_vin_Avenir,list_vin_Dase):
    
    seulement_liste1 = list(set(list_vin_Avenir) - set(list_vin_Dase))

    seulement_liste2 = list(set(list_vin_Dase) - set(list_vin_Avenir))

    resultat = seulement_liste1 + seulement_liste2

    print(resultat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw data 

# COMMAND ----------

path_raw_plug_74 = "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/74"
path_raw_plug_79 = "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/79"
path_raw_plug_68 = "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/68"

# COMMAND ----------


df_74 = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_raw_plug_74)
)

df_79 = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_raw_plug_79)
)

df_68 = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_raw_plug_68)
)

# COMMAND ----------

df_79_ano = anonymise_dataframe(df_79,'HEAD_VIN')
df_79_ano =df_79_ano.withColumnRenamed('VAN' , 'HEAD_VAN')
df_74_ano = anonymise_dataframe(df_74,'HEAD_VIN')
df_74_ano =df_74_ano.withColumnRenamed('VAN' , 'HEAD_VAN')
df_68_ano = anonymise_dataframe(df_68,'HEAD_VIN')
df_68_ano =df_68_ano.withColumnRenamed('VAN' , 'HEAD_VAN')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Premier dataframe de charges provenant du code Dase

# COMMAND ----------

path_plug = "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Dase/Request/VIN_comparison_10_from_plug/levdata/plugs/2022/01"


# COMMAND ----------

df_plug = (spark.read
        .format("parquet")
        .option("header",True)
        .options(delimiter=';')
        .load(path_plug)
        )

# COMMAND ----------

df_plug_ano = anonymise_dataframe(df_plug,"HEAVIN")

# COMMAND ----------

list_vin_Dase =df_plug_ano.select('VAN').distinct().toPandas()['VAN'].tolist()
print("Nombre de vins :", len(list_vin_Dase) )

# COMMAND ----------

list_vin_Dase

# COMMAND ----------

# # Anonymiser la liste
# liste_anonymisee =['a440072251b85bb2575fca092c59fbbd',
#  '29d03881949a474ee065cca3dc96b8c0',
#  'facb8c66f613048b17f7660924ef6b61',
#  '5c3d74294674c3060766e160b134d8ce',
#  'fc7c39cbf88aa5507a69824291f11c55',
#  '97574ea267e2a878326c856b7dd92f79',
#  '57ab2104d6abeba17fc16d6294fefcc7',
#  '8f44cb222cf4953c984c2ba5e68fce4c',
#  '20923ca744bf0e82f8f4e327c818be8b']

# COMMAND ----------

# MAGIC %md
# MAGIC Statistiques rapides sur le dataframe à l'aide de pandas 

# COMMAND ----------

df_plug_ano.toPandas().describe()

# COMMAND ----------

df_plug_renamed = df_plug_ano.withColumnRenamed("TSstart", "START")
df_plug_renamed = df_plug_renamed.withColumnRenamed("TSstop", "STOP")
df_plug_renamed = df_plug_renamed.withColumnRenamed("SEIDstart", "SESSION_START")
df_plug_renamed = df_plug_renamed.withColumnRenamed("SEIDstop", "SESSION_STOP")
df_plug_renamed = df_plug_renamed.withColumnRenamed("HMSGstart", "MESSAGE_START")
df_plug_renamed = df_plug_renamed.withColumnRenamed("HMSGstop", "MESSAGE_STOP")
df_plug_renamed = df_plug_renamed.withColumnRenamed("VAN", "HEAD_VAN")
df_plug_renamed = df_plug_renamed.withColumnRenamed("eSOCstart", "SOC_START")
df_plug_renamed = df_plug_renamed.withColumnRenamed("eSOCstop", "SOC_STOP")

df_plug_renamed = df_plug_renamed.withColumn("Diff_soc", expr("SOC_STOP - SOC_START"))

seconds_diff_dase = (F.unix_timestamp(F.col('STOP')) - F.unix_timestamp(F.col('START')))
df_dase_spark_ready = df_plug_renamed.withColumn('STOP-START_heures', seconds_diff_dase / 3600)
# df_plug_renamed = df_plug_renamed.withColumnRenamed("SEIDstart", "SOC_START_ID")
# df_plug_renamed = df_plug_renamed.withColumnRenamed("SEIDstop", "SOC_STOP_ID")

# COMMAND ----------

Min_Max_date(df_dase_spark_ready)

# COMMAND ----------

df_dase_spark_ready = df_dase_spark_ready.select("HEAD_VAN","SESSION_START","SESSION_STOP","MESSAGE_START","MESSAGE_STOP", "START","STOP","SOC_START","SOC_STOP","STOP-START_heures",'Diff_soc')
df_plug_renamed_pd = df_dase_spark_ready.toPandas()

# COMMAND ----------

df_plug_renamed_pd = df_plug_renamed_pd[["HEAD_VAN","SESSION_START","SESSION_STOP","MESSAGE_START","MESSAGE_STOP", "START","STOP","SOC_START","SOC_STOP","STOP-START_heures",'Diff_soc']]

# COMMAND ----------

df_plug_renamed_pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second dataframe de charges provenant du code Avenir

# COMMAND ----------

path_avenir =  's3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/ChargeThree01_test/'

# COMMAND ----------


df_chargethree = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_avenir)
)

# COMMAND ----------

df_chargethree_ano = anonymise_dataframe(df_chargethree,'HEAD_VIN')

# COMMAND ----------

list_vin_Avenir =df_chargethree_ano.select('VAN').distinct().toPandas()['VAN'].tolist()
print("Nombre de vins :", len(list_vin_Avenir) )

# COMMAND ----------

list_vin_Avenir

# COMMAND ----------

vin_pas_en_commun(list_vin_Avenir,list_vin_Dase)

# COMMAND ----------

df_chargethree_pd = df_chargethree_ano.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Statistiques rapides sur le dataframe à l'aide de pandas 

# COMMAND ----------

df_chargethree_pd.describe()

# COMMAND ----------

Min_Max_date(df_chargethree_ano)

# COMMAND ----------

# MAGIC %md
# MAGIC Nettoyage

# COMMAND ----------

df_chargethree_filtered_renamed = df_chargethree_ano.withColumnRenamed("HEAD_SESS_ID_start", "SESSION_START")
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumnRenamed("HEAD_SESS_ID_stop", "SESSION_STOP")
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumnRenamed("HEAD_MESS_ID_start", "MESSAGE_START")
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumnRenamed("HEAD_MESS_ID_stop", "MESSAGE_STOP")
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumnRenamed("VAN", "HEAD_VAN")

# On remplace les null par 0
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.fillna(0, subset=['SESSION_START', 'SESSION_STOP', 'MESSAGE_START', 'MESSAGE_STOP'])

# Convertir les colonnes en type entier
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumn("SESSION_START", col("SESSION_START").cast("int"))
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumn("SESSION_STOP", col("SESSION_STOP").cast("int"))
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumn("MESSAGE_START", col("MESSAGE_START").cast("int"))
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumn("MESSAGE_STOP", col("MESSAGE_STOP").cast("int"))
df_chargethree_filtered_renamed = df_chargethree_filtered_renamed.withColumn("Diff_soc", expr("SOC_STOP - SOC_START"))
seconds_diff_avenir = (F.unix_timestamp(F.col('STOP')) - F.unix_timestamp(F.col('START')))
df_avenir_spark_ready = df_chargethree_filtered_renamed.withColumn('STOP-START_heures', seconds_diff_avenir / 3600)
df_avenir_spark_ready = df_avenir_spark_ready.select("HEAD_VAN","SESSION_START",'SESSION_STOP','MESSAGE_START',"MESSAGE_STOP","START","STOP","SOC_START","SOC_STOP","STOP-START_heures",'Diff_soc')
df_chargethree_pd = df_avenir_spark_ready.toPandas()

df_chargethree_pd = df_chargethree_pd[["HEAD_VAN","SESSION_START",'SESSION_STOP','MESSAGE_START',"MESSAGE_STOP","START","STOP","SOC_START","SOC_STOP","STOP-START_heures",'Diff_soc']]

# COMMAND ----------

df_chargethree_pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## First comparison

# COMMAND ----------

df_chargethree_pd.dtypes

# COMMAND ----------

df_plug_renamed_pd.dtypes

# COMMAND ----------

df_chargethree_pd.sort_values(by="START")

# COMMAND ----------

df_plug_renamed_pd.sort_values(by="START")

# COMMAND ----------

# MAGIC %md
# MAGIC Jointure sur les dates

# COMMAND ----------

df_avenir_spark_ready_join =df_avenir_spark_ready
df_dase_spark_ready_join = df_dase_spark_ready.withColumnRenamed("SESSION_START", "SESSION_START_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("SESSION_STOP", "SESSION_STOP_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("MESSAGE_START", "MESSAGE_START_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("MESSAGE_STOP", "MESSAGE_STOP_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("START", "START_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("STOP", "STOP_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("SOC_START", "SOC_START_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("SOC_STOP", "SOC_STOP_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("STOP-START_heures", "STOP-START_heures_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("Diff_soc", "Diff_soc_Dase")
df_dase_spark_ready_join = df_dase_spark_ready_join.withColumnRenamed("HEAD_VAN", "HEAD_VAN_Dase")

# COMMAND ----------

# join_condition = [df_dase_spark_ready_join.HEAD_VAN_Dase == df_avenir_spark_ready_join.HEAD_VAN, ((df_dase_spark_ready_join.START_Dase >= df_avenir_spark_ready_join.START ) &(df_dase_spark_ready_join.START_Dase <= df_avenir_spark_ready_join.STOP ) | (df_dase_spark_ready_join.STOP_Dase >= df_avenir_spark_ready_join.START ) &(df_dase_spark_ready_join.STOP_Dase <= df_avenir_spark_ready_join.STOP ) )]
# df_join_date = df_dase_spark_ready_join.join(df_avenir_spark_ready_join, how="left_outer", on =join_condition)

# COMMAND ----------

# DBTITLE 1,CrossJoin with Date Charge Commune
# crossjoin = jointure cartésienne
df_Date_charge_commune = df_dase_spark_ready_join.crossJoin(df_avenir_spark_ready_join)\
    .withColumn("Date_charge_commune", 
                (df_dase_spark_ready_join.START_Dase.between(df_avenir_spark_ready_join.START,df_avenir_spark_ready_join.STOP)) |
                 (df_dase_spark_ready_join.STOP_Dase.between(df_avenir_spark_ready_join.START, df_avenir_spark_ready_join.STOP)) |
                 (df_avenir_spark_ready_join.START.between(df_dase_spark_ready_join.START_Dase,df_dase_spark_ready_join.STOP_Dase)) |
                 (df_avenir_spark_ready_join.STOP.between(df_dase_spark_ready_join.START_Dase, df_dase_spark_ready_join.STOP_Dase)))


df_join = df_Date_charge_commune.filter("Date_charge_commune").drop("Date_charge_commune")
#df_join = df_join.drop("Date_charge_commune").dropDuplicates(["HEAD_VAN_Dase", "START_Dase", "STOP_Dase"])

# COMMAND ----------

# DBTITLE 1,Convert DataFrame to Pandas DataFrame.
df_join.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/StellantisGADP/Charges_SOAI/main/assets/join.png)

# COMMAND ----------

df_join_diff = df_join.withColumn("difference_absolue", abs(df_join["STOP-START_heures_Dase"] - df_join["STOP-START_heures"]))

# COMMAND ----------

moyenne_difference_absolue = df_join_diff.agg(avg("difference_absolue")).collect()[0][0]

print("La moyenne de la colonne difference_absolue est :"+str( moyenne_difference_absolue) + " heures")

# COMMAND ----------

df_pandas = df_join_diff.select("difference_absolue").toPandas()

# Tracer l'histogramme avec Seaborn
plt.figure(figsize=(10, 6))  # Ajuster la taille de la figure
sns.histplot(df_pandas['difference_absolue'])  # Tracer l'histogramme avec KDE
plt.xlabel('Difference du temps de charge (en heure)')
plt.ylabel('Fréquence')
plt.title('Histogramme difference du temps de charge entre Dase et Avenir')
plt.show()

# COMMAND ----------

df_join_diff_filtered = df_join_diff.filter(F.col("difference_absolue") >= 40)

# COMMAND ----------


df_join_diff_filtereddf_join_diff_pd = df_join_diff_filtered.toPandas()


plt.figure(figsize=(10, 8), dpi=80)

min_list = []
min_list.append(df_join_diff_filtereddf_join_diff_pd['STOP-START_heures_Dase'].min())
min_list.append(df_join_diff_filtereddf_join_diff_pd['STOP-START_heures'].min())
min_diff_soc = builtins.min(min_list)

max_list = []
max_list.append(df_join_diff_filtereddf_join_diff_pd['STOP-START_heures_Dase'].max())
max_list.append(df_join_diff_filtereddf_join_diff_pd['STOP-START_heures'].max())
max_diff_soc = builtins.max(max_list)


# Spécifiez la plage des bins à utiliser pour les deux histogrammes
bin_range = (min_diff_soc, max_diff_soc)


sns.histplot(data=df_join_diff_filtereddf_join_diff_pd, x="STOP-START_heures", binrange=bin_range, bins=10, color='green',label='Avenir')
plt.legend(loc='upper left')
plt.xticks(rotation=45)


sns.histplot(data=df_join_diff_filtereddf_join_diff_pd, x="STOP-START_heures_Dase", binrange=bin_range, bins=10, color='r', label='Dase')
plt.title('Distrubution du temps de charge lorsque la difference est supèrieur à 40h')
plt.legend(loc='upper left')
plt.xticks(rotation=45)

plt.show()


# COMMAND ----------

df_avenir_spark_ready_join_filtered = df_avenir_spark_ready_join.filter(F.col("STOP-START_heures")<=50)

df_Date_charge_commune_v2 = df_dase_spark_ready_join.crossJoin(df_avenir_spark_ready_join_filtered)\
    .withColumn("Date_charge_commune", 
                (df_dase_spark_ready_join.START_Dase.between(df_avenir_spark_ready_join_filtered.START,df_avenir_spark_ready_join_filtered.STOP)) |
                 (df_dase_spark_ready_join.STOP_Dase.between(df_avenir_spark_ready_join_filtered.START, df_avenir_spark_ready_join_filtered.STOP)) |
                 (df_avenir_spark_ready_join_filtered.START.between(df_dase_spark_ready_join.START_Dase,df_dase_spark_ready_join.STOP_Dase)) |
                 (df_avenir_spark_ready_join_filtered.STOP.between(df_dase_spark_ready_join.START_Dase, df_dase_spark_ready_join.STOP_Dase)))


df_join_v2 = df_Date_charge_commune_v2.filter("Date_charge_commune")
df_join_v2 = df_join_v2.drop("Date_charge_commune").dropDuplicates(["HEAD_VAN_Dase", "START_Dase", "STOP_Dase"])

df_join_diff_v2 = df_join_v2.withColumn("difference_absolue", abs(df_join["STOP-START_heures_Dase"] - df_join["STOP-START_heures"]))

# COMMAND ----------

moyenne_difference_absolue_v2 = df_join_diff_v2.agg(avg("difference_absolue")).collect()[0][0]

print("La moyenne de la colonne difference_absolue est :"+str( moyenne_difference_absolue_v2) + " heures")

# COMMAND ----------

df_pandas = df_join_diff_v2.select("difference_absolue").toPandas()

# Tracer l'histogramme avec Seaborn
plt.figure(figsize=(10, 6))  # Ajuster la taille de la figure
sns.histplot(df_pandas['difference_absolue'])  # Tracer l'histogramme avec KDE
plt.xlabel('Difference du temps de charge (en heure)')
plt.ylabel('Fréquence')
plt.title('Histogramme difference du temps de charge entre Dase et Avenir')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion : 
# MAGIC
# MAGIC Sur les 86 charges commune, on aperçoit une grande difference concernant le temps de charge. Cela est du aux outliers présents dans avenir. Lors de la jointure les outliers sont associés à plusieurs charges du à leur range élevé. Comme on le voit après avoir enlever les outlier la difference de temps de charge diminue passant d'une moyenne de 11h à 4h.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Graphiques statistiques

# COMMAND ----------

# MAGIC %md
# MAGIC box plot

# COMMAND ----------

df_plug_renamed_pd_copy = df_plug_renamed_pd.copy()
df_chargethree_pd_copy = df_chargethree_pd.copy()

df_plug_renamed_pd_copy['STOP-START_heures'] = (df_plug_renamed_pd_copy['STOP'] - df_plug_renamed_pd_copy['START']).dt.total_seconds() / 3600
df_chargethree_pd_copy['STOP-START_heures'] = (df_chargethree_pd_copy['STOP'] - df_chargethree_pd_copy['START']).dt.total_seconds() / 3600

fig, axs = plt.subplots(1, 2, figsize=(12, 6))


sns.boxplot(y='STOP-START_heures', data=df_plug_renamed_pd_copy, ax=axs[0])
axs[0].set_title('Dase')
axs[0].set_ylabel('Nombre d\'heures')
axs[0].set_ylim(0, 80)

sns.boxplot(y='STOP-START_heures', data=df_chargethree_pd_copy, ax=axs[1])
axs[1].set_title('Avenir')
axs[1].set_ylabel('Nombre d\'heures')
axs[1].set_ylim(0, 80)

plt.tight_layout()


plt.show()

# COMMAND ----------

df_plug_renamed_pd_copy = df_plug_renamed_pd.copy()
df_chargethree_pd_copy = df_chargethree_pd.copy()

df_plug_renamed_pd_copy['STOP-START_heures'] = (df_plug_renamed_pd_copy['STOP'] - df_plug_renamed_pd_copy['START']).dt.total_seconds() / 3600
df_chargethree_pd_copy['STOP-START_heures'] = (df_chargethree_pd_copy['STOP'] - df_chargethree_pd_copy['START']).dt.total_seconds() / 3600

fig, axs = plt.subplots(1, 2, figsize=(12, 6))


sns.boxplot(y='STOP-START_heures', data=df_plug_renamed_pd_copy, ax=axs[0])
axs[0].set_title('Dase')
axs[0].set_ylabel('Nombre d\'heures')
axs[0].set_ylim(0, 20)

sns.boxplot(y='STOP-START_heures', data=df_chargethree_pd_copy, ax=axs[1])
axs[1].set_title('Avenir')
axs[1].set_ylabel('Nombre d\'heures')
axs[1].set_ylim(0, 20)
# Ajuster l'espacement entre les sous-graphiques
plt.tight_layout()

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC filtrage pour enlever les charges de plus de 10h

# COMMAND ----------

df_plug_renamed_pd_copy = df_plug_renamed_pd.copy()
df_chargethree_pd_copy = df_chargethree_pd.copy()

df_plug_renamed_pd_copy['STOP-START_heures'] = (df_plug_renamed_pd_copy['STOP'] - df_plug_renamed_pd_copy['START']).dt.total_seconds() / 3600
df_chargethree_pd_copy['STOP-START_heures'] = (df_chargethree_pd_copy['STOP'] - df_chargethree_pd_copy['START']).dt.total_seconds() / 3600

df_plug_renamed_pd_copy = df_plug_renamed_pd_copy[df_plug_renamed_pd_copy["STOP-START_heures"]<= 10]
df_chargethree_pd_copy = df_chargethree_pd_copy[df_chargethree_pd_copy["STOP-START_heures"]<= 10]

fig, axs = plt.subplots(1, 2, figsize=(12, 6))


sns.boxplot(y='STOP-START_heures', data=df_plug_renamed_pd_copy, ax=axs[0])
axs[0].set_title('Dase')
axs[0].set_ylabel('Nombre d\'heures')


sns.boxplot(y='STOP-START_heures', data=df_chargethree_pd_copy, ax=axs[1])
axs[1].set_title('Avenir')
axs[1].set_ylabel('Nombre d\'heures')

# Ajuster l'espacement entre les sous-graphiques
plt.tight_layout()


plt.show()

# COMMAND ----------

df_chargethree_pd_copy[df_chargethree_pd_copy["STOP-START_heures"]< 0]

# COMMAND ----------

# MAGIC %md
# MAGIC Outlier avec le plus grand temps de charge coté Dase

# COMMAND ----------

max_temps_charge_dase = df_plug_renamed_pd['STOP-START_heures'].max()
df_dase_max_temps_charge = df_plug_renamed_pd[df_plug_renamed_pd['STOP-START_heures'] == max_temps_charge_dase]
df_dase_max_temps_charge

# COMMAND ----------

# MAGIC %md
# MAGIC Outlier avec le plus grand temps de charge coté Avenir

# COMMAND ----------

max_temps_charge_avenir = df_chargethree_pd['STOP-START_heures'].max()
df_avenir_max_temps_charge = df_chargethree_pd[df_chargethree_pd['STOP-START_heures'] == max_temps_charge_avenir]
df_avenir_max_temps_charge

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion :
# MAGIC
# MAGIC On retrouve avec les boites à moustaches la plus grande quantité d'outlier sur le temps de charge pour le traitement avenir. De plus, on aperçoit qu'il y a au moins 1 temps de charge négative pour Avenir.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Distribution

# COMMAND ----------

# MAGIC %md
# MAGIC Distribution temps de charge

# COMMAND ----------



df_chargethree_pd_copy = df_chargethree_pd.copy()
df_plug_renamed_pd_copy = df_plug_renamed_pd.copy()

min_list = []
min_list.append(df_chargethree_pd_copy['STOP-START_heures'].min())
min_list.append(df_plug_renamed_pd_copy['STOP-START_heures'].min())
min_diff_soc = builtins.min(min_list)

max_list = []
max_list.append(df_chargethree_pd_copy['STOP-START_heures'].max())
max_list.append(df_plug_renamed_pd_copy['STOP-START_heures'].max())
max_diff_soc = builtins.max(max_list)


# Spécifiez la plage des bins à utiliser pour les deux histogrammes
bin_range = (min_diff_soc, max_diff_soc)



fig, axs = plt.subplots(2, 1, figsize=(12, 6), sharex=True)

sns.histplot(df_plug_renamed_pd_copy['STOP-START_heures'], binrange=bin_range,  ax=axs[0])
axs[0].set_xlabel('Différence entre STOP et START (heures)')
axs[0].set_ylabel('Fréquence')
axs[0].set_title('Histogramme temps de charge Dase')



sns.histplot(df_chargethree_pd_copy['STOP-START_heures'], binrange=bin_range,  ax=axs[1])
axs[1].set_xlabel('Différence entre STOP et START (heures)')
axs[1].set_ylabel('Fréquence')
axs[1].set_title('Histogramme temps de charge Avenir')


plt.tight_layout()

# COMMAND ----------

# MAGIC %md
# MAGIC ![Name of the image](https://raw.githubusercontent.com/StellantisGADP/Charges_SOAI/main/assets/conclusion_cas_pratique.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Etude de la distribution sur la difference de SOC

# COMMAND ----------


df_chargethree_pd_copy = df_chargethree_pd.copy()
df_plug_renamed_pd_copy = df_plug_renamed_pd.copy()

plt.figure(figsize=(10, 8), dpi=80)

min_list = []
min_list.append(df_chargethree_pd_copy['Diff_soc'].min())
min_list.append(df_plug_renamed_pd_copy['Diff_soc'].min())
min_diff_soc = builtins.min(min_list)

max_list = []
max_list.append(df_chargethree_pd_copy['Diff_soc'].max())
max_list.append(df_plug_renamed_pd_copy['Diff_soc'].max())
max_diff_soc = builtins.max(max_list)


# Spécifiez la plage des bins à utiliser pour les deux histogrammes
bin_range = (min_diff_soc, max_diff_soc)


sns.histplot(data=df_chargethree_pd_copy, x="Diff_soc",  binrange=bin_range, bins=10, color='green',label='Avenir')
plt.legend(loc='upper left')
plt.xticks(rotation=45)


sns.histplot(data=df_plug_renamed_pd_copy, x="Diff_soc", binrange=bin_range, bins=10, color='r', label='Dase')
plt.title('Distrubution du soc gagné durant les charges')
plt.legend(loc='upper left')
plt.xticks(rotation=45)

plt.show()


# COMMAND ----------


    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot

# COMMAND ----------

def transform_to_plot(df_plug):
    df_spark_start = df_plug.select("START","HEAD_VAN",'SESSION_START','MESSAGE_START','Diff_soc','STOP-START_heures').withColumn("EVNT", F.lit("plug"))
    df_spark_stop = df_plug.select("STOP","HEAD_VAN",'SESSION_STOP',"MESSAGE_STOP",'Diff_soc','STOP-START_heures').withColumn("EVNT", F.lit("unplug"))
    df_union =df_spark_start.union(df_spark_stop)
    df_union = df_union.withColumnRenamed("START", "time")
    df_union = df_union.orderBy("HEAD_VAN")
    df_union = df_union.dropDuplicates()
    window_spec = Window.partitionBy('HEAD_VAN').orderBy('time')
    df_union = df_union.withColumn('row_num', F.row_number().over(window_spec))
    df_union = df_union.orderBy('HEAD_VAN', 'row_num').drop('row_num')
    return df_union

# COMMAND ----------

def plot(
        df1,
        df2,
        df_68,
        df_74,
        df_79,
        VIN,
        start_time=None,
        end_time=None,
        col_name1 = "BATT_STTS_SUMM_SOC",
        col_name2 = "VEHC_STTS_CHRG_STT",
        col_name3 ="BATT_CHRG_STT",
        col_name4="BATT_STTS_SUMM_CHRG_STT",
        col_name5="VEHC_STTS_LIFT_MILG",
        col_name6="BATT_LOAD_LEVL"):
        for i, vin in enumerate(VIN):
            # Filtrer les données selon le VIN et trier par le temps
            df1_temp = df1[df1['HEAD_VAN'] == vin]
            df1_temp = df1_temp.sort_values(by='time')
            df2_temp = df2[df2['HEAD_VAN'] == vin]
            df2_temp = df2_temp.sort_values(by='time')
            df_79_temp = df_79[df_79['HEAD_VAN'] == vin]
            df_79_temp = df_79_temp.sort_values(by='HEAD_COLL_TIMS')
            df_74_temp = df_74[df_74['HEAD_VAN'] == vin]
            df_74_temp = df_74_temp.sort_values(by='HEAD_COLL_TIMS') 
            df_68_temp = df_68[df_68['HEAD_VAN'] == vin]
            df_68_temp = df_68_temp.sort_values(by='HEAD_COLL_TIMS') 

            # Convertir les chaînes de caractères en objets datetime si spécifié
            if start_time is not None:
                start_time_datetime = datetime.strptime(start_time, '%Y-%m-%d')
            if end_time is not None:
                end_time_datetime = datetime.strptime(end_time, '%Y-%m-%d')
            
            # Créer une figure avec deux sous-tracés empilés verticalement
            fig, (ax1, ax2, ax3,ax4,ax5,ax6, ax7, ax8) = plt.subplots(8, 1, figsize=(17, 13), sharex=True)
            
            # Dase plug unplug Subplot
            ax1.step(df2_temp['time'], df2_temp['EVNT'], marker='o', linestyle='-', color='purple', label=vin, where='post')
            ax1.legend()
            ax1.invert_yaxis()
            ax1.set_ylabel('EVNT')
            ax1.tick_params(axis='x', rotation=45)
            ax1.set_title('Plug and unplug graph Dase')

            #Avenir plug unplug Subplot
            ax2.step(df1_temp['time'], df1_temp['EVNT'], marker='o', linestyle='-', color='b', label=vin, where='post')
            ax2.legend()
            ax2.invert_yaxis()
            ax2.set_ylabel('EVNT')
            ax2.tick_params(axis='x', rotation=45)
            ax2.set_title('Plug and unplug graph Avenir')
        
            # First avenir data subplot
            ax3.plot(df_79_temp['HEAD_COLL_TIMS'], df_79_temp[col_name1], marker='o', linestyle='-', color='green')
            ax3.set_title('Evolution du SOC avec la colonne ' + col_name1 + " venant du message 79  -- AVENIR --")
            ax3.set_xlabel('Time')
            ax3.set_ylabel('State of Charge (%)')
            ax3.grid(True)
            ax3.tick_params(axis='x', rotation=45)
            
            #Second avenir data subplot
            ax4.plot(df_74_temp['HEAD_COLL_TIMS'], df_74_temp[col_name2], marker='o', linestyle='-', color='cyan')
            ax4.set_title('Evolution du SOC avec la colonne ' + col_name2 + " venant du message 74  -- AVENIR --")
            ax4.set_xlabel('Time')
            ax4.set_ylabel('State of Charge (%)')
            ax4.grid(True)
            ax4.tick_params(axis='x', rotation=45)
            
            #Third avenir data subplot
            ax5.plot(df_74_temp['HEAD_COLL_TIMS'], df_74_temp[col_name5], marker='o', linestyle='-', color='#f0006a')
            ax5.set_title('Evolution des kilometres parcourus avec la colonne ' + col_name5 + " venant du message 74 -- AVENIR -- ")
            ax5.set_xlabel('Time')
            ax5.set_ylabel('KM')
            ax5.grid(True)
            ax5.tick_params(axis='x', rotation=45)
            
            #First dase data subplot
            ax6.plot(df_68_temp['HEAD_COLL_TIMS'], df_68_temp[col_name3], marker='o', linestyle='-', color='orange')
            ax6.set_title('Evolution du SOC avec la colonne ' + col_name3 + " venant du message 68  -- DASE -- ")
            ax6.set_xlabel('Time')
            ax6.set_ylabel('State value (0,1,3,4)')
            ax6.grid(True)
            ax6.tick_params(axis='x', rotation=45)
            
            #Second dase data subplot
            ax7.plot(df_79_temp['HEAD_COLL_TIMS'], df_79_temp[col_name4], marker='o', linestyle='-', color='#ff2a2e')
            ax7.set_title('Evolution du SOC avec la colonne ' + col_name4 + " venant du message 79  -- DASE -- ")
            ax7.set_xlabel('Time')
            ax7.set_ylabel('State value (0,1,3,4)')
            ax7.grid(True)
            ax7.tick_params(axis='x', rotation=45)
            
            #Third dase data subplot
            ax8.plot(df_68_temp['HEAD_COLL_TIMS'], df_68_temp[col_name6], marker='o', linestyle='-', color='#96a816')
            ax8.set_title('Niveau de charge ' + col_name6 + " venant du message 68  -- DASE -- ")
            ax8.set_xlabel('Time')
            ax8.set_ylabel('KM')
            ax8.grid(True)
            ax8.tick_params(axis='x', rotation=45)
            
            # Limiter l'intervalle de temps si spécifié
            if start_time is not None and end_time is not None:
                ax8.set_xlim(start_time_datetime, end_time_datetime)

            plt.tight_layout()

            plt.show()
            print("----------------------------------------------------------------------------------------------------------------------------------------------------------------")


# COMMAND ----------

import matplotlib.pyplot as plt

def plot_comparison(df_dase, df_avenir, list_van, start_time=None, end_time=None):
    for i, van in enumerate(list_van):

        df_temp_dase = df_dase[df_dase['HEAD_VAN'] == van].sort_values(by='time')
        df_temp_avenir = df_avenir[df_avenir['HEAD_VAN'] == van].sort_values(by='time')

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

        ax1.step(df_temp_dase['time'], df_temp_dase['EVNT'], marker='o', linestyle='-', color='purple', label=van, where='post', alpha=0.5)
        ax1.invert_yaxis()
        ax1.set_ylabel('EVNT')
        ax1.set_title('Plug and unplug graph Dase')

        ax_twinx = ax1.twinx()
        ax_twinx.bar(x=df_temp_dase['time'], height=df_temp_dase['Diff_soc'], width=0.1, color='orange', alpha=1,label="Quantité de soc gagné durant la charge")

        ax2.step(df_temp_avenir['time'], df_temp_avenir['EVNT'], marker='o', linestyle='-', color='b', label=van, where='post', alpha=0.5)
        ax2.invert_yaxis()
        ax2.set_ylabel('EVNT')
        ax2.set_title('Plug and unplug graph Avenir')

        ax_twinx2 = ax2.twinx()
        ax_twinx2.bar(x=df_temp_avenir['time'], height=df_temp_avenir['Diff_soc'], width=0.1, color='orange', alpha=1, label="Quantité de SOC gagné durant la charge")
        ax_twinx2.set_ylim(0, 100)

        # Positionnement des légendes
        ax1.legend(loc='upper right', bbox_to_anchor=(1, 1))
        ax_twinx.legend(loc='upper right', bbox_to_anchor=(1, 0.8))
        ax2.legend(loc='upper right', bbox_to_anchor=(1, 1))
        ax_twinx2.legend(loc='upper right', bbox_to_anchor=(1, 0.8))
        ax1.tick_params(axis='x', rotation=45)
        ax2.tick_params(axis='x', rotation=45)
        plt.tight_layout()
        plt.show()
        print("----------------------------------------------------------------------------------------------------------------------------------------------------------------")



# COMMAND ----------

df_dase_transfo = transform_to_plot(df_dase_spark_ready)
df_avenir_transfo = transform_to_plot(df_avenir_spark_ready)

# COMMAND ----------

# On met le doublon à 0 pour la difference de soc lorsqu'il y a une decharge
df_dase_plot = df_dase_transfo.withColumn("prev_value", lag("Diff_soc", 1).over(Window.orderBy("HEAD_VAN", "SESSION_START")))
df_dase_plot = df_dase_plot.withColumn("Diff_soc", when(col("Diff_soc") == col("prev_value"), 0).otherwise(col("Diff_soc")))
df_dase_plot = df_dase_plot.drop("prev_value")

df_avenir_plot = df_avenir_transfo.withColumn("prev_value", lag("Diff_soc", 1).over(Window.orderBy("HEAD_VAN", "SESSION_START")))
df_avenir_plot = df_avenir_plot.withColumn("Diff_soc", when(col("Diff_soc") == col("prev_value"), 0).otherwise(col("Diff_soc")))
df_avenir_plot = df_avenir_plot.drop("prev_value")

# COMMAND ----------

plot_comparison(df_dase_plot.toPandas(),df_avenir_plot.toPandas(),list_vin_Avenir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etude générale

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Valeures possible pour BATT_CHRG_STT :
# MAGIC <br>
# MAGIC 0 : Disconnected <br>
# MAGIC 1 : In progress <br>
# MAGIC 2 : Failure <br>
# MAGIC 3 : Stopped <br>
# MAGIC 4 : Finished <br>
# MAGIC 5 : Reserved <br>
# MAGIC 6 : Reserved <br>
# MAGIC 7 : Reserved<br>

# COMMAND ----------

# DBTITLE 1,a
plot(df_avenir_transfo.toPandas(), df_dase_transfo.toPandas(),df_68_ano.toPandas(), df_74_ano.toPandas(),df_79_ano.toPandas(), list_vin_Avenir)

# COMMAND ----------

# MAGIC %md
# MAGIC Etude sur la charge de 70h (outlier Avenir)

# COMMAND ----------

plot(df_avenir_transfo.toPandas(), df_dase_transfo.toPandas(),df_68_ano.toPandas(), df_74_ano.toPandas(),df_79_ano.toPandas(), ["5c3d74294674c3060766e160b134d8ce"],"2022-01-05","2022-01-09")

# COMMAND ----------

# MAGIC %md
# MAGIC Etude sur le véhicule manquant coté Dase

# COMMAND ----------

plot(df_avenir_transfo.toPandas(), df_dase_transfo.toPandas(),df_68_ano.toPandas(), df_74_ano.toPandas(),df_79_ano.toPandas(), ["97574ea267e2a878326c856b7dd92f79"])

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



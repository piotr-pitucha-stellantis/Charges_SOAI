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
import matplotlib.pyplot as plt
import seaborn as sns
# import openpyxl

# COMMAND ----------

nrvqtmap02 = spark.table("ods.corvet_ict.nrv_bi_nrvqtmap02")
nrvqt01 = spark.table("ods.corvet_ict.nrv_bi_nrvqt01")
nrvqt21 = spark.table("ods.corvet_ict.nrv_bi_nrvqt21")
nrvqt32 = spark.table("ods.corvet_ict.nrv_bi_nrvqt32")

# COMMAND ----------

df = nrvqt01
df.persist()

# COMMAND ----------

# Recuperation pas a pas d'un seul attribut CODE_CARACTERISTIQUE sur une liste de vin 
CODE_CARACTERISTIQUE = "DXD"
# Recuperation de la position de la caracteristique dans LCDV_ETENTDU
IDX_CARACTERISTIQUE = nrvqtmap02.filter(F.col("CARACTERISTIQUE") == CODE_CARACTERISTIQUE).select("IDX").first()[0]
 
df_ = df.select("VIN","LCDV_ETENDU","FAMILLE")
# Recuperation de la valeur et de l'indice dans LCDV_ETENTDU a partir de la position IDX_CARACTERISTIQUE
df_ = df_.withColumn('VALEUR',df_.LCDV_ETENDU.substr((3*(IDX_CARACTERISTIQUE-1))+1,2)).filter(F.col("VALEUR") == "04")
df_ = df_.withColumn('INDICE',df_.LCDV_ETENDU.substr((3*(IDX_CARACTERISTIQUE-1))+3,1))
 
# Jointure avec nrvqt21 pour recuperer les libelles (DESIGNATION_CARACTERISTIQUE, DESIGNATION_VALEUR)
df_ = df_.join(nrvqt21.filter(F.col("CARACTERISTIQUE") == CODE_CARACTERISTIQUE), ["VALEUR","FAMILLE"], how='left')
 

# COMMAND ----------

df_VIN = df_.select("VIN").distinct()

# COMMAND ----------

display(df_VIN)

# COMMAND ----------

df_VIN.count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



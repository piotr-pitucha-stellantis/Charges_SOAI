# Databricks notebook source
# MAGIC %md
# MAGIC # Charge session data generation 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text('workspace', '/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/Processed_data/avenir')

# COMMAND ----------

dbutils.widgets.text('S3', 's3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/')

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
import os
import json
from datetime import datetime
import time

# import openpyxl

# COMMAND ----------

path_chargeTwo = dbutils.widgets.get('S3')+'Avenir/ChargeTwo_test/'
path_chargeTwo

# COMMAND ----------

df_charge = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_chargeTwo)
)


# COMMAND ----------

df_vehicle_status=spark.read.parquet(dbutils.widgets.get('S3')+ "/Raw/avenir/df_74")

# COMMAND ----------

# modif smaurel 20220622 prise en compte du head_session
# df_vehicle_status=df_vehicle_status.dropDuplicates(['HEAD_VIN','HEAD_MESS_ID']).orderBy('HEAD_VIN','HEAD_MESS_ID')
df_vehicle_status=df_vehicle_status.dropDuplicates(['HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID']).orderBy('HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID')

df_vehicle_status=df_vehicle_status.withColumnRenamed('HEAD_VIN','VIN')


# COMMAND ----------

# modif smaurel 20220622
# charge_join=df_charge.join(df_vehicle_status.select('VIN','HEAD_COLL_TIMS','VEHC_STTS_CHRG_STT','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','HEAD_MESS_ID'), 
#                            on=[df_vehicle_status['VIN']==df_charge['HEAD_VIN'],
#                                df_vehicle_status['HEAD_COLL_TIMS'].between(df_charge['START'],df_charge['STOP'])], how='leftouter')
charge_join=df_charge.join(df_vehicle_status.select('VIN','HEAD_COLL_TIMS','VEHC_STTS_CHRG_STT','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','HEAD_MESS_ID','HEAD_SESS_ID'), 
                           on=[df_vehicle_status['VIN']==df_charge['HEAD_VIN'],
                               df_vehicle_status['HEAD_COLL_TIMS'].between(df_charge['START'],df_charge['STOP'])], how='leftouter')

# COMMAND ----------

# modif smaurel 20220622
# charge_join_gb=charge_join.groupBy('HEAD_VIN','START').agg(
#     F.min('HEAD_MESS_ID').alias('msg_id_start'),
#     F.max('HEAD_MESS_ID').alias('msg_id_stop'))

# il faudrait récupérer les id (sess_ID + mess_ID) du premier message et du dernier message de la charge.

# je vais faire le traitement en deux étapes la première sur les premiers messages, la dernière sur les derniers messages

# récupérer les id (sess_ID + mess_ID) du premier message 

# charge_join_gb=charge_join.groupBy('HEAD_VIN','START').orderBy('HEAD_SESS_ID','HEAD_MESS_ID').
window_charge = Window.partitionBy('HEAD_VIN','START').orderBy('HEAD_SESS_ID','HEAD_MESS_ID')
charge_join_w1 = (
                    charge_join
                    .select('HEAD_VIN','START','STOP','HEAD_SESS_ID','HEAD_MESS_ID','HEAD_COLL_TIMS')
                    .withColumn("row_number", row_number().over(window_charge))
#                     .withColumn("max_row_number", F.max('row_number').over(window_charge))
                    .filter(F.col('row_number')==1)
                    .withColumnRenamed('HEAD_SESS_ID','HEAD_SESS_ID_start')
                    .withColumnRenamed('HEAD_MESS_ID','HEAD_MESS_ID_start')
                    .withColumnRenamed('HEAD_COLL_TIMS','HEAD_COLL_TIMS_start')
                    .drop('row_number')
                )


# COMMAND ----------

# jointure avec df74 pour obtenir les températures, autonomie et soc sur le premier message de chaque charge
df_charge_join_1=(charge_join_w1.join(
                                df_vehicle_status.select('VIN','HEAD_COLL_TIMS','HEAD_SESS_ID','HEAD_MESS_ID','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','VEHC_STTS_CHRG_STT'),
                                      on=[
                                          charge_join_w1['HEAD_VIN']==df_vehicle_status['VIN'],
                                          charge_join_w1['HEAD_SESS_ID_start']==df_vehicle_status['HEAD_SESS_ID'],
                                          charge_join_w1['HEAD_MESS_ID_start']==df_vehicle_status['HEAD_MESS_ID']
                                      ],how='Leftouter')
                            .withColumnRenamed('VEHC_STTS_OUTS_TEMP','TEMP_START_ID')
                            .withColumnRenamed('VEHC_STTS_ZEV_AUTN', 'AUTONOMY_START_ID')
                            .withColumnRenamed('VEHC_STTS_CHRG_STT','SOC_START_ID')
                            .drop('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID','HEAD_SESS_ID')
                 )

# COMMAND ----------

window_charge_desc = Window.partitionBy('HEAD_VIN','START').orderBy(F.desc('HEAD_SESS_ID'),F.desc('HEAD_MESS_ID'))
charge_join_w2 = (
                    charge_join
                    .select('HEAD_VIN','START','STOP','HEAD_SESS_ID','HEAD_MESS_ID','HEAD_COLL_TIMS')
                    .withColumn("row_number", row_number().over(window_charge_desc))
#                     .withColumn("max_row_number", F.max('row_number').over(window_charge))
                    .filter(F.col('row_number')==1)
                    .withColumnRenamed('HEAD_SESS_ID','HEAD_SESS_ID_stop')
                    .withColumnRenamed('HEAD_MESS_ID','HEAD_MESS_ID_stop')
                    .withColumnRenamed('HEAD_COLL_TIMS','HEAD_COLL_TIMS_stop')
                    .drop('row_number')
                )

# COMMAND ----------

# jointure avec df74 pour obtenir les températures, autonomie et soc sur le dernier message de chaque charge
df_charge_join_2=(charge_join_w2.join(
                                df_vehicle_status.select('VIN','HEAD_COLL_TIMS','HEAD_SESS_ID','HEAD_MESS_ID','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','VEHC_STTS_CHRG_STT'),
                                      on=[
                                          charge_join_w2['HEAD_VIN']==df_vehicle_status['VIN'],
                                          charge_join_w2['HEAD_SESS_ID_stop']==df_vehicle_status['HEAD_SESS_ID'],
                                          charge_join_w2['HEAD_MESS_ID_stop']==df_vehicle_status['HEAD_MESS_ID']
                                      ],how='Leftouter')
                            .withColumnRenamed('VEHC_STTS_OUTS_TEMP','TEMP_STOP_ID')
                            .withColumnRenamed('VEHC_STTS_ZEV_AUTN', 'AUTONOMY_STOP_ID')
                            .withColumnRenamed('VEHC_STTS_CHRG_STT','SOC_STOP_ID')
                            .drop('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID','HEAD_SESS_ID')
                )

# COMMAND ----------

df_charge_join_final = (df_charge_join_1.select('HEAD_VIN','START','HEAD_SESS_ID_start','HEAD_MESS_ID_start','TEMP_START_ID','AUTONOMY_START_ID','SOC_START_ID')
                            .join(df_charge_join_2.select('HEAD_VIN','START','HEAD_SESS_ID_stop','HEAD_MESS_ID_stop','TEMP_STOP_ID','AUTONOMY_STOP_ID','SOC_STOP_ID'),
                                            on=['HEAD_VIN','START'
#                                                   df_charge_join_1['HEAD_VIN'] == df_charge_join_2['HEAD_VIN'],
#                                                   df_charge_join_1['START'] == df_charge_join_2['START']
                                              ],
                                            how='Leftouter')
#                                         .withColumnRenamed('VEHC_STTS_OUTS_TEMP','TEMP_STOP_ID')
#                                         .withColumnRenamed('VEHC_STTS_ZEV_AUTN', 'AUTONOMY_STOP_ID')
#                                         .withColumnRenamed('VEHC_STTS_CHRG_STT','SOC_STOP_ID')
#                                         .drop('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID')
                       )

# COMMAND ----------

# jointure avec le dataframe charge_two pour ajouter les nouvelles colonnes

df_charge_join_4=df_charge.join(df_charge_join_final, on=['HEAD_VIN','START'], how='leftouter')

# COMMAND ----------

#selection des signaux à enregistrer
# df_to_save=df_charge_join_4.select('HEAD_VIN','START','STOP','msg_id_start','msg_id_stop','TEMP_START_ID','TEMP_STOP_ID','AUTONOMY_START_ID','AUTONOMY_STOP_ID',
#                      'SOC_START','SOC_STOP')

df_to_save=df_charge_join_4.select('HEAD_VIN','START','STOP','SOC_START','SOC_STOP','SOC_START_ID','SOC_STOP_ID',
                                   'TEMP_START_ID','TEMP_STOP_ID','AUTONOMY_START_ID','AUTONOMY_STOP_ID',
                     )


# COMMAND ----------

path_result2 = dbutils.widgets.get('S3')+'Avenir/ChargeThree01_test/'
path_result2

# COMMAND ----------

df_to_save=df_to_save.dropDuplicates()
df_to_save.write.mode('overwrite').parquet(path_result2)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # NEW_END

# COMMAND ----------

df_charge_join_1=charge_join_gb.join(df_vehicle_status.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','VEHC_STTS_CHRG_STT'),
                                      on=[charge_join_gb['HEAD_VIN']==df_vehicle_status['VIN'],
                                      charge_join_gb['msg_id_start']==df_vehicle_status['HEAD_MESS_ID']],how='Leftouter').withColumnRenamed(
                                        'VEHC_STTS_OUTS_TEMP','TEMP_START_ID').withColumnRenamed(
                                        'VEHC_STTS_ZEV_AUTN', 'AUTONOMY_START_ID').withColumnRenamed(
                                        'VEHC_STTS_CHRG_STT','SOC_START_ID').drop('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID')



# COMMAND ----------

df_charge_join_2=df_charge_join_1.join(df_vehicle_status.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','VEHC_STTS_CHRG_STT'),
                                      on=[df_charge_join_1['HEAD_VIN']==df_vehicle_status['VIN'],
                                      df_charge_join_1['msg_id_stop']==df_vehicle_status['HEAD_MESS_ID']],how='Leftouter').withColumnRenamed(
                                        'VEHC_STTS_OUTS_TEMP','TEMP_STOP_ID').withColumnRenamed(
                                        'VEHC_STTS_ZEV_AUTN', 'AUTONOMY_STOP_ID').withColumnRenamed(
                                        'VEHC_STTS_CHRG_STT','SOC_STOP_ID').drop('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ci-dessous le problème avec un passage d'un nombre de ligne de 92 à 59077

# COMMAND ----------

df_charge_join=df_charge_join.join(df_vehicle_status.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID','VEHC_STTS_OUTS_TEMP','VEHC_STTS_ZEV_AUTN','VEHC_STTS_CHRG_STT'),
                                      on=[df_charge_join['HEAD_VIN']==df_vehicle_status['VIN'],
                                      df_charge_join['msg_id_stop']==df_vehicle_status['HEAD_MESS_ID']],how='Leftouter').withColumnRenamed(
                                        'VEHC_STTS_OUTS_TEMP','TEMP_STOP_ID').withColumnRenamed(
                                        'VEHC_STTS_ZEV_AUTN', 'AUTONOMY_STOP_ID').withColumnRenamed(
                                        'VEHC_STTS_CHRG_STT','SOC_STOP_ID').drop('VIN','HEAD_COLL_TIMS')

# COMMAND ----------

df_charge_join_4=df_charge.join(df_charge_join, on=['HEAD_VIN','START'], how='leftouter')

# COMMAND ----------

df_to_save=df_charge_join_4.select('HEAD_VIN','START','STOP','msg_id_start','msg_id_stop','TEMP_START_ID','TEMP_STOP_ID','AUTONOMY_START_ID','AUTONOMY_STOP_ID',
                     'SOC_START','SOC_STOP')

# COMMAND ----------

path_result3 = dbutils.widgets.get('S3')+'Avenir/ChargeThree02_test/'
path_result3

# COMMAND ----------

df_to_save=df_to_save.dropDuplicates()
df_to_save.write.mode('overwrite').parquet(path_result3)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



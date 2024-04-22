# Databricks notebook source
# MAGIC %md
# MAGIC # Charge session data generation 2

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

df_10_workspace = pd.read_csv('/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/Processed_data/avenir/df_10_vin.csv')
liste_vin_10 = df_10_workspace['VIN'].tolist()

# COMMAND ----------

df_79=spark.read.parquet(dbutils.widgets.get('S3')+ "/Raw/avenir/df_79")
df_79 = df_79.withColumnRenamed('HEAD_VIN','VIN')
df_79=df_79.dropDuplicates(['VIN','HEAD_MESS_ID']).orderBy('VIN','HEAD_MESS_ID')

# COMMAND ----------

path_chargeone = dbutils.widgets.get('S3')+'Avenir/ChargeOne_test/'
path_chargeone

# COMMAND ----------

charge = (spark.read
  .format("parquet")
  .option("header",True)
  .load(path_chargeone)
)
charge=charge.dropDuplicates(['HEAD_VIN','CHARGE_START']).orderBy('HEAD_VIN','CHARGE_START')

# COMMAND ----------

charge_join=charge.join(df_79.select('VIN','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID'), on=[df_79['VIN']==charge['HEAD_VIN'], 
                                                                                            df_79['HEAD_COLL_TIMS'].between(charge['CHARGE_START'],charge['CHARGE_STOP'])
                                                                                           ], how='leftouter')


# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE ONE DATAFRAME BY CHARGE STATUS INTO CHARGE SESSION

# COMMAND ----------

df_nocharging=charge_join.select('HEAD_VIN','CHARGE_START','CHARGE_STOP','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').filter(F.col('BATT_STTS_SUMM_CHRG_STT')==0).orderBy('HEAD_VIN','CHARGE_START','HEAD_COLL_TIMS').groupby(
                    'HEAD_VIN','CHARGE_START','CHARGE_STOP').agg(F.min('HEAD_MESS_ID').alias('NO_CHARGING_START'),F.max('HEAD_MESS_ID').alias('NO_CHARGING_STOP'))
df_nocharging=df_nocharging.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_nocharging['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_nocharging['NO_CHARGING_START']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_NO_CHARGING_START').drop('HEAD_MESS_ID','VIN').join(
df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_nocharging['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_nocharging['NO_CHARGING_STOP']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_NO_CHARGING_STOP').drop('HEAD_MESS_ID','VIN')
#df_nocharging.show(10,False)

# COMMAND ----------

df_nocharging=charge_join.select('HEAD_VIN','CHARGE_START','CHARGE_STOP','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').filter(F.col('BATT_STTS_SUMM_CHRG_STT')==0).orderBy('HEAD_VIN','CHARGE_START','HEAD_COLL_TIMS').groupby(
                    'HEAD_VIN','CHARGE_START','CHARGE_STOP').agg(F.min('HEAD_MESS_ID').alias('NO_CHARGING_START'),F.max('HEAD_MESS_ID').alias('NO_CHARGING_STOP'))
df_nocharging=df_nocharging.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_nocharging['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_nocharging['NO_CHARGING_START']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_NO_CHARGING_START').drop('HEAD_MESS_ID','VIN').join(
df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_nocharging['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_nocharging['NO_CHARGING_STOP']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_NO_CHARGING_STOP').drop('HEAD_MESS_ID','VIN')
#df_nocharging.show(10,False)

# COMMAND ----------

df_ongoing=charge_join.select('HEAD_VIN','CHARGE_START','CHARGE_STOP','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').filter(F.col('BATT_STTS_SUMM_CHRG_STT')==1).orderBy('HEAD_VIN','CHARGE_START','HEAD_COLL_TIMS').groupby(
                    'HEAD_VIN','CHARGE_START','CHARGE_STOP').agg(F.min('HEAD_MESS_ID').alias('ONGOING_START'),F.max('HEAD_MESS_ID').alias('ONGOING_STOP'))
df_ongoing=df_ongoing.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_ongoing['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_ongoing['ONGOING_START']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_ONGOING_START').drop('HEAD_MESS_ID','VIN').join(
df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_ongoing['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_ongoing['ONGOING_STOP']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_ONGOING_STOP').drop('HEAD_MESS_ID','VIN')
#df_ongoing.show(10,False)

# COMMAND ----------

df_failure=charge_join.select('HEAD_VIN','CHARGE_START','CHARGE_STOP','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').filter(F.col('BATT_STTS_SUMM_CHRG_STT')==2).orderBy('HEAD_VIN','CHARGE_START','HEAD_COLL_TIMS').groupby(
                    'HEAD_VIN','CHARGE_START','CHARGE_STOP').agg(F.min('HEAD_MESS_ID').alias('FAILURE_START'),F.max('HEAD_MESS_ID').alias('FAILURE_STOP'))
df_failure=df_failure.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_failure['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_failure['FAILURE_START']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_FAILURE_START').drop('HEAD_MESS_ID','VIN').join(
df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_failure['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_failure['FAILURE_STOP']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_FAILURE_STOP').drop('HEAD_MESS_ID','VIN')

#df_failure.show(10,False)

# COMMAND ----------

df_remote=charge_join.select('HEAD_VIN','CHARGE_START','CHARGE_STOP','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').filter(F.col('BATT_STTS_SUMM_CHRG_STT')==3).orderBy('HEAD_VIN','CHARGE_START','HEAD_COLL_TIMS').groupby(
                    'HEAD_VIN','CHARGE_START','CHARGE_STOP').agg(F.min('HEAD_MESS_ID').alias('REMOTE_START'),F.max('HEAD_MESS_ID').alias('REMOTE_STOP'))
df_remote=df_remote.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_remote['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_remote['REMOTE_START']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_REMOTE_START').drop('HEAD_MESS_ID','VIN').join(
    df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_remote['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_remote['REMOTE_STOP']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_REMOTE_STOP').drop('HEAD_MESS_ID','VIN')

#df_remote.show(10,False)

# COMMAND ----------

df_finished=charge_join.select('HEAD_VIN','CHARGE_START','CHARGE_STOP','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').filter(F.col('BATT_STTS_SUMM_CHRG_STT')==4).orderBy('HEAD_VIN','CHARGE_START','HEAD_COLL_TIMS').groupby(
                    'HEAD_VIN','CHARGE_START','CHARGE_STOP').agg(F.min('HEAD_MESS_ID').alias('FINISHED_START'),F.max('HEAD_MESS_ID').alias('FINISHED_STOP'))
df_finished=df_finished.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_finished['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_finished['FINISHED_START']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_FINISHED_START').drop('HEAD_MESS_ID','VIN').join(
    df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==df_finished['HEAD_VIN'],df_79['HEAD_MESS_ID']==df_finished['FINISHED_STOP']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_FINISHED_STOP').drop('HEAD_MESS_ID','VIN')

#df_finished.show(10,False)

# COMMAND ----------

condition=['HEAD_VIN','CHARGE_START','CHARGE_STOP']
charge_join_2=charge.join(df_nocharging,on=condition, how='leftouter')
charge_join_2=charge_join_2.join(df_ongoing,on=condition, how='leftouter')
charge_join_2=charge_join_2.join(df_failure,on=condition, how='leftouter')
charge_join_2=charge_join_2.join(df_remote,on=condition, how='leftouter')
charge_join_2=charge_join_2.join(df_finished,on=condition, how='leftouter')

# COMMAND ----------

# MAGIC %md
# MAGIC # SELECT CHARGE SESSION START AND STOP

# COMMAND ----------

charge_join_3=charge_join_2.withColumn('START',
                                          when(F.col('T_ONGOING_START').isNotNull(),F.col('T_ONGOING_START')).otherwise(F.col('CHARGE_START'))
                                          )
charge_join_3=charge_join_3.withColumn('STOP',
                                        when(F.col('T_FINISHED_START').isNotNull(),F.col('T_FINISHED_START')).otherwise(F.col('CHARGE_STOP'))
                                        )

# COMMAND ----------

# MAGIC %md
# MAGIC # SAVE DATA

# COMMAND ----------

path_result = dbutils.widgets.get('S3')+'Avenir/ChargeTwoInter_test/'
path_result

# COMMAND ----------

charge_join_3.write.mode("overwrite").parquet(path_result)

# COMMAND ----------

# CREATE ONE DATAFRAME BY CHARGE STATUS EXT TO CHARGE SESSION

# COMMAND ----------

charge=spark.read.parquet(path_result)
charge=charge.dropDuplicates(['HEAD_VIN','CHARGE_START']).orderBy('HEAD_VIN','CHARGE_START')

# COMMAND ----------

#Création d'une ID par ligne
charge=charge.withColumn("CHARGE_ID", F.row_number().over(Window.partitionBy().orderBy(['HEAD_VIN','START'])))
#charge=charge.withColumnRenamed('START','HEAD_COLL_TIMS')

# COMMAND ----------

df_79=df_79.withColumnRenamed('VIN','HEAD_VIN')
charge=charge.withColumn('START_2',F.col('START'))
df_bat_sum=df_79.select('HEAD_VIN','HEAD_COLL_TIMS','BATT_STTS_SUMM_CHRG_STT','HEAD_MESS_ID').join(charge.withColumnRenamed('START_2','HEAD_COLL_TIMS').select('HEAD_VIN','HEAD_COLL_TIMS','START','STOP','CHARGE_ID'),on=['HEAD_VIN','HEAD_COLL_TIMS'], how='outer').orderBy('HEAD_VIN','HEAD_COLL_TIMS')

# COMMAND ----------

#Ajouter un id
df_bat_sum=df_bat_sum.withColumn("id", F.last('CHARGE_ID', True).over(Window.partitionBy().orderBy(['HEAD_VIN', 'HEAD_COLL_TIMS']).rowsBetween(-sys.maxsize, 0)))
df_bat_sum=df_bat_sum.withColumn("Stop_id", F.last('STOP', True).over(Window.partitionBy().orderBy(['HEAD_VIN', 'HEAD_COLL_TIMS']).rowsBetween(-sys.maxsize, 0)))
df_bat_sum=df_bat_sum.withColumn("Start_id", F.last('START', True).over(Window.partitionBy().orderBy(['HEAD_VIN', 'HEAD_COLL_TIMS']).rowsBetween(-sys.maxsize, 0)))

# COMMAND ----------

window=Window.partitionBy('id').orderBy(['HEAD_VIN','HEAD_COLL_TIMS'])
df_ongoing_2=df_bat_sum.withColumn('BATT_STTS_SUMM_CHRG_STT_LEAD', F.lead(F.col('BATT_STTS_SUMM_CHRG_STT')).over(window))

# COMMAND ----------

df_ongoing_2=df_ongoing_2.filter((F.col('BATT_STTS_SUMM_CHRG_STT')==1)&(F.col('BATT_STTS_SUMM_CHRG_STT_LEAD')==0)&(F.col('HEAD_COLL_TIMS')>F.col('Stop_id'))).groupBy('id').agg(F.min('HEAD_MESS_ID').alias('ON_GOING_2')).orderBy(desc('id'))

# COMMAND ----------

charge=charge.join(df_ongoing_2, on=[charge['CHARGE_ID']==df_ongoing_2['id']], how='leftouter').drop('id')

# COMMAND ----------

#FINISH STATUS AFTER END OF CHARGE LEVEL 1

# COMMAND ----------

df_finish_2=df_bat_sum.filter((F.col('BATT_STTS_SUMM_CHRG_STT')==4)&(F.col('HEAD_COLL_TIMS')>F.col('Stop_id'))).groupBy('id').agg(F.min('HEAD_MESS_ID').alias('END_CHARGE_2'))

# COMMAND ----------

charge=charge.join(df_finish_2, on=[charge['CHARGE_ID']==df_finish_2['id']], how='leftouter')

# COMMAND ----------

df_79=df_79.withColumnRenamed('HEAD_VIN', 'VIN')
charge=charge.join(df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==charge['HEAD_VIN'],df_79['HEAD_MESS_ID']==charge['END_CHARGE_2']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_END_CHARGE_2').drop('VIN','HEAD_MESS_ID').join(
df_79.select('VIN','HEAD_COLL_TIMS','HEAD_MESS_ID'), on=[df_79['VIN']==charge['HEAD_VIN'],df_79['HEAD_MESS_ID']==charge['ON_GOING_2']], how='leftouter').withColumnRenamed('HEAD_COLL_TIMS','T_ON_GOING_2').drop('HEAD_MESS_ID','VIN')

# COMMAND ----------

charge.persist()

# COMMAND ----------

charge=charge.withColumn('Diffhours_end', (F.col('T_END_CHARGE_2').cast('long')-F.col('STOP').cast('long'))/3600)
charge=charge.withColumn('Diffhours_ongoing', (F.col('T_ON_GOING_2').cast('long')-F.col('STOP').cast('long'))/3600)

# COMMAND ----------

charge=charge.withColumn('STOP_2', F.least(F.col('T_END_CHARGE_2'),F.col('T_ON_GOING_2')))

# COMMAND ----------

charge=charge.withColumn('Diffhours_stop', (F.col('STOP_2').cast('long')-F.col('STOP').cast('long'))/3600)

# COMMAND ----------

charge_3=charge.withColumn('STOP',
                                        when(((F.col('T_FINISHED_START').isNull())&(F.col('STOP_2').isNotNull())&(F.col('Diffhours_stop')<2)),F.col('STOP_2')).otherwise(F.col('STOP'))
                                        )

# COMMAND ----------

path_result2 = dbutils.widgets.get('S3')+'Avenir/ChargeTwo_test/'
path_result2

# COMMAND ----------

charge_3.write.mode("overwrite").parquet(path_result2)

# COMMAND ----------

charge.unpersist()
charge_3.unpersist()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



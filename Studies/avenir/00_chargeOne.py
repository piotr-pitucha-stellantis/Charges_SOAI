# Databricks notebook source
# MAGIC %md
# MAGIC # Charge session data generation 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text('Data_path_S3', 's3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/')

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

# MAGIC %md
# MAGIC ## VIN

# COMMAND ----------

# path =dbutils.widgets.get('Data_path_S3') + "Studies/Avenir/csv/df_10_vin.csv"
# df_vin_spk = (spark.read
#     .format("csv")
#     .option("header",True)
#     .option("index", False)
#     .options(delimiter=',')
#     .load(path)
#     )
# df_vin = df_vin_spk.toPandas()

# COMMAND ----------

liste_vin_10 = ["VR3UKZKXZLJ787446", "VR1UJZKXZMW058021", "VR3UHZKXZLT012976", "VR1UJZKXZKW118622", "VR3UHZKXZMT031912", "VR1UJZKXZMW083478", "VR3UHZKXZLT010961", "VR3UHZKXZLT100617", "VR3UHZKXZKT136380", "VR3UHZKXZMT030960"]


# COMMAND ----------

liste_vin_10

# COMMAND ----------

start_date = "2022-01-01"
end_data = "2022-02-01"

# COMMAND ----------

df_79 = tcv.read(
        spark, 79,start_date,end_data, False, liste_vin_10, 'carbide'
    )
df_79.write.mode("overwrite").parquet(dbutils.widgets.get('Data_path_S3') + "Studies/Avenir/parquet/79/df_79")

# COMMAND ----------

df_74 = tcv.read(
        spark, 74, start_date, end_data, False, liste_vin_10, 'carbide'
    )
df_74.write.mode("overwrite").parquet(dbutils.widgets.get('Data_path_S3') + "Studies/Avenir/parquet/74/df_74")

# COMMAND ----------

trip = spark.table("gadp_cdm.ods_cdm_ignition_cycles.ods_cvh01_analytics_carbide_historicals")

# COMMAND ----------

display(trip)

# COMMAND ----------

trip_10 =trip.filter(F.col("VIN").isin(liste_vin_10))

# COMMAND ----------

display(trip_10)

# COMMAND ----------

trip_10.select("VIN").distinct().count()

# COMMAND ----------

path_result = dbutils.widgets.get('Data_path_S3') + 'Studies/Avenir/parquet/ChargeOne_test/'
path_result

# COMMAND ----------


start_time = time.time()


# Message 74 vehicles status
vehicle_statuts=spark.read.parquet(dbutils.widgets.get('Data_path_S3') + "Studies/Avenir/parquet/74/df_74")

table_shape=vehicle_statuts.select('HEAD_VIN').distinct().count()
print("number of vin : " +str(table_shape))

#SELECT RELEVANT COLUMNS FOR DATA PROCESSING
df_vehicle_statuts=vehicle_statuts.select('HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID','HEAD_COLL_TIMS','VEHC_STTS_LIFT_MILG','VEHC_STTS_CHRG_STT')

#PERSIST DATA TO IMPROVE PROCESSING SPEED
df_vehicle_statuts.persist()

#READ DASE DATABASE TO IDENTIFY TRIP SESSIONS 
df_trips = trip_10
df_trips.persist()
#ADJUST TRIP START AND STOP TIMES TO MAKE SURE WE DON'T TOUCH TO CHARGE SESSION
df_trips=df_trips.withColumn('StartCalc_shift',F.col('DATE_STRT')+F.expr('INTERVAL 10 seconds')
                            ).withColumn('StopCalc_shift',F.col('DATE_END')-F.expr('INTERVAL 10 seconds'))

# JOIN RAW DATA AND TRIP TABLE WITH STANDARD PARAMATERS
df_vehicle_statuts_trips=df_vehicle_statuts.join(df_trips, on = 
                                                    [(df_vehicle_statuts["HEAD_VIN"] == df_trips["VIN"]) &
                                                    (df_vehicle_statuts['HEAD_MESS_ID'].between(
                                                    df_trips['MESS_ID_STRT'], df_trips['MESS_ID_END']))],how='leftouter')

#SELECT ONLY RELEVANT FEATURES TO IMPROVE SPEED
df_vehicle_statuts_trips=df_vehicle_statuts_trips.select('HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID','HEAD_COLL_TIMS','VEHC_STTS_LIFT_MILG','VEHC_STTS_CHRG_STT',
                                                            'DATE_STRT','StartCalc_shift','DATE_END','StopCalc_shift')
#SORT THE DATAFRAME BY HEAD_VIN AND TIMES
df_vehicle_statuts_trips=df_vehicle_statuts_trips.orderBy('HEAD_VIN','HEAD_COLL_TIMS')

#CREATE HEAD_VIN WINDOW TO AVOID LOOPS PROCESSING 
window_vin = Window.partitionBy('HEAD_VIN').orderBy('HEAD_COLL_TIMS')

#ADD NEW FEATURES TO DETECT DELTA SOC
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('DELTA_SOC',
                                                                (F.col('VEHC_STTS_CHRG_STT')-F.lead(F.col('VEHC_STTS_CHRG_STT')).over(window_vin))
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_STATUT',
                                                                F.when(F.col('DELTA_SOC')<0,1).otherwise(0)
                                                                )
#HERE WE SPECIFY THAT WE WANT FOR EACH CHARGE SESSION A MINIMUM OF 2 POINTS
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_STATUT_SHIFT',
                                                                F.lag(F.col('CHARGE_STATUT')).over(window_vin)
                                                                )

df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE',
                                                                F.when(((F.col('CHARGE_STATUT_SHIFT')==1)|(F.col('CHARGE_STATUT')==1)),1).otherwise(0)
                                                                )
#RUN STATUT IS DEFINE TO ELIMINATE CHARGE SESSION DUE TO REGENERATIVE BRAKE
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('RUN',
                                                                (F.col('VEHC_STTS_LIFT_MILG')-F.lag(F.col('VEHC_STTS_LIFT_MILG')).over(window_vin))
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('RUN_STATUT',
                                                                F.when(F.col('RUN').between(0.01,50),1).otherwise(0)
                                                                )

df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('TRIP',
                                                                F.when(
                                                                    ((F.col('HEAD_COLL_TIMS')>=F.col('StartCalc_shift'))
                                                                        &(F.col('HEAD_COLL_TIMS')<=F.col('StopCalc_shift'))),1).otherwise(0)
                                                                )
#WHEN WE HAVE TRIP AND RUN IN THE SAME TIME WE WILL ASSUME THAT WE CAN'T HAVE A CHARGE SESSION DURING THE SAME MOMENT

df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('TRIP_STATUT',
                                                                F.when(
                                                                    (F.col('TRIP')==1)&(F.col('RUN_STATUT')==1),1).otherwise(0)
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE_2',
                                                                F.when(((F.col('CHARGE_APWE')==1)&(F.col('TRIP_STATUT')==0)),1).otherwise(0)
                                                                )

#HERE WE WILL PROCESS TIMESTAMP ON 10 POINTS BEFORE AND AFTER WITH BANDWITH OF 600 SEC TO DEAL WITH THE CHARGE FLAT STEP DURING
#OUR CHARGES SESSION
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE_3',F.when(
                                        (F.col('CHARGE_APWE_2')==1)|
                                        ((((F.lag(F.col('CHARGE_APWE_2')).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS')).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),2).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),2).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),3).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),3).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),4).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),4).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),5).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),5).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),6).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),6).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),7).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),7).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),8).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),8).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),9).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),9).over(window_vin).cast('long')<600))|
                                        ((F.lag(F.col('CHARGE_APWE_2'),10).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lag(F.col('HEAD_COLL_TIMS'),10).over(window_vin).cast('long')<600)))&

                                        (((F.lead(F.col('CHARGE_APWE_2')).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS')).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),2).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),2).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),3).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),3).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),4).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),4).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),5).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),5).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),6).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),6).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),7).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),7).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),8).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),8).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),9).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),9).over(window_vin).cast('long')>-600))|
                                        ((F.lead(F.col('CHARGE_APWE_2'),10).over(window_vin)==1)&(F.col('HEAD_COLL_TIMS').cast('long')-F.lead(F.col('HEAD_COLL_TIMS'),10).over(window_vin).cast('long')>-600))))
                                                                                            ,1).otherwise(0))

#SHIFT CHARGE SESSION TO MAKE SURE WE KEEP TWO POINTS FOR EACH CHARGE SESSION
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE_3',F.when(
                                                                (F.lead(F.col('CHARGE_APWE_3')).over(window_vin)==0)&
                                                                (F.lag(F.col('CHARGE_APWE_3')).over(window_vin)==0),0).otherwise(F.col('CHARGE_APWE_3')))

#ADJUST CHARGE SESSION START AND STOP
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE_3_delta',
                                                                F.col('CHARGE_APWE_3')-F.lag(F.col('CHARGE_APWE_3')).over(window_vin)
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE_3_delta',
                                                                F.when(F.col('HEAD_COLL_TIMS')==F.min(F.col('HEAD_COLL_TIMS')).over(window_vin),0).otherwise(F.col('CHARGE_APWE_3_delta'))
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_APWE_3_delta',
                                                                F.when(F.col('CHARGE_APWE_3_delta')!=1,0).otherwise(1)
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_SESSION',
                                                                F.sum(F.col('CHARGE_APWE_3_delta')).over(window_vin)
                                                                )
df_vehicle_statuts_trips=df_vehicle_statuts_trips.withColumn('CHARGE_SESSION',
                                                                F.when(F.col('CHARGE_APWE_3')==0,0).otherwise(F.col('CHARGE_SESSION'))
                                                                )
df_vehicle_statuts_trips_gb_DF=df_vehicle_statuts_trips.groupBy('HEAD_VIN','CHARGE_SESSION').agg(F.min('HEAD_COLL_TIMS').alias('CHARGE_START'),
                                                                                                    F.max('HEAD_COLL_TIMS').alias('CHARGE_STOP')).filter(F.col('CHARGE_SESSION')>0)
df_vehicle_statuts_trips_soc=df_vehicle_statuts_trips.select('HEAD_VIN','HEAD_COLL_TIMS','VEHC_STTS_CHRG_STT')


df_vehicle_statuts_trips_gb_DF_join=df_vehicle_statuts_trips_gb_DF.withColumnRenamed('CHARGE_START','HEAD_COLL_TIMS').join(
    df_vehicle_statuts_trips_soc,on=['HEAD_VIN','HEAD_COLL_TIMS'],how='left_outer').withColumnRenamed('VEHC_STTS_CHRG_STT','SOC_START').withColumnRenamed('HEAD_COLL_TIMS','CHARGE_START')

df_vehicle_statuts_trips_gb_DF_join_2=df_vehicle_statuts_trips_gb_DF_join.withColumnRenamed('CHARGE_STOP','HEAD_COLL_TIMS').join(
    df_vehicle_statuts_trips_soc,on=['HEAD_VIN','HEAD_COLL_TIMS'],how='left_outer').withColumnRenamed('VEHC_STTS_CHRG_STT','SOC_STOP').withColumnRenamed('HEAD_COLL_TIMS','CHARGE_STOP')

df_vehicle_statuts_trips_gb_DF_join_2=df_vehicle_statuts_trips_gb_DF_join_2.withColumn('CHARGE_LEVEL', F.col('SOC_STOP')-F.col('SOC_START'))
df_vehicle_statuts_trips_gb_DF_join_2=df_vehicle_statuts_trips_gb_DF_join_2.withColumn('CHARGE_STATUT_1', F.lit(1))
df_vehicle_statuts_trips_gb_DF_join_2=df_vehicle_statuts_trips_gb_DF_join_2.filter(F.col('CHARGE_LEVEL')>1)
df_vehicle_statuts_trips_gb_DF_join_2=df_vehicle_statuts_trips_gb_DF_join_2.dropDuplicates()
#UNPERSIST DATA TO RELEASE MEMORY
df_vehicle_statuts.unpersist()

#if i ==100:
#    df_vehicle_statuts_trips_gb_DF_join_2=df_vehicle_statuts_trips_gb_DF_join_2.limit(0)
    
#df_charge_session_level_1=df_charge_session_level_1.union(df_vehicle_statuts_trips_gb_DF_join_2)
df_vehicle_statuts_trips_gb_DF_join_2.write.mode("overwrite").parquet(path_result)
print("--- %s seconds ---" % (time.time() - start_time))

# COMMAND ----------


display(df_vehicle_statuts_trips_gb_DF_join_2)

# COMMAND ----------



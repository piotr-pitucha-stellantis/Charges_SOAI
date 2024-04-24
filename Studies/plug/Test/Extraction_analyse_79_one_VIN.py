# Databricks notebook source
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re
import  toolbox_connected_vehicle as tcv
import pandas.testing as pd_testing
import datetime
# import openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC # Extraction datalab v2

# COMMAND ----------

vin = ['VF3M4DGZULS176848']

message_list = [79]

# COMMAND ----------

for message in message_list:
    # one message_type only (54, 56, 57 ...)
    message_type = message
    start_date = "2022-01-01"
    end_date = "2022-02-01"
    # True for cleaned data, False for raw data
    preprocessing = True
    # Optionally a list of vin can be specified
    list_vin = vin
    # carcom or carbide or None for both
    data_collect = 'carbide' 

    tcv_df = tcv.read(
        spark, message_type, start_date, end_date, preprocessing, list_vin, data_collect
    )
    print("TCV extracted for message ", message)
    data_path_extraction = f"s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_1_VIN_othen/Bloc_{message}"
    tcv_df.write.mode('Overwrite').parquet(data_path_extraction)
    print("TCV wrote for message ", message)

# COMMAND ----------

dfv2_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_1_VIN_othen/Bloc_79")
                    )

# COMMAND ----------

dfv2_pd = dfv2_spk.toPandas() 

# COMMAND ----------

dfv2_pd

# COMMAND ----------

# MAGIC %md
# MAGIC # Import datalabv1 extraction

# COMMAND ----------

dfv1_spk = (spark.read
  .format("csv")
  .option("header",True)
  .options(delimiter=';')
  .load(f"file:/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/df_79_other_vin.csv")
)

# COMMAND ----------

dfv1_pd =dfv1_spk.toPandas()

# COMMAND ----------

dfv1_pd

# COMMAND ----------

# MAGIC %md
# MAGIC # Comparison

# COMMAND ----------

dfv1_pd = dfv1_pd[['HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID','HEAD_COLL_TIMS','HEAD_RECP_TIMS','TIMS_SESS_DURT','HEAD_GNSS_TIMS']].sort_values(by="HEAD_COLL_TIMS")
dfv1_pd['HEAD_SESS_ID'] = dfv1_pd['HEAD_SESS_ID'].astype('int64')
dfv1_pd['HEAD_MESS_ID'] = dfv1_pd['HEAD_MESS_ID'].astype('int64')

# COMMAND ----------

dfv2_pd = dfv2_pd[['HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID','HEAD_COLL_TIMS','HEAD_RECP_TIMS','TIMS_SESS_DURT','HEAD_GNSS_TIMS']].sort_values(by="HEAD_COLL_TIMS")

# COMMAND ----------

dfv1_pd

# COMMAND ----------

dfv2_pd

# COMMAND ----------

df_join = pd.merge(dfv1_pd, dfv2_pd, on=['HEAD_VIN', 'HEAD_SESS_ID','HEAD_MESS_ID'], how='left')

# COMMAND ----------

df_join.rename(columns={'HEAD_COLL_TIMS_y': 'HEAD_COLL_TIMS_v2','HEAD_RECP_TIMS_y':'HEAD_RECP_TIMS_v2','TIMS_SESS_DURT_y':'TIMS_SESS_DURT_v2','HEAD_GNSS_TIMS_y':'HEAD_GNSS_TIMS_v2',
                        'HEAD_COLL_TIMS_x': 'HEAD_COLL_TIMS_v1','HEAD_RECP_TIMS_x':'HEAD_RECP_TIMS_v1','TIMS_SESS_DURT_x':'TIMS_SESS_DURT_v1','HEAD_GNSS_TIMS_x':'HEAD_GNSS_TIMS_v1'}, inplace=True)

# COMMAND ----------

df_join

# COMMAND ----------

df_join['HEAD_COLL_TIMS_v1'] = pd.to_datetime(df_join['HEAD_COLL_TIMS_v1'])
df_join['HEAD_RECP_TIMS_v1'] = pd.to_datetime(df_join['HEAD_RECP_TIMS_v1'])
df_join['HEAD_GNSS_TIMS_v1'] = pd.to_datetime(df_join['HEAD_GNSS_TIMS_v1'])
df_join['HEAD_COLL_TIMS_v2'] = pd.to_datetime(df_join['HEAD_COLL_TIMS_v2'])
df_join['HEAD_RECP_TIMS_v2'] = pd.to_datetime(df_join['HEAD_RECP_TIMS_v2'])
df_join['HEAD_GNSS_TIMS_v2'] = pd.to_datetime(df_join['HEAD_GNSS_TIMS_v2'])
df_join['diff_col'] = df_join['HEAD_COLL_TIMS_v1'] - df_join['HEAD_COLL_TIMS_v2']
df_join['diff_recp'] = df_join['HEAD_RECP_TIMS_v1'] - df_join['HEAD_RECP_TIMS_v2']
df_join['diff_gnss'] = df_join['HEAD_GNSS_TIMS_v1'] - df_join['HEAD_GNSS_TIMS_v2']

# COMMAND ----------

df_join = df_join[['HEAD_VIN','HEAD_SESS_ID','HEAD_MESS_ID','diff_col','diff_recp','diff_gnss']]

# COMMAND ----------

df_join

# COMMAND ----------

df_join[(df_join['diff_col']< datetime.timedelta(days=0, hours=1) )| (df_join['diff_recp']< datetime.timedelta(days=0, hours=1)) | (df_join['diff_gnss']< datetime.timedelta(days=0, hours=1))]

# COMMAND ----------

Q

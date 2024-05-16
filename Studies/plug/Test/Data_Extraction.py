# Databricks notebook source
# MAGIC %md
# MAGIC # Start widgets

# COMMAND ----------

# widget data path 
dbutils.widgets.text('Data_path_S3', 's3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/public/')

# COMMAND ----------

# widget data path 
dbutils.widgets.text('Workspace_Data_Path', '/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/list_all_VIN.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re
import  toolbox_connected_vehicle as tcv
import json
# import openpyxl

# COMMAND ----------

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
print("Chemin du notebook :", notebook_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

# path =dbutils.widgets.get('Data_path') 
# path

# COMMAND ----------

# df_workbook = pd.read_csv(path, sep=";")

# COMMAND ----------

liste_30_random = df_workbook['HEAD_VIN'].sample(n=30).tolist()

# COMMAND ----------

# liste_30_vin =['VR3F4DGZTMY505590', 'W0VZ45GBXLS122055', 'VF3M45GBUKS528551', 'VR7BCZKXCME011036', 'VXKUKZKXZMW013212', 'VR3UHZKXZMT022681', 'VR3UHZKXZKT109129', 'VR7BCZKXCME008952', 'VR7A4DGZSML016960', 'VR3UHZKXZLT135730', 'VF3M4DGZULS176848', 'VR3UHZKXZMT020312', 'VXKUKZKXZMW012686', 'VR3UHZKXZMT034724', 'VR3UHZKXZLT016070', 'VR1J45GBULY057165', 'VXKUHZKXZL4372524', 'VR3UHZKXZLT145348', 'VXKUHZKXZL4399554', 'VR7A4DGZSML012682', 'VR3UKZKXZMJ559243', 'VXKUHZKXZL4423170', 'VR3UHZKXZMT022596', 'VF3M4DGZUMS042089', 'VR3UHZKXZMT055174', 'VR3F3DGZTLY019033', 'VR3UHZKXZLT128619', 'VF3M4DGZUMS015260', 'VF3M4DGZUMS119816', 'W0VZ4DGZ8LS176042']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vehicles information

# COMMAND ----------

# df_info = spark.table("ods.corvet_ict.nrv_bi_nrvqt01")
# filtered_df = df_info.filter(F.col("VIN").isin(liste_30_vin))
# df_info_lib_fam = spark.table("ods.corvet_ict.nrv_bi_nrvqt34")
# df_join = df_info_lib_fam.join(filtered_df.select("CODE_FAMILLE", "VIN"), on ="CODE_FAMILLE")
# df_join.select('VIN','LIBELLE').toPandas().to_csv("/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/list_30_selected_vin_famille.csv", index=False, header=True, sep=";")


# COMMAND ----------

# df_30_vin = pd.DataFrame(liste_30_vin, columns=["HEAD_VIN"]) 
# df_30_vin.to_csv("/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/list_30_selected_vin.csv", index=False, header=True, sep=";")




# COMMAND ----------

# MAGIC %md
# MAGIC # Parameter Recovery

# COMMAND ----------

dbutils.widgets.text("Vin", "")
dbutils.widgets.text("Message", "")
dbutils.widgets.text("Start date", "")
dbutils.widgets.text("End date", "")
dbutils.widgets.text("Folder extraction", "")
dbutils.widgets.text("Folder name", "")

# COMMAND ----------

vin = dbutils.widgets.get("Vin")
vin = json.loads(vin)
message_list = dbutils.widgets.get("Message")
message_list = json.loads(message_list)
start_date = dbutils.widgets.get("Start date")
end_date = dbutils.widgets.get("End date")
folder_extraction = dbutils.widgets.get("Folder extraction")
folder_name = dbutils.widgets.get("Folder name")

# COMMAND ----------

new_val = dbutils.widgets.get("Data_path_S3") + folder_name
dbutils.widgets.text("Data_path_S3", new_val)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TCV

# COMMAND ----------

# df_9_VIN = (spark.read
#         .format("delta")
#         .option("header",True)
#         .options(delimiter=';')
#         .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/9_Vin_Comparison")
#         )
# liste_9 = df_9_VIN.toPandas()['HEAD_VIN'].tolist()

# COMMAND ----------

#message_list = [54,74,77,79]
message_list = [68]

# COMMAND ----------

for message in message_list:
    # one message_type only (54, 56, 57 ...)
    message_type = message
    start_date = "2022-01-01"
    end_date = "2022-02-01"
    # True for cleaned data, False for raw data
    preprocessing = True
    # Optionally a list of vin can be specified
    list_vin = liste_9
    # carcom or carbide or None for both
    data_collect = 'carbide' 
    tcv_df = tcv.read(
        spark, message_type, start_date, end_date, preprocessing, list_vin, data_collect
    )
    print("TCV extracted for message ", message)
    # data_path_extraction = dbutils.widgets.get('Data_path_S3') + "data/Raw/"+folder_extraction+f"/Bloc_{message}"
    data_path_extraction = "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/68"
    tcv_df.write.mode('Overwrite').parquet(data_path_extraction)
    print("TCV wrote for message ", message)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Test

# COMMAND ----------

# sp_df = (spark.read
#   .format("parquet")
#   .option("header",True)
#   .options(delimiter=';')
#   .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_30_VIN/Bloc_68")
# )

# COMMAND ----------

# display(sp_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



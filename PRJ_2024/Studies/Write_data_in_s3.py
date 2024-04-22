# Databricks notebook source
df_vin = pd.read_csv( "/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/Processed_data/avenir/df_vin_van.csv",sep=';')

# COMMAND ----------

path = 

# COMMAND ----------

df_vin.write.mode('overwrite').parquet(path)

# COMMAND ----------

dbutils.fs.cp("file:/Workspace/Users/valerie.brisson@stellantis.com/Test_DB/donnees.csv","s3://cv-eu-west-1-001-dev-gadp-dafe/j483971/test/donnees.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC list "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Dase/Request"

# COMMAND ----------

dbutils.fs.mkdirs("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/csv")


# COMMAND ----------

dbutils.fs.rm("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/public", recurse=True)

# COMMAND ----------

Besoins pour Plugs 


fichier BTA => BTA_DATA_map
save local config => "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/public/"+folder_name + "delta_config/local_config"

raw df79 => dbutils.widgets.get('Data_path_S3') +"data/Raw/"+folder_extraction+"/79"
raw df74 => dbutils.widgets.get('Data_path_S3') +"data/Raw/"+folder_extraction+"/74"
levdata => plugs.write.parquet(dbutils.widgets.get('Data_path_S3') +"data/Processed/levdata/plugs"+wpath,mode='append')



# COMMAND ----------

dbutils.fs.cp('file:'+'/Workspace/Repos/ugo.merlier@stellantis.com/CHRG00/PRJ_2024/Data/Processed_data/avenir/df_vin_van.csv',"s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/csv/df_vin_van.csv")

# COMMAND ----------

dbutils.fs.cp("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_30_VIN/Bloc_74","s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Dase/parquet/74/Bloc_74", recurse=True)

# COMMAND ----------

dbutils.fs.cp("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/avenir/df_79","s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/79/df_79", recurse=True)

# COMMAND ----------

dbutils.fs.cp("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/avenir/df_74","s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Studies/Avenir/parquet/74/df_74", recurse=True)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



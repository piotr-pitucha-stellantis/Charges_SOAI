# Databricks notebook source
import pandas as pd

from datetime import timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# import openpyxl

# COMMAND ----------

dfv1_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Processed/Plot/df_ready_plot/datalabv1_id")
                    )
dfv2_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Processed/Plot/df_ready_plot/datalabv2_id")
                    )

# COMMAND ----------

dfv1_spk = dfv1_spk.withColumnRenamed("time", "time_x")
dfv2_spk = dfv2_spk.withColumnRenamed("time", "time_y")
df_join = dfv1_spk.join(dfv2_spk, ['HEAVIN', 'SEIDstart', 'HMSGstart', 'EVNT'], 'left')
display(df_join)

# COMMAND ----------

# 2. Convertissez les colonnes de dates en format timestamp
df_join = df_join.withColumn('time_x', F.unix_timestamp(F.col('time_x'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))
df_join = df_join.withColumn('time_y', F.unix_timestamp(F.col('time_y'), 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))

# 3. Calculez la différence entre les deux colonnes de dates
df_join = df_join.withColumn('diff', (F.col('time_x').cast('long') - F.col('time_y').cast('long')))

# COMMAND ----------

df_midnight_1 = df_join
# Convert the 'date' column to timestamp format
df_midnight_1 = df_midnight_1.withColumn("time_x", F.col("time_x").cast("timestamp"))

df_midnight_1 = df_midnight_1.withColumn("hour", F.hour("time_x"))
df_midnight_1 = df_midnight_1.withColumn("minute", F.minute("time_x"))

# Filter the data
filtered_df = df_midnight_1.filter(
    ((df_midnight_1["hour"] == 23) & (df_midnight_1["minute"] >= 30)) |  # 30 minutes before midnight
    ((df_midnight_1["hour"] == 0) & (F.abs(df_midnight_1["minute"]) <= 30))  # 30 minutes after midnight
)

dfv1_pd_filtered = filtered_df.toPandas()

# COMMAND ----------

# 3. Calculez la différence entre les deux colonnes de dates
df_join = df_join.withColumn('diff', (F.col('time_y').cast('long') - F.col('time_y').cast('long')))


df_midnight_1 = df_join
# Convert the 'date' column to timestamp format
df_midnight_1 = df_midnight_1.withColumn("time_y", F.col("time_y").cast("timestamp"))

df_midnight_1 = df_midnight_1.withColumn("hour", F.hour("time_y"))
df_midnight_1 = df_midnight_1.withColumn("minute", F.minute("time_y"))

# Filter the data
filtered_df = df_midnight_1.filter(
    ((df_midnight_1["hour"] == 23) & (df_midnight_1["minute"] >= 30)) |  # 30 minutes before midnight
    ((df_midnight_1["hour"] == 0) & (F.abs(df_midnight_1["minute"]) <= 30))  # 30 minutes after midnight
)

dfv2_pd_filtered = filtered_df.toPandas()

# COMMAND ----------

dfv1_pd_filtered

# COMMAND ----------

dfv2_pd_filtered

# COMMAND ----------

# MAGIC %md
# MAGIC # Pandas version

# COMMAND ----------

# Créer un DataFrame de test
dfv1_pd = dfv1_spk.toPandas()
dfv1_pd['SEIDstart'] = dfv1_pd['SEIDstart'].astype('int64')
dfv1_pd['HMSGstart'] = dfv1_pd['HMSGstart'].astype('int64')
dfv1_pd['time_x'] = pd.to_datetime(dfv1_pd['time_x'])
dfv2_pd = dfv2_spk.toPandas()
df_join = pd.merge(dfv1_pd, dfv2_pd, on=['HEAVIN', 'SEIDstart','HMSGstart','EVNT'], how='left')
df_join['time_x'] = pd.to_datetime(df_join['time_x'])
df_join['time_y'] = pd.to_datetime(df_join['time_y'])
df_join['diff'] = df_join['time_x'] - df_join['time_y']


# Définir une fonction pour filtrer les entrées autour de minuit
def filter_around_midnight(timestamp):
    midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    # Définir une marge de 30 minutes autour de minuit
    margin = timedelta(minutes=10)
    # Vérifier si l'heure est dans la plage de minuit +/- 30 minutes
    if abs((timestamp - midnight).total_seconds()) <= margin.total_seconds():
        return True
    # Vérifier si l'heure est dans la plage de 23h30 à minuit
    elif timestamp.hour == 23 and timestamp.minute >= 30:
        return True
    return False

# Appliquer la fonction de filtrage pour obtenir les entrées autour de minuit
around_midnight_entries_x = df_join[df_join['time_x'].apply(filter_around_midnight)]
print(around_midnight_entries_x)

around_midnight_entries_y = df_join[df_join['time_y'].apply(filter_around_midnight)]
print(around_midnight_entries_y)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # TCV analysis midnight delay

# COMMAND ----------

dfv2_midnight_spk = (spark.read
                    .format("parquet")
                    .option("header",True)
                    .options(delimiter=';')
                    .load("s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/data/Raw/Extraction_1_VIN_midnight/Bloc_79")
                    ).select('HEAD_VIN', 'HEAD_SESS_ID', 'HEAD_MESS_ID','HEAD_COLL_TIMS')
                    
dfv2_midnight_spk = dfv2_midnight_spk.withColumnRenamed("HEAD_COLL_TIMS", "HEAD_COLL_TIMS_v2")

# COMMAND ----------

dfv1_midnight_spk = (spark.read
  .format("csv")
  .option("header",True)
  .options(delimiter=';')
  .load(f"file:/Workspace/Users/ugo.merlier@stellantis.com/Project/chrg00/PRJ_2024/Data/comparaison_ID.csv")
).select('HEAD_VIN', 'HEAD_SESS_ID', 'HEAD_MESS_ID','HEAD_COLL_TIMS')

dfv1_midnight_spk = dfv1_midnight_spk.withColumnRenamed("HEAD_COLL_TIMS", "HEAD_COLL_TIMS_v1")

# COMMAND ----------

dfv2_midnight_spk.count()

# COMMAND ----------

dfv1_midnight_spk.count()

# COMMAND ----------

df_join_midnight = dfv1_midnight_spk.join(dfv2_midnight_spk, ['HEAD_VIN', 'HEAD_SESS_ID', 'HEAD_MESS_ID'], 'left')

# COMMAND ----------

display(df_join_midnight)

# COMMAND ----------

# 3. Calculez la différence entre les deux colonnes de dates
df_join_midnight = df_join_midnight.withColumn('diff', (F.col('HEAD_COLL_TIMS_v1').cast('long') - F.col('HEAD_COLL_TIMS_v2').cast('long')))

# COMMAND ----------

display(df_join_midnight)

# COMMAND ----------

df_midnight_1 = df_join_midnight
# Convert the 'date' column to timestamp format
df_midnight_1 = df_midnight_1.withColumn("HEAD_COLL_TIMS_v1", F.col("HEAD_COLL_TIMS_v1").cast("timestamp"))

df_midnight_1 = df_midnight_1.withColumn("hour", F.hour("HEAD_COLL_TIMS_v1"))
df_midnight_1 = df_midnight_1.withColumn("minute", F.minute("HEAD_COLL_TIMS_v1"))

# Filter the data
filtered_df = df_midnight_1.filter(
    ((df_midnight_1["hour"] == 23) & (df_midnight_1["minute"] >= 30)) |  # 30 minutes before midnight
    ((df_midnight_1["hour"] == 0) & (F.abs(df_midnight_1["minute"]) <= 30))  # 30 minutes after midnight
)

df_midnight_join_HEAD_COLL_TIMS_v1 = filtered_df.toPandas()

# COMMAND ----------

df_midnight_join_HEAD_COLL_TIMS_v1

# COMMAND ----------

df_midnight_1 = df_join_midnight
# Convert the 'date' column to timestamp format
df_midnight_1 = df_midnight_1.withColumn("HEAD_COLL_TIMS_v2", F.col("HEAD_COLL_TIMS_v2").cast("timestamp"))

df_midnight_1 = df_midnight_1.withColumn("hour", F.hour("HEAD_COLL_TIMS_v2"))
df_midnight_1 = df_midnight_1.withColumn("minute", F.minute("HEAD_COLL_TIMS_v2"))

# Filter the data
filtered_df = df_midnight_1.filter(
    ((df_midnight_1["hour"] == 23) & (df_midnight_1["minute"] >= 30)) |  # 30 minutes before midnight
    ((df_midnight_1["hour"] == 0) & (F.abs(df_midnight_1["minute"]) <= 30))  # 30 minutes after midnight
)

df_midnight_join_HEAD_COLL_TIMS_v2 = filtered_df.toPandas()

# COMMAND ----------

df_midnight_join_HEAD_COLL_TIMS_v2

# COMMAND ----------



# COMMAND ----------



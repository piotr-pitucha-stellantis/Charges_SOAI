# Databricks notebook source
# MAGIC %md
# MAGIC # S3 commands to modify and better visualize the architecture

# COMMAND ----------

# MAGIC %md
# MAGIC  S3 project path : s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00

# COMMAND ----------

# MAGIC %md
# MAGIC ![bucket s3 logo](file:/Workspace/Repos/ugo.merlier@stellantis.com/CHRG00/assets/s3.pnga)

# COMMAND ----------

# MAGIC %md
# MAGIC Bucket S3 are storage containers in Amazon's online storage service, called Amazon Simple Storage Service (S3). These buckets can hold a large amount of data and are used to securely store data in the cloud.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy from the workspace to S3 storage or from s3 to s3

# COMMAND ----------

dbutils.fs.cp("file:/Workspace/Users/valerie.brisson@stellantis.com/Test_DB/donnees.csv","s3://cv-eu-west-1-001-dev-gadp-dafe/j483971/test/donnees.csv")

dbutils.fs.cp("s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/source/", "s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/destination/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Folder creation

# COMMAND ----------

dbutils.fs.mkdirs("s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/dossier/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Move from the S3 to S3 storage 

# COMMAND ----------

dbutils.fs.mv("s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/source/", "s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/destination/")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Display folder content

# COMMAND ----------

dbutils.fs.ls("s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/dossier/")

%sql
list "s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/dossier/"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Recursive delete

# COMMAND ----------

dbutils.fs.rm("s3://cv-eu-west-1-001-dev-gadp-dafe/id/chemin/vers/votre/dossier/", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Practice

# COMMAND ----------

# MAGIC %sql
# MAGIC list "s3://cv-eu-west-1-001-dev-gadp-dafe/sd43982/chrg00/Dase/Request/last_test/local_config/"
# MAGIC

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



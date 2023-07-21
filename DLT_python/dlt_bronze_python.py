# Databricks notebook source
# import sys, os
# sys.path.append(os.path.abspath('/Workspace/Repos/yas.mokri@databricks.com/tfnsw_bootcamp/'))

import dlt
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, explode

# COMMAND ----------

input_path = spark.conf.get("mypipeline.input_path")

@dlt.table(name="dlt_bronze")
def bronze_table():
  bronze_df = (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")  
      .load(input_path)
  )
  return bronze_df


# COMMAND ----------

@dlt.view()
def bronze_view(temporary=True):

  df_bronze = dlt.read_stream("dlt_bronze")

  #descriptor_file = "/Workspace/Repos/yas.mokri@databricks.com/tfnsw_bootcamp/gtfs_realtime_1007_extension.desc"
  descriptor_file = "/dbfs/FileStore/tmp/yas.mokri@databricks.com/gtfs.desc"
  
  proto_df = df_bronze.select(col("timestamp").alias("ingest_time") , from_protobuf(df_bronze.data, "FeedMessage", descFilePath=descriptor_file).alias("proto"))


  return proto_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## create silver using what you've learned so far and your previous code for unpacking proto_df
# MAGIC - create a live table
# MAGIC - read from a live table  

# COMMAND ----------

@dlt.table()
def dlt_silver():

  proto_df = dlt.read_stream("bronze_view")
  unpacked_df = proto_df.select('ingest_time', 'proto.*').select('ingest_time', explode(col('entity')).alias("entity"))

  # hand on exercise- continue by unpacking some more fields like entity then vehicle, use a pattern similar proto_df.select('proto.*')
  unpacked_df = unpacked_df.select('ingest_time', "entity", "entity.*").select('ingest_time', "entity", "id", "alert","vehicle.*")

  return unpacked_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's do this in SQL (open the gold notebook from the repo with SQL as the language)
# MAGIC

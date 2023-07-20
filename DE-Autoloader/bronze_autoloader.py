# Databricks notebook source
bronze_table_name = "bronze_train_data"
checkpoint_location = f"/tmp/checkpoints/{bronze_table_name}"

# COMMAND ----------


spark.sql("drop catalog if exists transport_bootcamp cascade")

# COMMAND ----------

dbutils.fs.rm("/tmp/", True)

# COMMAND ----------

current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
datasets_location = f'/FileStore/tmp/{current_user_id}/datasets/'

dbutils.fs.rm(datasets_location, True)
catalog_name = "transport_bootcamp"
spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name}')
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
spark.sql(f'create database if not exists {catalog_name}.{database_name};')
spark.sql(f'use {catalog_name}.{database_name}')

print (f"Created database :  {catalog_name}.{database_name}") 

# COMMAND ----------

path = "/tmp/apidata/"
schema = "/tmp/schema/"
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet") 
    .option("cloudFiles.schemaLocation",schema) 
    .load(path)
)
display(df)

# COMMAND ----------

bronze_df = df
bronze_df.writeStream.option("mergeSchema", "true").option("checkpointLocation", checkpoint_location).table(f"{catalog_name}.{database_name}.{bronze_table_name}")

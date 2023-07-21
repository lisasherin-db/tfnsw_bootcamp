# Databricks notebook source
# MAGIC %md
# MAGIC ## SETUP

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %run ../utils/refresh-env

# COMMAND ----------

# MAGIC %md
# MAGIC ##Autoloader
# MAGIC The same readStream as before with: format("cloudFiles")

# COMMAND ----------

input_path = f"{datasets_location}/apidata/"
schema_path = f"{datasets_location}/schema/"

bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet") 
    .option("cloudFiles.schemaLocation",schema_path) 
    .load(input_path)
)
display(bronze_df)

# COMMAND ----------

checkpoint_location = f"{datasets_location}/checkpoints/{bronze_table_name}"
bronze_df.writeStream.option("mergeSchema", "true").option("checkpointLocation", checkpoint_location).table(bronze_table_name)

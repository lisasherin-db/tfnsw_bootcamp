# Databricks notebook source
# silver_table_name = "silver_train_data"
checkpoint_location = f"{datasets_location}/{silver_table_name}"

# COMMAND ----------

df_bronze = spark.readStream.table(f"{catalog_name}.{database_name}.bronze_train_data")
display(df_bronze)

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, explode

descriptor_file = "/Workspace/Repos/yas.mokri@databricks.com/tfnsw_bootcamp/gtfs-realtime_1007_extension.desc"

proto_df = (
    df_bronze.select(
      from_protobuf(df_bronze.data, "FeedMessage", descFilePath=descriptor_file).alias("proto")
    )
)

unpacked_df = proto_df.select('proto.*').select(explode(col('entity')).alias("entity")).select("entity", "entity.*").select("entity", "id", "alert","vehicle.*")#.select("entity", "id", "alert","trip.*", "vehicle.*")


# COMMAND ----------

silver_df = unpacked_df
silver_df.writeStream.option("mergeSchema", "true").option("checkpointLocation", checkpoint_location).table(f"{catalog_name}.{database_name}.silver_train_data")

# COMMAND ----------

display(spark.readStream.table("transport_bootcamp.yas_mokri_bootcamp.silver_train_data").where("id=1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from transport_bootcamp.yas_mokri_bootcamp.silver_train_data

# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGE INTO using id assuming it's unique and updating the table two times 

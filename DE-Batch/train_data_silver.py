# Databricks notebook source
catalog_name = "transport_bootcamp"
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.{database_name}.bronze_train_data")
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
display(unpacked_df)

# COMMAND ----------

unpacked_df.createOrReplaceTempView("silver_temp_view")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS transport_bootcamp.yas_mokri_bootcamp.silver_train_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO transport_bootcamp.yas_mokri_bootcamp.silver_train_data 
# MAGIC SELECT * FROM silver_temp_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from transport_bootcamp.yas_mokri_bootcamp.silver_train_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ##MERGE INTO using id assuming it's unique and updating the table two times 

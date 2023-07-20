# Databricks notebook source
catalog_name = "transport_bootcamp"
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

df_silver = spark.read.table(f"{catalog_name}.{database_name}.silver_train_data")
df_silver.createOrReplaceTempView("silver_temp_view")
display(df_silver)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS transport_bootcamp.yas_mokri_bootcamp.gold_train_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO transport_bootcamp.yas_mokri_bootcamp.gold_train_data 
# MAGIC SELECT current_status, stop_id, count(*) as total FROM silver_temp_view GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from transport_bootcamp.yas_mokri_bootcamp.gold_train_data;

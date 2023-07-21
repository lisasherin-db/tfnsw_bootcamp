# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

df_silver = spark.read.table(silver_table_name)
df_silver.createOrReplaceTempView("silver_temp_view")
display(df_silver)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS transport_bootcamp.yas_mokri_bootcamp.gold_train_data (stop_id STRING, current_status STRING, total INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO transport_bootcamp.yas_mokri_bootcamp.gold_train_data 
# MAGIC SELECT current_status, stop_id, count(*) as total FROM silver_temp_view GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from transport_bootcamp.yas_mokri_bootcamp.gold_train_data;

# COMMAND ----------

# MAGIC %md
# MAGIC #Another Gold Table? 
# MAGIC Come up with more menaingful aggregate based on the silver table and create your own Gold table

# COMMAND ----------

# MAGIC %md
# MAGIC ## How about a view this time? what's the difference?

# COMMAND ----------

# You can use this to enable merge schema 
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

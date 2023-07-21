# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

print (f"Created database :  {database}") 
print(f"bronze table name: {bronze_table_name}")
print(f"silver table name: {silver_table_name}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read protobuf data from the Bronze table

# COMMAND ----------

df_bronze = spark.read.table(bronze_table_name)
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse protobuf data using fom_protobuf 

# COMMAND ----------

from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.functions import col, explode

proto_df = df_bronze.select(col("timestamp").alias("ingest_time") , from_protobuf(df_bronze.data, "FeedMessage", descFilePath=descriptor_file).alias("proto"))

display(proto_df)


# COMMAND ----------

proto_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## unpack struct data into columns 

# COMMAND ----------

unpacked_df = proto_df.select('ingest_time', 'proto.*').select('ingest_time', explode(col('entity')).alias("entity"))

# hand on exercise- continue by unpacking some more fields like entity then vehicle, use a pattern similar proto_df.select('proto.*')
unpacked_df = unpacked_df.select('ingest_time', "entity", "entity.*").select('ingest_time', "entity", "id", "alert","vehicle.*")

display(unpacked_df)

# COMMAND ----------

# you can do something similar using SQL as well. Then you can use unpacked_df = spark.sql("you_sql_query") to acheive the same results
#https://docs.databricks.com/optimizations/semi-structured.html

# COMMAND ----------

proto_df.createOrReplaceTempView("proto_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select entity.* from (select (explode(proto.entity)) as entity from proto_temp_view)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write to silver
# MAGIC Write the results to the silver table (you can use silver_table_name) using the method we used earlier to write to Bronze then use a SQL statement to verify the results

# COMMAND ----------

print(silver_table_name)

# COMMAND ----------

unpacked_df.write.mode('append').option("mergeSchema", "true").saveAsTable(silver_table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Alternative method using SQL

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
# MAGIC ##DML
# MAGIC Update congestion_level from UNKNOWN_CONGESTION_LEVEL to another value for one or some of the rows in the table
# MAGIC Write a SQL query to do this either by using %sql or spark.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, congestion_level FROM transport_bootcamp.yas_mokri_bootcamp.silver_train_data WHERE id in (1,2,10);

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE transport_bootcamp.yas_mokri_bootcamp.silver_train_data SET congestion_level="VERY_KNOWN_CONGESTION_LEVEL" WHERE id in (1,2,10) ;
# MAGIC SELECT id, congestion_level FROM transport_bootcamp.yas_mokri_bootcamp.silver_train_data WHERE id in (1,2,10);

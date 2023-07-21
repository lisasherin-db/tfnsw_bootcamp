# Databricks notebook source
# MAGIC %md
# MAGIC ## SETUP

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %run ../utils/refresh-env

# COMMAND ----------

import time
import requests
from datetime import datetime

# COMMAND ----------

def get_sydney_trains_data():
    # API endpoint URL
    url = api_uri

    # Set the required headers
    headers = {
        'Authorization': f'apikey {dbutils.secrets.get(scope=scope_name, key="opendata_apikey")}',
        'Accept': 'application/x-google-protobuf'
    }

    # Make the API call
    data = None
    try:
      response = requests.get(url, headers=headers)

      response.raise_for_status() # this will raise an exception if using dataflow we can retry the api call 

      data = response.content
    except Exception as e:
      rasie (e)

    return data


# COMMAND ----------

sleep_time = 10

# while True:
# for i in range(0,10) 
api_data = get_sydney_trains_data()
data = [{
  "source": "api_transport_nsw",
  "timestamp": datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S"),
  "data": api_data,
}]

df = spark.createDataFrame(data=data)

#write the data into a delta table (delta is the defaul format)
df.write.mode('append').option("mergeSchema", "true").saveAsTable(bronze_table_name)
# time.sleep(sleep_time)


# COMMAND ----------

# MAGIC %md 
# MAGIC let's write some code to explore api_data and the delta table 
# MAGIC - in python you can use display()
# MAGIC - you can switch to SQL by adding %sql 
# MAGIC - to read a delta table into a dataframe you can use this syntax: spark.read.table(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from transport_bootcamp.yas_mokri_bootcamp.bronze_train_data

# COMMAND ----------

display(api_data)
display(df)

# COMMAND ----------

bronze_df = spark.read.table("transport_bootcamp.yas_mokri_bootcamp.bronze_train_data")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from transport_bootcamp.yas_mokri_bootcamp.bronze_train_data

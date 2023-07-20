# Databricks notebook source
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

import requests

def get_sydney_trains_data():
    # API endpoint URL
    url = 'https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains'

    # Set the required headers
    headers = {
        'Authorization': f'apikey {dbutils.secrets.get(scope="lisasherin", key="opendata_apikey")}',
        'Accept': 'application/x-google-protobuf'
    }

    # Make the API call
    response = requests.get(url, headers=headers)

    response.raise_for_status() # this will raise an exception if using dataflow we can retry the api call (needs some error handling)

 
    # # Check if the request was successful (status code 200)
    # if response.status_code == 200:
    #     # Retrieve the data from the response
    data = response.content

    return data


# COMMAND ----------

 data = get_sydney_trains_data()

# COMMAND ----------

import time
from datetime import datetime
sleep_time = 10

# while True:
# for i in range(0,10) 
api_data = get_sydney_trains_data()
data = [{
  "source": "api_transport_nsw",
  "timestamp": datetime.utcnow(),
  "data": api_data,
}]
df = spark.createDataFrame(data=data)
df.write.mode('append').option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{database_name}.bronze_train_data")
print(f"Sleeping for {sleep_time} seconds")
  # time.sleep(sleep_time)


# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from transport_bootcamp.yas_mokri_bootcamp.bronze_train_data

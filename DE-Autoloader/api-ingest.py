# Databricks notebook source
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

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Retrieve the data from the response
        data = response.content

        return data
    else:
        return None


# COMMAND ----------

import time
from datetime import datetime
sleep_time = 10

# while True:
for i in range(0,2):
    api_data = get_sydney_trains_data()
    data = [{
      "source": "api_transport_nsw",
      "timestamp": datetime.utcnow(),
      "data": api_data,
    }]
    df = spark.createDataFrame(data=data)
    df.write.mode('append').parquet("/tmp/apidata/")
    # df.write.mode('append').option("mergeSchema", "true").saveAsTable(f"{catalog_name}.{database_name}.bronze_train_data")
    time.sleep(sleep_time)


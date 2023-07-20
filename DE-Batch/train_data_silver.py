# Databricks notebook source
# MAGIC %pip install protobuf

# COMMAND ----------

catalog_name = "transport_bootcamp"
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'

# COMMAND ----------

# install protobuf parser
import gtfs_realtime_1007_extension.proto__pb2 as pb
import requests

def process_sydney_trains_data(data):
  
      # Create a GTFS Realtime FeedMessage object
      feed = pb.FeedMessage()

      # Parse the protobuf data
      feed.ParseFromString(data)

      # Process the feed and create a list of dictionaries
      # update to include additional fields over time...
      data_list = []
      for entity in feed.entity:
          entity_data = {}
          if entity.HasField('id'):
              entity_data['id'] = entity.id

          if entity.HasField('is_deleted'):
              entity_data['is_deleted'] = entity.is_deleted

          if entity.HasField('trip_update'):
              trip_update = entity.trip_update
              if trip_update.HasField('trip'):
                  trip = trip_update.trip
                  entity_data['trip_id'] = trip.trip_id
                  entity_data['start_time'] = trip.start_time
                  entity_data['start_date'] = trip.start_date
                  entity_data['route_id'] = trip.route_id

              for stop_time_update in trip_update.stop_time_update:
                  stop_data = {}
                  stop_data['stop_id'] = stop_time_update.stop_id
                  stop_data['arrival_time'] = stop_time_update.arrival.time
                  stop_data['arrival_delay'] = stop_time_update.arrival.delay
                  stop_data['departure_time'] = stop_time_update.departure.time
                  stop_data['departure_delay'] = stop_time_update.departure.delay

                  entity_data.setdefault('stops', []).append(stop_data)

          if entity.HasField('vehicle'):
              vehicle = entity.vehicle
              if vehicle.HasField('trip'):
                  trip = vehicle.trip
                  entity_data['trip_id'] = trip.trip_id
                  entity_data['schedule_relationship'] = trip.schedule_relationship
                  entity_data['start_time'] = trip.start_time
                  entity_data['start_date'] = trip.start_date
                  entity_data['route_id'] = trip.route_id

              if vehicle.HasField('stop_id'):
                entity_data['stop_id'] = vehicle.stop_id

              if vehicle.HasField('position'):
                  position = vehicle.position
                  entity_data['latitude'] = position.latitude
                  entity_data['longitude'] = position.longitude
                  entity_data['bearing'] = position.bearing
                
              if vehicle.HasField('current_status'):
                entity_data['current_status'] = vehicle.current_status

              if vehicle.HasField('timestamp'):
                entity_data['timestamp'] = vehicle.timestamp

              if vehicle.HasField('congestion_level'):
                entity_data['congestion_level'] = vehicle.congestion_level

              if vehicle.HasField('occupancy_status'):
                entity_data['occupancy_status'] = vehicle.occupancy_status

          if entity.HasField('alert'):
              alert = entity.alert
              entity_data['alert_text'] = alert.header_text.translation[0].text
              entity_data['informed_entity'] = alert.informed_entity
              entity_data['cause'] = alert.cause
              entity_data['cause_detail'] = alert.cause_detail
              entity_data['effect'] = alert.effect
              entity_data['effect_detail'] = alert.effect_detail
              entity_data['description_text'] = alert.description_text

          data_list.append(entity_data)

      # Create a dataframe from the list of dictionaries
      df = spark.createDataFrame(data_list)
      return df
  else:
      # If the request was not successful, print the error message
      print(f'Request failed with status code {response.status_code}: {response.text}')

      return None

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.{database_name}.bronze_train_data")
display(df_bronze)

# COMMAND ----------

data = 
process_sydney_trains_data(data)

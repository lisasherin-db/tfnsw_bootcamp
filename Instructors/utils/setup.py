# Databricks notebook source
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
datasets_location = f'/FileStore/tmp/{current_user_id}/'

# catalog_name = "transport_bootcamp"
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'

# COMMAND ----------

database = f"{database_name}"
bronze_table_name = f"{database}.bronze_train_data"
silver_table_name = f"{database}.silver_train_data"

# COMMAND ----------

api_uri = 'https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains'

# COMMAND ----------

descriptor_file = f"/Workspace/Repos/{current_user_id}/tfnsw_bootcamp/gtfs_realtime_1007_extension.desc"

# COMMAND ----------

scope_name = "tfnsw_bootcamp"

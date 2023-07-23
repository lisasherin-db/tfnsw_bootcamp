# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Views
# MAGIC
# MAGIC Databricks <a href="https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions" target="_blank">dynamic views</a> allow user or group identity ACLs to be applied to data at the column (or row) level.
# MAGIC
# MAGIC Database administrators can configure data access privileges to disallow access to a source table and only allow users to query a redacted view. 
# MAGIC
# MAGIC Users with sufficient privileges will be able to see all fields, while restricted users will be shown arbitrary results, as defined at view creation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # SET UP

# COMMAND ----------

UC_enabled = TRUE
reset = False

# COMMAND ----------

# MAGIC %run ../Instructors/utils/setup

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Dynamic View related functions
# MAGIC
# MAGIC Unity Catalog introduces the following functions, which allow you to dynamically limit which users can access a row, column, or record in a view:
# MAGIC
# MAGIC **current_user():** Returns the current user’s email address.
# MAGIC
# MAGIC **is_account_group_member():** Returns TRUE if the current user is a member of a specific account-level group. Recommended for use in dynamic views against Unity Catalog data.
# MAGIC
# MAGIC **is_member():** Returns TRUE if the current user is a member of a specific workspace-level group. This function is provided for compatibility with the existing Hive metastore. Avoid using it with views against Unity Catalog data, because it does not evaluate account-level group membership.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show groups;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select current_user(), is_account_group_member('users'), is_account_group_member('hr');

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
import datetime

employee_rows = [
Row(dob=datetime.date(1987, 6, 11), sex='F', gender='F', first_name='Bonnie', last_name='Johnson', division='Greater Sydney', city='Sydney', state='NSW', zip=90047, updated=datetime.datetime(2019, 7, 29, 4, 50, 8)),
 Row(dob=datetime.date(1928, 3, 17), sex='M', gender='M', first_name='Michael', last_name='Johnston', division='Regional and Outer Metropolitan', city='Wollongong', state='NSW', zip=90755, updated=datetime.datetime(2019, 12, 4, 17, 10, 24)),
 Row(dob=datetime.date(1980, 7, 29), sex='M', gender='M', first_name='David', last_name='Woodard', division='Infrastructure and Place', city='Sydney', state='NSW', zip=91367, updated=datetime.datetime(2019, 12, 15, 14, 58, 8)),
 Row(dob=datetime.date(1997, 6, 17), sex='F', gender='F', first_name='Stephanie', last_name='Gibson', division='Infrastructure and Place', city='Sydney', state='NSW', zip=92886, updated=datetime.datetime(2019, 12, 5, 17, 31, 44)),
 Row(dob=datetime.date(1925, 3, 1), sex='M', gender='M', first_name='Christopher', last_name='Phelps', division='Customer Strategy and Technology', city='Sydney', state='NSW', zip=91007, updated=datetime.datetime(2019, 12, 11, 11, 37, 36)),
 Row(dob=datetime.date(1960, 1, 18), sex='F', gender='F', first_name='Gail', last_name='Bush', division='Greater Sydney', city='Sydney', state='NSW', zip=91386, updated=datetime.datetime(2019, 12, 7, 18, 1, 36)),
 Row(dob=datetime.date(1946, 11, 5), sex='M', gender='M', first_name='Chris', last_name='Wright', division='Greater Sydney', city='Sydney', state='NSW', zip=90232, updated=datetime.datetime(2019, 12, 9, 9, 42, 24)),
 Row(dob=datetime.date(1991, 4, 18), sex='M', gender='M', first_name='Kevin', last_name='Phillips', division='Customer Strategy and Technology', city='Sydney', state='NSW', zip=90403, updated=datetime.datetime(2019, 6, 12, 22, 43, 12)),
 Row(dob=datetime.date(1944, 8, 23), sex='F', gender='F', first_name='Pamela', last_name='Wilson', division='Corporate Services', city='Sydney', state='NSW', zip=90210, updated=datetime.datetime(2019, 12, 8, 9, 23, 12)),
 Row(dob=datetime.date(1951, 11, 12), sex='M', gender='M', first_name='Paul', last_name='Hinton', division='Regional and Outer Metropolitan', city='Newcastle', state='NSW', zip=92372, updated=datetime.datetime(2019, 12, 8, 9, 10, 24))
 ]

 # write to table
df = spark.createDataFrame(employee_rows)
df.printSchema()
df.show()

# COMMAND ----------

# don't need to add catalog and schema since I ran "USE" command at the beginning
df.write.saveAsTable(f"employee")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating conditional column access
# MAGIC
# MAGIC For this scenario, we will generate a table of train crew data with sensitive employee details we want to mask for some downstream users. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW employee_anonymised_view AS
# MAGIC   SELECT
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('hr') THEN dob
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS dob,
# MAGIC     sex,
# MAGIC     gender,
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('hr') THEN first_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS first_name,
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('hr') THEN last_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS last_name,
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('hr') THEN street_address
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS street_address,
# MAGIC     city,
# MAGIC     state,
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('hr') THEN zip
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS zip,
# MAGIC     updated
# MAGIC   FROM employee

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's check
# MAGIC
# MAGIC SELECT * FROM employee_anonymised_view;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating conditional row access
# MAGIC
# MAGIC Adding views with **`WHERE`** clauses to filter source data on different conditions for teams throughout an organization can be a beneficial option for granting access to only the necessary data to each audience. Dynamic views add the option to create these views with full access to underlying data for users with elevated privileges.
# MAGIC
# MAGIC Note the views can be layered on top of one another; below, the **`users_vw`** from the previous step is modified with conditional access. Users that aren't members of the specified group will only be able to see records from the city of Los Angeles that have been updated after the specified date.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW gs_employee_vw AS
# MAGIC SELECT * FROM employee_anonymised_view -- test or change back to employee table
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_account_group_member('hr') THEN TRUE
# MAGIC     ELSE division = "Greater Sydney"
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's check
# MAGIC
# MAGIC SELECT * FROM gs_employee_vw;

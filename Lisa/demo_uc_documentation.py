# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Adding comments and constraints to your data
# MAGIC
# MAGIC In the following example, we are creating a json file with **[comments](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-comment.html)** for discoverability and business documentation purposes. We are also adding **[constraints](https://learn.microsoft.com/en-gb/azure/databricks/tables/constraints)** (public preview) to tables, for quality and reference purposes, e.g. primary and foreign keys and quality constraints 
# MAGIC
# MAGIC **NOTE:** If you set a primary key ensure column is set to NOT NULL i.e.:
# MAGIC `ALTER TABLE table_name ALTER COLUMN column SET NOT NULL;`

# COMMAND ----------

# creating the file
catalog_documentation = [
    {
        "catalog_name": "lisa_sherin_dac_demo_catalog",
        "catalog_comment": "Catalog for demos and sandboxing for NSW Public Sector. Sources include TfNSW [OpenData](https://opendata.transport.nsw.gov.au/)",
        "schema_documentation": [
            {
                "schema_name": "ref_timetables",
                "schema_comment": "TfNSW has been supplying a GTFS dataset for journey planning for many years. Static timetables, stop locations, and route shape information in General Transit Feed Specification (GTFS) format for all operators, including regional, trackwork and transport routes not available in realtime feeds. Sourced from [OpenData](https://opendata.transport.nsw.gov.au/dataset/timetables-complete-gtfs) for more information.",
"table_documentation": [
    {
        "table_name": "pathways",
        "table_comment": "This table provides information for the customer about how to book the service",
        "column_comments": {
            "pathway_id": "An ID that uniquely identifies the pathway",
            "from_stop_id": "Location at which the pathway begins. It contains a stop_id that identifies a platform, entrance/exit, generic node or boarding area from the stops.txt file",
            "to_stop_id": "Location at which the pathway ends. It contains a stop_id that identifies a platform, entrance/exit, generic node or boarding area from the stops.txt file",
            "pathway_mode": "Type of pathway between the specified (from_stop_id, to_stop_id) pair.",
            "is_bidirectional": "Indicates in which direction the pathway can be used",
            "traversal_time": "Last time on the last day before travel to make the booking request.",
        },
        "table_constraints": ["pathway_pk PRIMARY KEY(pathway_id)"]
    },
    {
        "table_name": "levels",
        "table_comment": "This table provides information about the locations where pick up or drop off is possible",
        "column_comments": {
            "level_id": "Id of the level that can be referenced from stops.txt.",
            "level_index": "Collection of 'Feature' objects describing the locations",
            "level_name": "Optional name of the level (that matches level lettering/numbering used inside the building or the station)",
        },
         "table_constraints": ["levels_pk PRIMARY KEY(level_id)"],
    },
]

            },
        ],
    },
]

# COMMAND ----------

# parsing the file to add documentation and constraints

# get and set catalog
for catalog_docs in catalog_documentation:
    catalog = catalog_docs.get('catalog_name')
    catalog_comment = catalog_docs.get('catalog_comment')
    spark.sql(f'USE CATALOG {catalog}')
    spark.sql(f'COMMENT ON CATALOG {catalog} IS "{catalog_comment}";')

    # get and set schema
    for schema_docs in catalog_docs.get("schema_documentation"):
        schema = schema_docs.get("schema_name")
        schema_comment = schema_docs.get("schema_comment")
        spark.sql(f'USE SCHEMA {schema}')
        spark.sql(f'COMMENT ON SCHEMA {schema} IS "{schema_comment}";')
    
    # get table
    for table_docs in schema_docs.get("table_documentation"):
        table = table_docs.get("table_name")
        table_comment = table_docs.get("table_comment")
        spark.sql(f'COMMENT ON TABLE {table} IS "{table_comment}";')
        for column, comment in table_docs.get("column_comments").items():
          spark.sql(f'ALTER TABLE {table} ALTER COLUMN {column} COMMENT "{comment}"')
        for constraint in table_docs.get("table_constraints"):
          spark.sql(f'ALTER TABLE {table} ADD CONSTRAINT {constraint}')


# COMMAND ----------

# MAGIC %sql
# MAGIC -- now let's check the table, you should be able to find the table level comment and any constraints created
# MAGIC DESCRIBE TABLE EXTENDED pathways; 

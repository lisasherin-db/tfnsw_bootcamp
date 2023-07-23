# Databricks notebook source
dbutils.fs.rm(datasets_location, True)

# spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name}')
spark.sql(f'create database if not exists {database};')
spark.sql(f'use {database}')

print (f"Created database :  {database}") 
print(f"bronze table name: {bronze_table_name}")

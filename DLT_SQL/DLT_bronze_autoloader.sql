-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze_train
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze train table with all transactions"
AS 
SELECT * 
FROM
cloud_files( '${mypipeline.input_path}' , "parquet") 

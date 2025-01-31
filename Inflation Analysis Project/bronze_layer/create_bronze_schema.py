# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists inflation_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC use schema inflation_bronze;

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop schema inflation_bronze_layer

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------


# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists inflation_silver;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC use schema inflation_silver;

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------


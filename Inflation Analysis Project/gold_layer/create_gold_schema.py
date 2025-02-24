# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists inflation_gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC use schema inflation_gold;

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------


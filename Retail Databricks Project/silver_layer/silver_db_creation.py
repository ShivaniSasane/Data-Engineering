# Databricks notebook source
spark.sql("create database if not exists retail_silver")

# COMMAND ----------

spark.sql("show databases").show()
spark.sql("use database retail_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select current_database()

# COMMAND ----------


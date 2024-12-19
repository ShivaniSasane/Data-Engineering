# Databricks notebook source
spark.sql("create database if not exists retail_bronze")

# COMMAND ----------

spark.sql("show databases").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()
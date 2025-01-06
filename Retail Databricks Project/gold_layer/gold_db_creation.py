# Databricks notebook source
spark.sql("""
          create database if not exists retail_gold
          """)
spark.sql("use retail_gold")          

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database();

# COMMAND ----------


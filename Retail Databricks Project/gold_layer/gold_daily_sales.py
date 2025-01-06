# Databricks notebook source
spark.sql("use retail_gold")
spark.sql("""
          CREATE or Replace table gold_daily_sales
          as
          select transaction_date, sum(total_amount) as daily_total_sales
          FROM retail_silver.silver_transactions
          group by transaction_date
          order by transaction_date
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_gold.gold_daily_sales;

# COMMAND ----------


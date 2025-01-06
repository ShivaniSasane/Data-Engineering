# Databricks notebook source
spark.sql("use retail_gold")
spark.sql("""
          create or replace table gold_category_sales as
          select p.category as product_catogary,
            sum(t.total_amount) as catogary_total_sales
            from retail_silver.silver_products p
            join retail_silver.silver_transactions t on p.product_id = t.product_id
            group by p.category
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_gold.gold_category_sales;
# Databricks notebook source
spark.sql("use retail_silver")
spark.sql("""create table if not exists silver_products(
    product_id string,
    name string,
    category string,
    brand string,
    price double,
    stock_quantity int,
    rating double,
    is_active boolean,
    price_category string,
    stock_status string,
    last_updated timestamp
)
using delta""")

# COMMAND ----------

last_processed_df = spark.sql("select max(last_updated) as last_processed from silver_products")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']
if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'
    
print(last_processed_timestamp)

# COMMAND ----------

spark.sql(f"""create or replace temporary view bronze_incremental_products as
          select * from retail_bronze.bronze_product cust where cust.ingestion_timestamp > '{last_processed_timestamp}'""")

# COMMAND ----------

spark.sql("""
          create or replace temporary view silver_incremental_products as
          select product_id, name, category, brand,
          case when price < 0 then 0 else price end as price,
          case when stock_quantity < 0 then 0 else stock_quantity end as stock_quantity,
          case when rating < 0 then 0
                when rating > 5 then 5
          else rating end as rating,
          is_active,
          case when price > 1000 then "Premium"
          when price > 100 then "Standard"
          else "Budget" end as price_category,
          case when stock_quantity < 0 then "Out of stock"
          when stock_quantity < 10 then "Low stock"
          when stock_quantity < 50 then "Out of stockModerate stock"
          else "Sufficient stock"
          end as stock_status,
          current_timestamp() as last_updated
          from bronze_incremental_products
          where name is NOT NULL and category is NOT NULL
          """)

# COMMAND ----------

spark.sql("""
          merge into silver_products as target
          using silver_incremental_products as source
          on target.product_id = source.product_id
          when matched then 
          update set *
          when not matched then
          insert *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_silver.silver_products;

# COMMAND ----------


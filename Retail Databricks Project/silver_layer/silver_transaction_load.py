# Databricks notebook source
spark.sql("use retail_silver")
spark.sql("""
          create table if not exists silver_transactions(
              transaction_id string,
              customer_id string,
              product_id string,
              quantity int,
              total_amount double,
              transaction_date date,
              payment_method string,
              store_type string,
              order_status string,
              last_updated timestamp
        )
        using delta
          """
)

# COMMAND ----------

last_processed_df = spark.sql("select max(last_updated) as last_processed from silver_transactions")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']
if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'
    
print(last_processed_timestamp)

# COMMAND ----------

spark.sql(f"""create or replace temporary view bronze_incremental_transactions as
          select * from retail_bronze.bronze_transaction transactions where transactions.ingestion_timestamp > '{last_processed_timestamp}'""")
          

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_incremental_transactions;

# COMMAND ----------

spark.sql("""
          create or replace temporary view silver_incremental_transactions
          as
          select 
          transaction_id, product_id, customer_id,
          case when quantity < 0 then 0 else quantity end as quantity,
          case when total_amount < 0 then 0 else total_amount end as total_amount,
          cast(transaction_date as date) as transaction_date,
          payment_method,
          store_type,
          case when quantity =0 or total_amount = 0 then 'cancelled' else 'completed' end as order_status,
          current_timestamp() as last_updated  
          from bronze_incremental_transactions
          where transaction_date is not null 
          and product_id is not null 
          and customer_id is not null
          """)

# COMMAND ----------

spark.sql("select * from silver_incremental_transactions").show()

# COMMAND ----------

spark.sql("""
          Merge into silver_transactions as target
          using silver_incremental_transactions as source
          on target.transaction_id = source.transaction_id
          when matched then update set *
          when not matched then insert *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_silver.silver_transactions;

# COMMAND ----------


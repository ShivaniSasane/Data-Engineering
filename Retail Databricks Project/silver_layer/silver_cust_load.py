# Databricks notebook source
spark.sql("use retail_silver")
spark.sql("""create table if not exists silver_cust(
customer_id string,
name string,
email string,
country string,
customer_type string,
registration_date date,
age int,
gender string,
total_purchases int,
customer_segment string,
days_since_registration int,
last_updated timestamp
)""")

# COMMAND ----------


last_processed_df = spark.sql("select max(last_updated) as last_processed from silver_cust")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']
if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'
    
print(last_processed_timestamp)


# COMMAND ----------

spark.sql(f"""create or replace temporary view bronze_incremental_cust as
          select * from retail_bronze.bronze_cust cust where cust.ingestion_timestamp > '{last_processed_timestamp}'""")

# COMMAND ----------

spark.sql("select * from bronze_incremental_cust").show()

# COMMAND ----------

spark.sql("""
          create or replace temporary view silver_cust_incremental as
          select customer_id, name, email, country, customer_type, registration_date, age, gender, total_purchases,
          case when total_purchases > 1000 then 'high value' 
          when total_purchases > 5000 then 'medium value'else 'low value' 
          end as customer_segment,
          datediff(current_date(),registration_date) as days_since_registration,
          current_timestamp() as last_updated
           from bronze_incremental_cust
          where 
          age between 18 and 120 and
          email is not null and
          total_purchases >= 0 
          """)


# COMMAND ----------


display(spark.sql("select * from silver_cust_incremental"))


# COMMAND ----------

spark.sql("""
          merge into silver_cust target
          using silver_cust_incremental source
          On target.customer_id = source.customer_id
          when matched then update set *
          when not matched then insert *
          """)

# COMMAND ----------

spark.sql("select count(*) from silver_cust").show()

# COMMAND ----------


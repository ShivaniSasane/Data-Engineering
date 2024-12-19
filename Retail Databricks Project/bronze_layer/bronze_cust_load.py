# Databricks notebook source
from pyspark.sql.types import *

filePath = "/Volumes/workspace/retail/bronze_layer/customer_data/customer.csv"

df_customers = spark.read.csv(filePath, inferSchema=True, header=True)
df_customers.printSchema()
df_customers.show()


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new = df_customers.withColumn("ingestion_timestamp",current_timestamp())
df_new.show()


# COMMAND ----------

spark.sql("use retail_bronze")
#spark.sql("drop table if exists bronze_cust")
df_new.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("bronze_cust")

# COMMAND ----------

import datetime
archive_folder ="/Volumes/workspace/retail/bronze_layer/customer_data/archive/"
archive_file_path = archive_folder+filePath.split("/")[-1].split(".")[0]+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M%s"+'.txt')
print(archive_file_path)
dbutils.fs.mv(filePath,archive_file_path)


# COMMAND ----------


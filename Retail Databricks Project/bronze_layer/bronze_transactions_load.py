# Databricks notebook source
from pyspark.sql.types import *

filePath = "/Volumes/workspace/retail/bronze_layer/transactions/transactions.snappy.parquet"

df_transactions = spark.read.parquet(filePath)
df_transactions.printSchema()
df_transactions.show()


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col
df_trans_update = df_transactions.withColumn("transaction_date",to_timestamp(col("transaction_date")))
df_trans_update.printSchema()
df_trans_update.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_transaction_update = df_trans_update.withColumn("ingestion_timestamp",current_timestamp())
df_transaction_update.show()


# COMMAND ----------

spark.sql("use retail_bronze")
df_transaction_update.write.format("delta").mode("append").saveAsTable("bronze_transaction")


# COMMAND ----------

import datetime
archive_folder ="/Volumes/workspace/retail/bronze_layer/transactions/archive/"
archive_file_path = archive_folder+filePath.split("/")[-1].split(".")[0]+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M%s"+'.snappy.parquet')
print(archive_file_path)
dbutils.fs.mv(filePath,archive_file_path)


# COMMAND ----------


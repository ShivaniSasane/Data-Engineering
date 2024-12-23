# Databricks notebook source
from pyspark.sql.types import *

filePath = "/Volumes/workspace/retail/bronze_layer/product_catalog/product.json"

df_product = spark.read.json(filePath)
df_product.printSchema()
df_product.show()


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_prod_new = df_product.withColumn("ingestion_timestamp",current_timestamp())
df_prod_new.show()


# COMMAND ----------

spark.sql("use retail_bronze")
df_prod_new.write.format("delta").mode("append").saveAsTable("bronze_product")

# COMMAND ----------

import datetime
archive_folder ="/Volumes/workspace/retail/bronze_layer/product_catalog/archive/"
archive_file_path = archive_folder+filePath.split("/")[-1].split(".")[0]+"_"+datetime.datetime.now().strftime("%Y%m%d%H%M%s"+'.json')
print(archive_file_path)
dbutils.fs.mv(filePath,archive_file_path)


# COMMAND ----------


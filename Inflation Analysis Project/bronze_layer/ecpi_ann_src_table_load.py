# Databricks notebook source
filepath = "/Volumes/workspace/inflation_schema/inflation_volume"
ecpi_annual_filename = "Inflation-data_ecpi_a.csv"


# COMMAND ----------

from pyspark.sql.types import *

table_schema = StructType([
    StructField("Country_Code", StringType(), True),
    StructField("IMF_Country_Code", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Indicator_Type", StringType(), True),
    StructField("Series_Name", StringType(), True), 
    *[StructField(str(year), DoubleType(), True) for year in range(1970, 2024)],
    StructField("Note", StringType(), True)
])

# COMMAND ----------

df_ann_ecpi = spark.read.csv(f"{filepath}/{ecpi_annual_filename}", header=True, schema=table_schema)
df_ann_ecpi.show(10)


# COMMAND ----------

df_ann_ecpi.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("inflation_bronze.inflation_ecpi_annual")

# COMMAND ----------


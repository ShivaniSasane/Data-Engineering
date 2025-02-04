# Databricks notebook source
filepath = "/Volumes/workspace/inflation_schema/inflation_volume"
hcpi_annual_filename = "Inflation-data_hcpi_a.csv"


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

df_ann_hcpi = spark.read.csv(f"{filepath}/{hcpi_annual_filename}", header=True, schema=table_schema)
df_ann_hcpi.show(10)


# COMMAND ----------

df_ann_hcpi.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("inflation_bronze.inflation_hcpi_annual")

# COMMAND ----------


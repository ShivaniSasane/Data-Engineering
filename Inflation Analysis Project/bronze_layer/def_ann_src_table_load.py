# Databricks notebook source
filepath = "/Volumes/workspace/inflation_schema/inflation_volume"
def_annual_filename = "Inflation-data_def_a.csv"


# COMMAND ----------

from pyspark.sql.types import *

def_table_schema = StructType([
    StructField("Country_Code", StringType(), True),
    StructField("IMF_Country_Code", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Indicator_Type", StringType(), True),
    StructField("Series_Name", StringType(), True), 
    *[StructField(str(year), DoubleType(), True) for year in range(1970, 2024)]
])

# COMMAND ----------

df_ann_def = spark.read.csv(f"{filepath}/{def_annual_filename}", header=True, schema=def_table_schema)
df_ann_def.show(10)


# COMMAND ----------

df_ann_def.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("inflation_bronze.inflation_def_annual")
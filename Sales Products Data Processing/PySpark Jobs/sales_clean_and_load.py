"""
Load Sales data in csv file as initial step. This file will be read by another job to perform some other actions.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Sales Project") \
        .getOrCreate()

# Schema for US Sales Data
sales_us_schema =StructType([StructField("SalesKey",IntegerType(), False),
                             StructField("OrderDate", DateType(), True),
                             StructField("CustomerKey", IntegerType(), False), 
                             StructField("TerritoryRegion", StringType(), True),
                             StructField("OrderNumber",	StringType(), True),
                             StructField("LineNumber", IntegerType(), True),
                             StructField("Quantity", DecimalType(10,4), True),
                             StructField("Price", DecimalType(10,4), True),
                             StructField("Freight", DecimalType(10,4), True)])

# Read and clean US Sales Data. Also display number of records
df_sales_us = spark.read.csv(r"D:\\Data\\Sales Data\\United States\\Sales 2019 and later.txt",sep="\t", schema=sales_us_schema, header=False)
df_sales_us = df_sales_us.dropna(subset=["SalesKey","CustomerKey"])
df_sales_us = df_sales_us.drop("Freight")
df_sales_us = df_sales_us.filter(col("OrderDate") >= "2020-01-01")

df_sales_us.show()
df_sales_us.printSchema()

df_sales_us_count=df_sales_us.count()
print("Count of Sales in US: ",df_sales_us_count)

# Schema for other countries Sales data
sales_others_schema =StructType([StructField("SalesKey",IntegerType(), False),
                             StructField("OrderDate", DateType(), True),
                             StructField("CustomerKey", IntegerType(), False), 
                             StructField("TerritoryRegion", StringType(), True),
                             StructField("OrderNumber",	StringType(), True),
                             StructField("LineNumber", IntegerType(), True),
                             StructField("Quantity", DecimalType(10,4), True),
                             StructField("Price", DecimalType(10,4), True)])

# Read and clean other countries sales data and display number of records
other_countries_path="D:/Data/Sales Data/Other Countries"
df_sales_others = spark.read.option("header", False) \
                .schema(sales_others_schema) \
                .csv(other_countries_path, recursiveFileLookup=True) 
df_sales_others = df_sales_others.dropna(subset=["SalesKey","CustomerKey"])
df_sales_others = df_sales_others.filter(col("OrderDate") >= "2020-01-01")

df_sales_others.show()
df_sales_others.printSchema()

df_sales_others_count=df_sales_others.count()
print("Count of Sales in other countries: ",df_sales_others_count)

# combine sales data for US and other countries to get all country's data
df_sales = df_sales_us.union(df_sales_others)
df_sales.show()

df_sales_count=df_sales.count()
print("Count of Sales in all countries: ",df_sales_count)

#write data to csv file
df_sales.coalesce(1).write.mode("overwrite").option("header", True).csv("D:/Data/sales-csv")

spark.stop()
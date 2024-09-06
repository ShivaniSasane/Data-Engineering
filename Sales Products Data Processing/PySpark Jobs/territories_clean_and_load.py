"""
Clean and load Sales Territories Data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Sales Project") \
        .getOrCreate()

# Schema for sales territories data
sales_territories_schema =StructType([StructField("TerritoryKey",IntegerType(), False),
                             StructField("TerritoryRollup", StringType(), True)
                             ])

# Read and clean other countries sales data and display number of records
dimensions_path="D:/Data/Dimensions"

df_sales_territories = spark.read.option("header", False) \
                .schema(sales_territories_schema) \
                .csv(dimensions_path+"/Sales Territories.csv") 

df_sales_territories = df_sales_territories.dropna(subset=["TerritoryKey"])
df_sales_territory = df_sales_territories.withColumn("Territory",split(trim(df_sales_territories["TerritoryRollup"]), "\|").getItem(0)) \
                                            .withColumn("Country",split(trim(df_sales_territories["TerritoryRollup"]), "\|").getItem(1))
df_sales_territory =df_sales_territory.drop("TerritoryRollup")
df_sales_territory.printSchema()
df_sales_territory.show()

df_sales_territories_count=df_sales_territories.count()
print("Count of sales territories: ",df_sales_territories_count)

#write data to csv file
df_sales_territory.coalesce(1).write.mode("overwrite").option("header", True).csv("D:/Data/territories-csv")

spark.stop()
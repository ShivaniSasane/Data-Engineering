"""
Read Sales, Customers, Products data and perform joins and aggregations to display required values
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Sales Project") \
        .getOrCreate()

source_path ="D:\\Data\\"

# Read Customer, Sales and Products data
df_sales = spark.read.csv(source_path+"sales-csv\\*.csv", inferSchema=True, header=True)
df_sales = df_sales.withColumnRenamed("CustomerKey","CustomerKey_left")
df_customers = spark.read.csv(source_path+"customers-csv\\*.csv", inferSchema=True, header=True)
df_products = spark.read.csv(source_path+"products-csv\\*.csv", inferSchema=True, header=True)

# Join Customer and Sales
df_cust_sales = df_sales.join(df_customers, df_sales.CustomerKey_left == df_customers.CustomerKey, "inner")
# Join Customer, Sales and Products
df_cust_sales_prod = df_cust_sales.join(df_products, df_products.ProductKey == df_cust_sales.SalesKey,"inner") \
                    .select("ProductKey","OrderDate","TerritoryRegion","OrderNumber","Quantity","Price", \
                            "CustomerKey","FirstName","LastName","StateName","Country","Gender","BirthDate", \
                            "ProductCode","ProductSubCategory","ProductName","ProductCategory")

# Find total price and number of quantities sold per customer. Display customer first name and last name 
df_sales_aggregation = df_cust_sales.groupBy("CustomerKey","FirstName","LastName") \
                        .agg(sum("Price").alias("TotalPrice"), sum("Quantity").alias("TotalQuantity")).show()

# Find total sales per month for year 2020
df_sales_count = df_sales.groupBy(year(df_sales["OrderDate"]).alias("Year"),month(df_sales["OrderDate"]).alias("Month")) \
                        .agg(count("SalesKey").alias("TotalSales")) \
                        .filter(col("Year").like("%2020%")) \
                        .orderBy("Year","Month").show()

# Total sales by country
df_sales_by_country = df_cust_sales_prod.groupBy("Country") \
                        .agg(count("ProductKey").alias("TotalProducts"),sum("Price").alias("TotalPrice")).show()

# Count of ProductCategory purchased by Gender
df_female_products_purchased = df_cust_sales_prod.groupBy("Gender","ProductCategory") \
                                .agg(count("ProductKey").alias("ProductCount")).show()

spark.stop()
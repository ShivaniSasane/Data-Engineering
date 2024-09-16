"""
Clean, join and load Products data into csv
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Sales Project") \
        .getOrCreate()

# Read data of Product Rollup Data
product_rollup_schema =StructType([StructField("ProductSubcategory_right",StringType(),True),
                                StructField("ProductCategory",StringType(),True)
                                ])

df_product_rollup = spark.read.csv(r"D:\\Data\\Dimensions\\Product Rollup.txt",sep="\t", schema=product_rollup_schema, header=False)
df_product_rollup = df_product_rollup.filter(~col("ProductSubcategory_right").like("%Product Subcategory"))
df_product_rollup = df_product_rollup.dropna(subset=["ProductCategory"])
df_product_rollup = df_product_rollup.withColumn("ProductSubcategory_right",initcap("ProductSubcategory_right"))

df_product_rollup.printSchema()
df_product_rollup.show()

df_product_rollup_count=df_product_rollup.count()
print("Count of product rollups: ",df_product_rollup_count)

# Schema for Products data
products_schema =StructType([StructField("ProductKey",IntegerType(), False),
                             StructField("ProductCode", StringType(), False), 
                             StructField("ProductSubCategory", StringType(), True),
                             StructField("SizeMeasureUnit", StringType(), True),
                             StructField("ProductName", StringType(), True),
                             StructField("StandardCost", DecimalType(10,4), True),
                             StructField("Color", StringType(), True),
			     StructField("Sensitive", StringType(), True)])

# Read and clean other countries sales data and display number of records
dimensions_path="D:/Data/Dimensions"
df_products = spark.read.option("header", False) \
                .schema(products_schema) \
                .csv(dimensions_path+"/Product.csv") 
df_products = df_products.dropna(subset=["ProductKey"]).drop("SizeMeasureUnit","Sensitive")
df_products = df_products.withColumn("ProductSubCategory",initcap("ProductSubCategory"))
df_products = df_products.filter(~col("ProductSubCategory").like("%Internal Parts"))

df_products.printSchema()
df_products.show()

df_products_count=df_products.count()
print("Count of products : ",df_products_count)

# Join dataframes Products and products Rollup
df_products_joined = df_products.join(df_product_rollup, df_products.ProductSubCategory == df_product_rollup.ProductSubcategory_right, "left")
df_products_joined = df_products_joined.drop("ProductSubcategory_right").distinct()

#df_products_joined.show()

df_products_joined_count=df_products_joined.count()
print("Count of joined distinct products: ",df_products_joined_count)


#write data to csv file
df_products_joined.coalesce(1).write.mode("overwrite").option("header", True).csv("D:/Data/products-csv")

spark.stop()
"""
Customers data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("Sales Project") \
        .getOrCreate()

# Read data of Customers
customer_schema =StructType([StructField("CustomerKey",IntegerType(),False),
                             StructField("FirstName",StringType(),False),
                             StructField("LastName",StringType(),False),
                             StructField("City",StringType(),True),
                             StructField("StateName",StringType(),True),
                             StructField("Country",StringType(),True),
                             StructField("BirthDate",DateType(),True),
                             StructField("MS",StringType(),True),
                             StructField("Gender",StringType(),True),
                             StructField("YearlyIncome",IntegerType(),True),
                             StructField("UserName",StringType(),True),
                             StructField("TotalChildren",IntegerType(),True),
                             StructField("ChildrenAtHome",IntegerType(),True),
                             StructField("HouseOwner",IntegerType(),True),
                             StructField("CarsOwned",IntegerType(),True)])

df_customers = spark.read.csv(r"D:\\Data\\Dimensions\\CustomerList.txt",sep="\t", schema=customer_schema, header=False)
df_customers = df_customers.dropna(subset=["CustomerKey"])
df_customers.printSchema()
df_customers.show()

df_customers_count=df_customers.count()
print("Count of Customers in all countries: ",df_customers_count)

#write data to csv file
#df_output.coalesce(1).write.mode("overwrite").option("header", True).csv("D:/Data/customers-csv")

spark.stop()
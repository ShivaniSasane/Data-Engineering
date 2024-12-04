from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

#read aws s3 csv file

spark = SparkSession.builder.master("local[*]").appName("Read Local CSV File").getOrCreate()
path_to_csv="D:\\Data\\data_by_Geography_and_Service_2022.csv"
df = spark.read.csv(path_to_csv,header=True, inferSchema=True)

df.printSchema()

#modify data to remove $
df_modified=df.withColumn("Avg_Submtd_Cvrd_Chrg",regexp_replace(col("Avg_Submtd_Cvrd_Chrg"), "^.", "")) \
            .withColumn("Avg_Tot_Pymt_Amt",regexp_replace(col("Avg_Tot_Pymt_Amt"),"^.","")) \
            .withColumn("Avg_Mdcr_Pymt_Amt",regexp_replace(col("Avg_Mdcr_Pymt_Amt"),"^.","")) \
            .withColumn("Chrg_Amt_Currency",lit("USD"))

#modify data to remove "," and convert to decimal type 
df_cast=df_modified.withColumn("Avg_Submtd_Cvrd_Chrg",regexp_replace("Avg_Submtd_Cvrd_Chrg",",","").cast(DecimalType(20,2))) \
            .withColumn("Avg_Tot_Pymt_Amt",regexp_replace("Avg_Tot_Pymt_Amt",",","").cast(DecimalType(20,2))) \
            .withColumn("Avg_Mdcr_Pymt_Amt",regexp_replace("Avg_Mdcr_Pymt_Amt",",","").cast(DecimalType(20,2)))

#calculate sum of columns
print(f"Following are the sum of all discharges, average of covered charges, total payments and medicare payments:")
df_group=df_cast.groupBy("Rndrng_Prvdr_Geo_Desc") \
            .agg(sum("Tot_Dschrgs").alias("sum_tot_dschrgs"), \
                 sum("Avg_Submtd_Cvrd_Chrg").alias("sum_avg_submtd_cvrd_chrg"), \
                 sum("Avg_Tot_Pymt_Amt").alias("sum_avg_tot_pymt_amt"), \
                 sum("Avg_Mdcr_Pymt_Amt").alias("sum_avg_mdcr_pymt_amt"))

df_group.show()

#write to local
df_group.coalesce(1).write.mode("overwrite").option("header", True).csv("D:/Data/ip-hospitals-local-op-csv")
spark.stop()

"""
This script reads data from aws file data_by_Geography_and_Service_2022.csv 
for year 2022 and writes data into aws directory. Both files are in csv format.
Calculates sum of all avg values per geographical location in USA
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

#read aws s3 csv file
spark = SparkSession.builder \
    .appName("S3Connection") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "XYZ")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "ABC")

df = spark.read.csv("s3a://REDACT/data_by_Geography_and_Service_2022.csv",header=True, inferSchema=True)

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

#write to aws S3
df_group.coalesce(1).write.mode("overwrite").option("header", True).csv("s3a://REDACT/ip-hospitals-aws-load-csv/")
spark.stop()

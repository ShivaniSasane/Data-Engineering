# This script will send the records from local CSV file to AWS Kinesis stream. In AWS, data is read further using firehose and put into S3 bucket

import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("Send Local CSV File to Kinesis") \
.config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()
    
kinesis_stream_name = 'medicare-ip-hospitals-stream'
aws_region = 'us-east-1'
partition_key = 'Rndrng_Prvdr_Geo_Cd'  

kinesis_client = boto3.client('kinesis', region_name=aws_region)

csv_file_path = 'D:\\Data\\data_by_Geography_and_Service_2021.csv'
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

df.printSchema()

# Rows to Json records conversion
json_records = df.toJSON().collect()

kinesis_client = boto3.client('kinesis', region_name=aws_region, \
                              aws_access_key_id='ABC',aws_secret_access_key='XYZ')

# Send records to Kinesis
for record in json_records:
    try:
        kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=record,
            PartitionKey=partition_key
        )
        print(f"Record Transfer Success: {record}")
    except Exception as e:
        print(f"Record Transfer Failed: {record}")
        print(f"Error: {str(e)}")

spark.stop()

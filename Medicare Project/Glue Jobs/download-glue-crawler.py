# This script is used to download the crawler from aws glue catalog in json file format
import boto3
import json

glue = boto3.client('glue', region_name='us-east-1', aws_access_key_id='WWW',aws_secret_access_key='XYZ')  
crawler_name = "sample-crawler-name"

response = glue.get_crawler(Name=crawler_name)
crawler_config = response["Crawler"]

#Save crawler as JSON file
with open("D:\Data\sample_crawler_config.json", "w") as f:
    json.dump(crawler_config, f, indent=4, default=str)

print("Crawler configuration exported for ",crawler_name)

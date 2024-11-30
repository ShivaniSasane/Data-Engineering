# This project will copy s3 files from source bucket to destination buckets as per the contents if they are integer or non-integer
# This will also delete the original files from source bucket
from airflow import DAG, XComArg
from airflow.decorators import task
import pendulum
from airflow.providers.amazon.aws.operators.s3 import (S3CopyObjectOperator,S3ListOperator,S3DeleteObjectsOperator)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_SOURCE_BUCKET = "redact-example-bucket"
S3_INTEGER_BUCKET = "redact-example-bucket/integer-files"
S3_NOT_INT_BUCKET = "redact-example-bucket/non-integer-files"


with DAG("aws_s3_file_copy_project", schedule=None, start_date=pendulum.datetime(2024,11,10),catchup=False,tags=['aws'],doc_md=__doc__):
    list_s3_files_src_bucket = S3ListOperator(task_id="list_s3_src_buckets",
                                          aws_conn_id="redact",
                                          bucket=S3_SOURCE_BUCKET)

    @task
    def read_files_from_s3(src_files_list):
        s3_hook=S3Hook(aws_conn_id="redact")
        print(src_files_list)
        content_list=[]
        for file in src_files_list:
            file_content=s3_hook.read_key(
                key=file,
                bucket_name=S3_SOURCE_BUCKET)
            content_list.append(file_content)
        return content_list


    content_list=read_files_from_s3(XComArg(list_s3_files_src_bucket))
    
    @task
    def check_if_int(content_list):
        destination_list=[]
        for content_item in content_list:
            if isinstance(content_item,int):
                destination_list.append(S3_INTEGER_BUCKET)
            else:
                destination_list.append(S3_NOT_INT_BUCKET)
        return destination_list
    
    dest_file_list = check_if_int(content_list)

    @task
    def gen_src_dest_pairs(src_file_list,dest_file_list):
        list_src_dest_pairs=[]
        for s,d in zip(src_file_list,dest_file_list):
            list_src_dest_pairs.append(
                {
                    "source_bucket_key" : f"s3://{S3_SOURCE_BUCKET}/{s}",
                    "dest_bucket_key" : f"s3://{d}/{s}"
                }
            )
        return list_src_dest_pairs
    
    src_dest_pairs = gen_src_dest_pairs(XComArg(list_s3_files_src_bucket),dest_file_list)

    copy_files_s3 = S3CopyObjectOperator.partial(       #partial will ensure that tfor each file task_id and conn_id remains same
        task_id="copy_s3_files",
        aws_conn_id="redact"
    ).expand_kwargs(src_dest_pairs)   # dynamic task mapping

    delete_src_bucket_contents = S3DeleteObjectsOperator.partial(
        task_id = "delete_s3_files",
        aws_conn_id= "redact",
        bucket = S3_SOURCE_BUCKET
    ).expand(keys=XComArg(list_s3_files_src_bucket))


    copy_files_s3 >> delete_src_bucket_contents
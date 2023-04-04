import boto3
from airflow.models import Variable
import os

def load_data_from_s3(**kwargs):
    acces_key = Variable.get("aws_access_key_id")
    secret_key = Variable.get("aws_secret_access_key")
    s3_endpoint = Variable.get("s3_endpoint_url")
    
    s3_client = boto3.resource(
        "s3",
        "us-east-1",
        aws_access_key_id = acces_key,
        aws_secret_access_key = secret_key,
        endpoint_url= s3_endpoint,
        use_ssl= False,
        verify= False
    )

    my_bucket = s3_client.Bucket('ccma-pre')

    for file in my_bucket.objects.all():
        print(file.key)

def download_from_s3 (file_path: str, bucket_name: str, local_path: str, file_name: str) -> str:
    acces_key = Variable.get("aws_access_key_id")
    secret_key = Variable.get("aws_secret_access_key")
    s3_endpoint = Variable.get("s3_endpoint_url")

    s3_client = boto3.client(
        "s3",
        "us-east-1",
        aws_access_key_id = acces_key,
        aws_secret_access_key = secret_key,
        endpoint_url= s3_endpoint,
        use_ssl= False,
        verify= False
    )

    if not os.path.exists(local_path):
        os.makedirs(local_path, exist_ok=True)

    out_dir = local_path + "/" + file_name
    s3_client.download_file(bucket_name, file_path, out_dir)
    print("Saved at " + local_path)

def read_data_from_s3 (file_path: str, bucket_name: str) -> str:
    acces_key = Variable.get("aws_access_key_id")
    secret_key = Variable.get("aws_secret_access_key")
    s3_endpoint = Variable.get("s3_endpoint_url")

    s3_client = boto3.resource(
        "s3",
        "us-east-1",
        aws_access_key_id = acces_key,
        aws_secret_access_key = secret_key,
        endpoint_url= s3_endpoint,
        use_ssl= False,
        verify= False
    )

    obj = s3_client.Object(bucket_name, file_path)
    file_content = obj.get()['Body'].read().decode('utf-8')
    return file_content

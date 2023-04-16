import boto3
import os
from airflow.models import Variable
from funcions.utilities.filesystem_utiles import create_folder

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

def download_from_s3 (bucket_name: str, file_path: str, file_name: str) -> str:
    # Create S3 clien connection
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

    # Create temporal path if not exists
    print("file_path " + file_path)
    local_path = os.path.join("/opt/bitnami/airflow/tmp",  file_path)
    create_folder(local_path)

    # Dowload data to temporal path
    out_dir =  os.path.join(local_path,  file_name)
    print("Save files at " + out_dir)
    s3_client.download_file(bucket_name, file_path, out_dir)
    

def read_data_from_s3 (bucket_name: str, file_path: str, file_name: str) -> str:
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
    complete_file_path = os.path.join(file_path, file_name)
    obj = s3_client.Object(bucket_name, complete_file_path)
    file_content = obj.get()['Body'].read().decode('utf-8')
    return file_content

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import boto3

def load_data_from_s3(**kwargs):
    '''
    s3_hook = S3Hook(aws_conn_id='cmma-pre-s3')
    bucket_name = 'ccma-pre'
    s3_key = 'prova.txt'
    local_file = 'prova.txt'
    #s3_hook.download_file(bucket_name, s3_key, local_file)
    keys = s3_hook.list_keys(bucket_name)
    for key in keys:
        print(key)
     '''
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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'get_data_from_s3',
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily',
    tags=['s3', 'test']
) as dag:

    t1 = PythonOperator(
        task_id='load_data',
        python_callable=load_data_from_s3,
    )

t1

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def load_data_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id='cmma-pre-s3')
    bucket_name = 'ccma-pre'
    s3_key = 'prova.txt'
    local_file = 'prova.txt'
    #s3_hook.download_file(bucket_name, s3_key, local_file)
    keys = s3_hook.list_keys(bucket_name)
    for key in keys:
        print(key)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'load_data_from_s3',
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily',
) as dag:

    t1 = PythonOperator(
        task_id='load_data',
        python_callable=load_data_from_s3,
    )

t1

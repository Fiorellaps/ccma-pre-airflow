from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3
import os
import sys

sys.path.insert(0,'./')
sys.path.insert(0,'/dags/dags')


global_dag_config = {
    "job_name": "ETL-GFK",
    "description":"Ingesta GFK",
    "owner":"ccma",
    "email_dest":["fpa@nextret.net"]
}
current_path = "casos_uso/territori/"

dag_arguments =  {
    #"end_date": datetime(),
    #"depends_on_past": False,
    #"email": ['fpa@nextret.net'],
    #"email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    #"dagrun_timeout":timedelta(minutes=60)
    "max_active_runs": 1,
    "start_date": datetime(2022, 12, 1),
    "provide_context": True
}

def create_folder(path):
    print("create path", path)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        print("path created", path)

def download_from_s3 (bucket_name: str, file_path: str, file_name: str) -> str:
    # Create S3 clien connection
    print("---path",sys.path)
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
    local_path = os.path.join("/tmp/bitnami",  file_path)
    create_folder(local_path)

    # Dowload data to temporal path
    out_dir =  os.path.join(local_path,  file_name)
    origin_path = os.path.join(file_path,  file_name)
    print("Save files at " + out_dir)
    print("bucket", bucket_name)
    print("origin_path", origin_path)
    s3_client.download_file(bucket_name, origin_path, out_dir)

with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:
    config_download_jar_from_s3 = {
        "file_path": "gfk",
        "bucket_name": "airflowdags",
        "file_name": "ccma-etl-0.2311.0-SNAPSHOT-jar-with-dependencies.jar"
    }
    download_data_from_s3 = PythonOperator(
            task_id='download_data_from_s3',
            python_callable=download_from_s3,
            op_kwargs={
                "file_path": config_download_jar_from_s3["file_path"],
                "bucket_name": config_download_jar_from_s3["bucket_name"],
                "file_name": config_download_jar_from_s3["file_name"]
            },
            dag=dag,
    )
    download_data_from_s3
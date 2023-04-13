from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.trino.operators.trino import TrinoOperator

from datetime import datetime, timedelta
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
def read_data_from_s3 (bucket_name: str, file_path: str) -> str:
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
    print("read", file_path)
    obj = s3_client.Object(bucket_name, file_path)
    file_content = obj.get()['Body'].read().decode('utf-8').replace(';', '')
    print("content", file_content)
    return file_content

 
    
    

def create_folder(path):
    print("create path", path)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        print("path created", path)

def download_from_s3 (bucket_name: str, 
                      folder_path: str, 
                      file_path: str, 
                      local_folder_path: str,
                      local_file_path: str) -> str:
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
    print("folder_path " + folder_path)

    create_folder(local_folder_path)

    # Dowload data to temporal path
    
    print("Save files at " + local_file_path)
    print("bucket", bucket_name)
    print("origin_path", file_path)
    s3_client.download_file(bucket_name, file_path, local_file_path)

with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:
    
    '''config_download_jar_from_s3 = {
        "folder_path": "gfk",
        "bucket_name": "airflowdags",
        "file_path": "gfk/ccma-etl-0.2311.0-SNAPSHOT-jar-with-dependencies.jar"
    }
    local_folder_path = os.path.join("/opt/bitnami/tmp",  config_download_jar_from_s3["folder_path"])
    local_file_path = os.path.join("/opt/bitnami/tmp",  config_download_jar_from_s3["file_path"])

    download_data_from_s3 = PythonOperator(
            task_id='download_data_from_s3',
            python_callable=download_from_s3,
            op_kwargs={
                "folder_path": config_download_jar_from_s3["folder_path"],
                "bucket_name": config_download_jar_from_s3["bucket_name"],
                "file_path": config_download_jar_from_s3["file_path"],
                "local_folder_path": local_folder_path,
                "local_file_path": local_file_path
            },
            dag=dag,
    )'''
    '''
    execute_jar = BashOperator(
    task_id='task_execute_jar',
    bash_command='cd ' + local_folder_path + ' && java -jar ' + java_config['tmp_path'] + '/' + java_config["jar_name"] ,
    dag=dag
    )'''
    spark_config = {
        "namespace": "ccma-pre",
        "application_yaml": "gfk_vgfk_csv.yaml",
    }

    kubernetesOperator = SparkKubernetesOperator(
    task_id='spark_submit',
    namespace=spark_config["namespace"],
    application_file=spark_config["application_yaml"],
    do_xcom_push=True,
    dag=dag,
    )

    kubernetesSensor = SparkKubernetesSensor(
    task_id='spark_monitor',
    namespace=spark_config["namespace"],
    application_name="{{ task_instance.xcom_pull(task_ids='spark_submit')['metadata']['name'] }}",
    dag=dag,
    attach_log=True,
    
    )

    trino_config = {
        "query_file_path": "gfk/gfk_repair_tables.hql",
        "query_bucket_name": "airflowdags"
    }
    query_file_name = trino_config['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
    task_id_1 = "execute_trino_query_1" + query_file_name.lower().replace('_', '')
    
    trino_query = read_data_from_s3(bucket_name=trino_config['query_bucket_name'], 
                            file_path=trino_config['query_file_path'])
    
    execute_trino_query = TrinoOperator(
        task_id=task_id_1,
        sql="CALL system.sync_partition_metadata('ccma_analytics', 'enterprise_gfk_pgfk_csv', 'ADD', true)",
        handler=list
    )
    task_id_2 = "execute_trino_query_2" + query_file_name.lower().replace('_', '')
    execute_trino_query = TrinoOperator(
        task_id=task_id_2,
        sql="CALL system.sync_partition_metadata('ccma_analytics', 'enterprise_gfk_vgfk_csv', 'ADD', true)",
        handler=list
    )

    kubernetesOperator >> [kubernetesSensor, execute_trino_query]
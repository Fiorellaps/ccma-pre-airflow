from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.python_operator import PythonOperator
import sys

sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

#from functions.filesystem_utiles import remove_folder

from datetime import datetime, timedelta


global_dag_config = {
    "job_name": "ETL-GFK-TMP",
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


with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:
    
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
    query_file_name = trino_config['query_file_path'].split('/')[-1].split('.hql')[0].lower()
    task_id_1 = "execute_trino_query_1" + query_file_name.lower().replace('_', '')
    
    
    execute_trino_query_1 = TrinoOperator(
        task_id=task_id_1,
        sql="CALL system.sync_partition_metadata('ccma_analytics', 'enterprise_gfk_pgfk_csv', 'ADD', true)",
        handler=list
    )
    task_id_2 = "execute_trino_query_2" + query_file_name.lower().replace('_', '')
    
    execute_trino_query_2 = TrinoOperator(
        task_id=task_id_2,
        sql="CALL system.sync_partition_metadata('ccma_analytics', 'enterprise_gfk_vgfk_csv', 'ADD', true)",
        handler=list
    )

    kubernetesOperator >> kubernetesSensor >> [execute_trino_query_1 , execute_trino_query_2 ]
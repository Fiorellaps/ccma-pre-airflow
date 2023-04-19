from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
#from airflow.models import Variable
#from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
#from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
#from airflow.providers.trino.operators.trino import TrinoOperator
#from airflow.operators.python_operator import PythonOperator

import sys

sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.task_group_execute_spark_application import execute_spark_application
from datetime import datetime, timedelta




global_dag_config = {
    "job_name": "ETL-GFK",
    "description":"Ingesta GFK",
    "owner":"ccma",
    "email_dest":["fpa@nextret.net"]
}
current_path = "dags"

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
    
    spark_config_gfk_pgfk_csv = {
        "use_case": "gfk_pgfk_csv",
        "namespace": "ccma-pre",
        "code_type": "Java", # Java, R or Python
        "application_s3_location": "s3a://airflowdags/gfk/ccma-etl-0.2314.0-SNAPSHOT-jar-with-dependencies.jar",
        "application_main_class": "com.pragsis.ccma.etl.control.ControlProcess",
    }

    spark_application_gfk_pgfk_csv = execute_spark_application(
                                        dag=dag, 
                                        config=spark_config_gfk_pgfk_csv, 
                                        current_path=current_path
                                        )
      
    spark_application_gfk_pgfk_csv
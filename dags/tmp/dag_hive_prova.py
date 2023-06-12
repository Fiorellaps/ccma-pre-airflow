from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook


import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.task_group_execute_spark_application import execute_spark_application
from functions.task_group_execute_trino_file import execute_trino_file
from datetime import datetime, timedelta

global_dag_config = {
    "job_name": "PROVA-HIVE-CCMA",
    "description":"Ingesta GFK",
    "owner":"ccma",
    "email_dest":[],
    "application_s3_location": "s3a://"+Variable.get("ccma_entorn")+"/enterprise/zapping/etl/ccma-etl-0.2314.4-jar-with-dependencies.jar",
    "application_main_class": "com.pragsis.ccma.etl.control.ControlProcess"
}
current_path = "dags"

dag_arguments =  {
    #"end_date": datetime(),
    #"depends_on_past": False,
    "email": global_dag_config['email_dest'],
    "email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    #"dagrun_timeout": timedelta(minutes=60)
    "max_active_runs": 1,
    "start_date": datetime(2022, 12, 1),
    "provide_context": True
}


def execute_hive_query():
    #hive_hook = HiveCliHook(hive_cli_conn_id='hive_cli_default')  # Connection ID for Hive
    hive_query = ["SELECT in_any_inici_bloc, in_any_fi_bloc_ss, st_dia_inici_bloc, st_dia_fi_bloc_ss FROM ccma_pcar.hbbtv_ip_aud_cons_settings_bloc_base_aux"]

    hive_hook = HiveServer2Hook(hive_cli_conn_id="hive_cli_default")
    results = hive_hook.get_records(hive_query)[0]
    #hive_hook.run_cli(hql=hive_query)
    #results = hive_hook.get_results()

    print("resultado de la query", type(results))
    print("resultado 1", results[0])
    return results



from functions.task_group_execute_hive_hql import process_hive_query

with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:


    hive_task = PythonOperator(
    task_id='hive_query_task',
    python_callable=execute_hive_query,
    dag=dag,
    )
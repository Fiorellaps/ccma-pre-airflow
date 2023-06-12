from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook


import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.utiles.s3_utiles import read_data_from_s3

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
    fields = ["in_any_inici_bloc", "in_any_fi_bloc_ss", "st_dia_inici_bloc", "st_dia_fi_bloc_ss"]
    fields_joined = ', '.join(fields)
    table_name = "ccma_pcar.hbbtv_ip_aud_cons_settings_bloc_base_aux"
    hive_query = [f'SELECT {fields_joined} FROM {table_name}']

    hive_hook = HiveServer2Hook(hive_cli_conn_id="hive_cli_default")
    results = hive_hook.get_records(hive_query)[0]
    
    in_any_inici_bloc = results[0]
    in_any_fi_bloc_ss = results[1]
    st_dia_inici_bloc = results[2]
    st_dia_fi_bloc_ss = results[3]

    parameters = ["in_any_inici_bloc", "in_any_fi_bloc_ss", "st_dia_inici_bloc", "st_dia_fi_bloc_ss"]
    file_path = "opt/pcar/hive/A3_settings_dades.hql"
    bucket_name =  "ccma-pre"
    query = read_data_from_s3(
                            bucket_name=bucket_name, 
                            file_path=file_path
          )
    for i in range(0, len(parameters)):
        value = str(results[i])
        field_variable = "${" + parameters[i] + "}"
        query = query.replace(field_variable, value)

    print("query", query)
    #hive_hook = HiveCliHook(hive_cli_conn_id="hive_cli_default")
    #hive_hook.run_cli(hql=query, hive_conf={"bi_id_setting": 2})
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
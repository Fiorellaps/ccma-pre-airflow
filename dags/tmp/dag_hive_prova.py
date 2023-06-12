from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


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
    "application_s3_location": "s3a://" + Variable.get("ccma_entorn") + "/enterprise/zapping/etl/ccma-etl-0.2314.4-jar-with-dependencies.jar",
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

from functions.task_group_execute_hive_hql import execute_hive_query_with_parameters

with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:

    config_consolidacio_A3_setting_dades = {
        "fields_selected": ["in_any_inici_bloc", "in_any_fi_bloc_ss", "st_dia_inici_bloc", "st_dia_fi_bloc_ss"],
        "table_name_select": "ccma_pcar.hbbtv_ip_aud_cons_settings_bloc_base_aux",
        "query_parameters": ["in_any_inici_bloc", "in_any_fi_bloc_ss", "st_dia_inici_bloc", "st_dia_fi_bloc_ss"],
        "query_file_path": "opt/pcar/hive/A3_settings_dades.hql",
        "query_bucket_name": Variable.get("ccma_entorn")
    }

    hive_task = PythonOperator(
    task_id='hive_query_with_parameters_task',
    python_callable=execute_hive_query_with_parameters,
    op_kwargs={
                  "fields_selected": config_consolidacio_A3_setting_dades["fields_selected"],
                  "table_name_select": config_consolidacio_A3_setting_dades["table_name_select"],
                  "query_parameters": config_consolidacio_A3_setting_dades["query_parameters"],
                  "query_file_path": config_consolidacio_A3_setting_dades["query_file_path"],
                  "query_bucket_name":config_consolidacio_A3_setting_dades["query_bucket_name"]
              },
    dag=dag,
    )
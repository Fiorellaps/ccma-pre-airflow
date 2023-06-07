from airflow.providers.apache.sqoop.operators.sqoop import SqoopOperator

from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow import DAG

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.utiles.filesystem_utiles import remove_file
from functions.task_group_execute_hive_hql import execute_hive_hql

import os

ENTORNO = Variable.get("ccma_entorn")


def to_do(param:str):
    return True
    

def pcar_ea_vistos(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_EA_VISTOS",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # consolidacio_B_01_create_video_log_aux
        hive_config_consolidacio_B_01_create_video_log_aux = {
            "query_file_path": "opt/pcar/B_01_create_video_log_aux.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_B_01_create_video_log_aux["query_name"] = hive_config_consolidacio_B_01_create_video_log_aux['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
        
        hive_execute_consolidacio_B_01_create_video_log_aux = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_B_01_create_video_log_aux, 
                                            )
        
        # sq_carrega_dades
        sq_carrega_dades_command = "import --connect jdbc:mysql://localhost/mydatabase --table mytable --username myuser --password mypassword"
        sq_carrega_dades = SqoopOperator(
            task_id='sqoop_import',
            dag=dag,
            conn_id='sqoop_default',  # Airflow connection ID for Sqoop
            cmd=sq_carrega_dades_command
        )

        # consolidacio_B_03_video_log
        hive_config_consolidacio_B_03_video_log = {
            "query_file_path": "opt/pcar/B_03_video_log.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_B_03_video_log["query_name"] = hive_config_consolidacio_B_03_video_log['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
        
        hive_execute_consolidacio_B_03_video_log = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_B_03_video_log, 
                                            )
        
        (
            hive_execute_consolidacio_B_01_create_video_log_aux >>
            sq_carrega_dades >>
            hive_execute_consolidacio_B_03_video_log

        )

    return taskgroup

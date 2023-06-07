from airflow.operators.python_operator import PythonOperator
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
    

def pcar_b_validacio(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_B_VALIDACIO",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        carrega_step_config = PythonOperator(
            task_id='carrega_step_config',
            python_callable=to_do,
            op_kwargs={
                "param": "",
            },
            dag=dag,
        )

        # consolidacio_B_query_warning
        hive_config_consolidacio_B_query_warning = {
            "query_file_path": "opt/pcar/query_warning.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_B_query_warning["query_name"] = hive_config_consolidacio_B_query_warning['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
        
        hive_execute_consolidacio_B_query_warning = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_B_query_warning, 
                                            )
        
        # consolidacio_B_final_validacio_step
        hive_config_consolidacio_B_final_validacio_step = {
            "query_file_path": "opt/pcar/final_validacio_step.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_B_final_validacio_step["query_name"] = hive_config_consolidacio_B_final_validacio_step['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
        
        hive_execute_consolidacio_B_final_validacio_step = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_B_final_validacio_step, 
                                            )

        (
            carrega_step_config
            >> hive_execute_consolidacio_B_query_warning
            >> hive_execute_consolidacio_B_final_validacio_step
        )

    return taskgroup

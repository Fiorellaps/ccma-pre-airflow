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
    

def pcar_e_consolidacio(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_E_CONSOLIDACIO",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # consolidacio_E1_audiencia_cons
        hive_config_consolidacio_E1_audiencia_cons = {
            "query_file_path": "opt/pcar/E1_audiencia_cons.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_E1_audiencia_cons["query_name"] = hive_config_consolidacio_E1_audiencia_cons['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_E1_audiencia_cons = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_E1_audiencia_cons, 
                                            )
        
        (
            hive_execute_consolidacio_E1_audiencia_cons
        )

    return taskgroup

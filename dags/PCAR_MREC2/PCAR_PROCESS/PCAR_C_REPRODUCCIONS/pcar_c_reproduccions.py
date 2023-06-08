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
    

def pcar_c_reproduccions(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_C_REPRODUCCIONS",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # consolidacio_C1_raw_bloc_valides_aux
        hive_config_consolidacio_C1_raw_bloc_valides_aux = {
            "query_file_path": "opt/pcar/C1_raw_bloc_valides_aux.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_C1_raw_bloc_valides_aux["query_name"] = hive_config_consolidacio_C1_raw_bloc_valides_aux['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_C1_raw_bloc_valides_aux = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_C1_raw_bloc_valides_aux, 
                                            )
        
        # consolidacio_C2_reproduccions
        hive_config_consolidacio_C2_reproduccions = {
            "query_file_path": "opt/pcar/C2_reproduccions.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_C2_reproduccions["query_name"] = hive_config_consolidacio_C2_reproduccions['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_C2_reproduccions = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_C2_reproduccions, 
                                            )
        
        # consolidacio_C3_raw_ant_cons_no_cons
        hive_config_consolidacio_C3_raw_ant_cons_no_cons = {
            "query_file_path": "opt/pcar/C3_raw_ant_cons_no_cons.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_C3_raw_ant_cons_no_cons["query_name"] = hive_config_consolidacio_C3_raw_ant_cons_no_cons['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_C3_raw_ant_cons_no_cons = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_C3_raw_ant_cons_no_cons, 
                                            )

        (
            hive_execute_consolidacio_C1_raw_bloc_valides_aux
            >> hive_execute_consolidacio_C2_reproduccions
            >> hive_execute_consolidacio_C3_raw_ant_cons_no_cons
        )

    return taskgroup

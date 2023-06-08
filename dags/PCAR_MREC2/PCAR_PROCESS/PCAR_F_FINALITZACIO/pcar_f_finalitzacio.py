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
    

def pcar_f_finalitzacio(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_F_FINALITZACIO",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # consolidacio_F1_index_inval_pends_cons
        hive_config_consolidacio_F1_index_inval_pends_cons = {
            "query_file_path": "opt/pcar/F1_index_inval_pends_cons.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_F1_index_inval_pends_cons["query_name"] = hive_config_consolidacio_F1_index_inval_pends_cons['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_F1_index_inval_pends_cons = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_F1_index_inval_pends_cons, 
                                            )
        
        # consolidacio_F2_bloc
        hive_config_consolidacio_F2_bloc = {
            "query_file_path": "opt/pcar/F2_bloc.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_F2_bloc["query_name"] = hive_config_consolidacio_F2_bloc['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_F2_bloc = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_F2_bloc, 
                                            )
        
        (
            hive_execute_consolidacio_F1_index_inval_pends_cons >>
            hive_execute_consolidacio_F2_bloc
        )

    return taskgroup

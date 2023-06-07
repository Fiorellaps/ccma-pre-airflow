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
    

def pcar_d_warnings(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_D_WARNINGS",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # consolidacio_D1_verif_aud_disp
        hive_config_consolidacio_D1_verif_aud_disp = {
            "query_file_path": "opt/pcar/D1_verif_aud_disp.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_D1_verif_aud_disp["query_name"] = hive_config_consolidacio_D1_verif_aud_disp['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
        
        hive_execute_consolidacio_D1_verif_aud_disp = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_D1_verif_aud_disp, 
                                            )
        
        # consolidacio_D2_verif_aud_disp_historic
        hive_config_consolidacio_D2_verif_aud_disp_historic = {
            "query_file_path": "opt/pcar/D2_verif_aud_disp_historic.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_D2_verif_aud_disp_historic["query_name"] = hive_config_consolidacio_D2_verif_aud_disp_historic['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
        
        hive_execute_consolidacio_D2_verif_aud_disp_historic = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_D2_verif_aud_disp_historic, 
                                            )

        (
            hive_execute_consolidacio_D1_verif_aud_disp
            >> hive_execute_consolidacio_D2_verif_aud_disp_historic
        )

    return taskgroup

#from airflow.providers.apache.sqoop.operators.sqoop import SqoopOperator
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow import DAG

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.task_group_execute_hive_hql import execute_hive_hql
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_EA_VISTOS.MDTV_VIDEO_CARREGA.mdtv_video_carrega import mdtv_video_carrega


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

        # mdtv_video_carrega
        task_mdtv_video_carrega = mdtv_video_carrega(dag)

        # consolidacio_EA1_vistos
        hive_config_consolidacio_EA1_vistos = {
            "query_file_path": "opt/pcar/EA1_vistos.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_EA1_vistos["query_name"] = hive_config_consolidacio_EA1_vistos['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_EA1_vistos = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_EA1_vistos, 
                                            )
        
        (
            task_mdtv_video_carrega >>
            hive_execute_consolidacio_EA1_vistos
        )

    return taskgroup

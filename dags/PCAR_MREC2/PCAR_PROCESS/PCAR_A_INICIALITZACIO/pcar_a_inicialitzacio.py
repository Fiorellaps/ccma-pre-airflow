from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook


import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

#from functions.utiles.filesystem_utiles import remove_file
from functions.task_group_execute_hive_hql import execute_hive_hql
from functions.task_group_execute_hive_hql import execute_hive_query_with_parameters

import os

ENTORNO = Variable.get("ccma_entorn")


def to_do(param:str):
    print("error")
    return sys.exit('Error')
    

def pcar_a_inicialitzacio(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "task_name": "PCAR_A_INICIALITZACIO",
    }
    

    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # consolidacio_A1_setup
        hive_config_consolidacio_A1_setup = {
            "query_file_path": "opt/pcar/hive/A1_setup.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_A1_setup["query_name"] = hive_config_consolidacio_A1_setup['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_A1_setup = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_A1_setup, 
                                            )
        
        # consolidacio_A2_setting
        hive_config_consolidacio_A2_setting_base = {
            "query_file_path": "opt/pcar/hive/A2_settings_base.hql",
            "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
        }
        hive_config_consolidacio_A2_setting_base["query_name"] = hive_config_consolidacio_A2_setting_base['query_file_path'].split('/')[-1].split('.hql')[0].lower()
        
        hive_execute_consolidacio_A2_setting_base = execute_hive_hql(
                                            dag=dag, 
                                            config=hive_config_consolidacio_A2_setting_base, 
                                            )
        
        # consolidacio_A3_setting_dades
        hive_config_consolidacio_A3_setting_dades = {
            "fields_selected": ["in_any_inici_bloc", "in_any_fi_bloc_ss", "st_dia_inici_bloc", "st_dia_fi_bloc_ss"],
            "table_name_select": "ccma_pcar.hbbtv_ip_aud_cons_settings_bloc_base_aux",
            "query_parameters": ["in_any_inici_bloc", "in_any_fi_bloc_ss", "st_dia_inici_bloc", "st_dia_fi_bloc_ss"],
            "query_file_path": "opt/pcar/hive/A3_settings_dades.hql",
            "query_bucket_name": ENTORNO
        }
        
        hive_execute_consolidacio_A3_setting_dades = PythonOperator(
                            task_id='hive_query_with_parameters_task',
                            python_callable=execute_hive_query_with_parameters,
                            op_kwargs={
                                        "fields_selected": hive_config_consolidacio_A3_setting_dades["fields_selected"],
                                        "table_name_select": hive_config_consolidacio_A3_setting_dades["table_name_select"],
                                        "query_parameters": hive_config_consolidacio_A3_setting_dades["query_parameters"],
                                        "query_file_path": hive_config_consolidacio_A3_setting_dades["query_file_path"],
                                        "query_bucket_name":hive_config_consolidacio_A3_setting_dades["query_bucket_name"]
                                    },
                            dag=dag,
                            )
        
        # consolidacio_A4_raw_bloc_aux
        hive_config_consolidacio_A4_raw_bloc_aux = {
            "fields_selected": ["in_any_inici_bloc", "in_any_fi_bloc", "st_dia_inici_bloc", "st_dia_fi_bloc"],
            "table_name_select": "ccma_pcar.hbbtv_ip_aud_cons_settings_bloc_dades_aux",
            "query_parameters": ["in_any_inici_bloc", "in_any_fi_bloc", "st_dia_inici_bloc", "st_dia_fi_bloc"],
            "query_file_path": "opt/pcar/hive/A4_raw_bloc_aux.hql",
            "query_bucket_name": ENTORNO
        }
        
        hive_execute_consolidacio_A4_raw_bloc_aux = PythonOperator(
                            task_id='hive_query_with_parameters_task',
                            python_callable=execute_hive_query_with_parameters,
                            op_kwargs={
                                        "fields_selected": hive_config_consolidacio_A4_raw_bloc_aux["fields_selected"],
                                        "table_name_select": hive_config_consolidacio_A4_raw_bloc_aux["table_name_select"],
                                        "query_parameters": hive_config_consolidacio_A4_raw_bloc_aux["query_parameters"],
                                        "query_file_path": hive_config_consolidacio_A4_raw_bloc_aux["query_file_path"],
                                        "query_bucket_name":hive_config_consolidacio_A4_raw_bloc_aux["query_bucket_name"]
                                    },
                            dag=dag,
                            )
        

        (
            hive_execute_consolidacio_A1_setup
            >> hive_execute_consolidacio_A2_setting_base
            >> hive_execute_consolidacio_A3_setting_dades
            >> hive_execute_consolidacio_A4_raw_bloc_aux
        )

    return taskgroup

from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.utiles.s3_utiles import read_data_from_s3


def process_hive_query(bucket_name:str, file_path:str):
        
        hive_hook = HiveCliHook(hive_cli_conn_id='hive_cli_default')

        query = read_data_from_s3(
                            bucket_name=bucket_name, 
                            file_path=file_path
        )

        hive_hook.run_cli(hql=query)
            


def execute_hive_hql(dag: DAG, config:dict):

    task_group_id =  "hql_" + config['query_name']

    execute_hive_query = PythonOperator(
                            task_id = task_group_id,
                            python_callable=process_hive_query,
                            op_kwargs={
                                "file_path": config["query_file_path"],
                                "bucket_name": config["query_bucket_name"]
                            },
                            dag=dag
                        )
    
    return execute_hive_query

def execute_hive_query_with_parameters(fields_selected:list, table_name_select:str, query_parameters:list, query_file_path:str, query_bucket_name:str):
    
    fields_joined = ', '.join(fields_selected)
    hive_query = [f'SELECT {fields_joined} FROM {table_name_select}']

    hive_hook = HiveServer2Hook(hive_cli_conn_id="hive_cli_default")
    results = hive_hook.get_records(hive_query)[0]

    query = read_data_from_s3(
                            bucket_name=query_bucket_name, 
                            file_path=query_file_path
          )
    
    for i in range(0, len(query_parameters)):
        value = str(results[i])
        field_variable = "${" + query_parameters[i] + "}"
        query = query.replace(field_variable, value)

    hive_hook = HiveCliHook(hive_cli_conn_id="hive_cli_default")
    hive_hook.run_cli(hql=query)

#from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.task_group import TaskGroup
from airflow import DAG
#from airflow.models import Variable
from airflow.providers.apache.hive.hooks.hive import HiveCliHook

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')
#from functions.task_execute_trino_query import execute_trino_query
from functions.utiles.s3_utiles import read_data_from_s3
#import random
from trino.dbapi import trino


def process_hive_query(bucket_name, file_path):
        
        hive_hook = HiveCliHook(hive_cli_conn_id='hive_cli_default')

        query = read_data_from_s3(
                            bucket_name=bucket_name, 
                            file_path=file_path
        )

        hive_hook.run_cli(hql=query)
            


def execute_hive_hql(dag: DAG, config):

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

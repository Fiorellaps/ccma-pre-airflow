from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')
#from functions.task_execute_trino_query import execute_trino_query
from functions.utiles.s3_utiles import read_data_from_s3
import random


def process_trino_query(dag, taskgroup, query):
        query_list = query.split(";")
        if len(query_list) > 0:
            for i in range(0, len(query_list)):
                query = query_list[i]
                if query:
                    print("--query", query)
                    task_id = "execute_trino_query_" + str(i)
                    task = TrinoOperator(
                                            task_id=task_id,
                                            sql=query,
                                            handler=list,
                                            dag=dag
                                        )
                    taskgroup.add(task)


def execute_trino_file(dag: DAG, config):

    task_group_id =  "execute_" + config['query_name']
    with TaskGroup(
        task_group_id, tooltip="Tarea para ejecutar las queries de un fichero sql"
    ) as taskgroup:
        
        query = read_data_from_s3(
                            bucket_name=config['query_bucket_name'], 
                            file_path=config['query_file_path']
                            ).replace(';', '')
        task_id = "execute_trino_query"
        task = TrinoOperator(
                            task_id=task_id,
                            sql=query,
                            handler=list,
                            dag=dag
                        )

        task
       

    return taskgroup

#from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.models import Variable

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')
#from functions.task_execute_trino_query import execute_trino_query
from functions.utiles.s3_utiles import read_data_from_s3
#import random
from trino.dbapi import trino


def process_trino_query(bucket_name, file_path):
        trino_host = Variable.get("trino_host")
        trino_port = Variable.get("trino_port")

        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            catalog="hive",
            user='admin')
        cur = conn.cursor()
        query = read_data_from_s3(
                            bucket_name=bucket_name, 
                            file_path=file_path
        )
        query_list = query.split(";")
        for query in query_list:
            if len(query) > 5:
                try:
                    cur.execute(query)
                    rows = cur.fetchall()
                    print("---------- ok -------------", rows)
                except trino.dbapi.Error as e:
                    print(f'Error ejecutando Trino query: {e}')
                 
            


def execute_trino_file(dag: DAG, config):

    task_group_id =  "execute_" + config['query_name']
    with TaskGroup(
        task_group_id, tooltip="Tarea para ejecutar las queries de un fichero sql"
    ) as taskgroup:
        
        '''query = read_data_from_s3(
                            bucket_name=config['query_bucket_name'], 
                            file_path=config['query_file_path']
                            )
        '''
        ##.replace(';', '')
        task_id = "execute_trino_query"
        execute_trino_query = PythonOperator(
                            python_callable=process_trino_query,
                            op_kwargs={
                                "file_path": config["query_file_path"],
                                "bucket_name": config["query_bucket_name"]
                            },
                            dag=dag
                        )

        execute_trino_query
       

    return taskgroup

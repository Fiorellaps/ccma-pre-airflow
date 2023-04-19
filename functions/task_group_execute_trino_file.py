from airflow.providers.trino.operators.trino import TrinoOperator
from airflow import DAG
import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')
from functions.task_execute_trino_query import execute_trino_query



def execute_trino_query(dag: DAG, table_name, query):
    '''query_file_name = config['file_name'].split('.hql')[0]
    task_id = "execute_trino_query_" + query_file_name.lower().replace()
    
    trino_query = read_data_from_s3(bucket_name=config['bucket_name'], 
                            file_path=config['file_path'], 
                            file_name=config['file_name'])
    '''
    task_id = "execute_trino_query_" + table_name.lower().replace("_", "").replace(" ", "")
    execute_trino_query = TrinoOperator(
        task_id=task_id,
        sql=query,
        handler=list,
        dag=dag
    )

    return execute_trino_query

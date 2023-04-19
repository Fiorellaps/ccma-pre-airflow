from airflow.providers.trino.operators.trino import TrinoOperator
from airflow import DAG
import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/funcions')
from funcions.utilities.s3_utiles import read_data_from_s3


def execute_trino_query(dag: DAG, config):
    query_file_name = config['file_name'].split('.hql')[0]
    task_id = "execute_trino_query_" + query_file_name.lower().replace()
    
    trino_query = read_data_from_s3(bucket_name=config['bucket_name'], 
                            file_path=config['file_path'], 
                            file_name=config['file_name'])
    
    execute_trino_query = TrinoOperator(
        task_id=task_id,
        sql=trino_query,
        handler=list
    )

    return execute_trino_query
sys.modules[__name__] = execute_trino_query

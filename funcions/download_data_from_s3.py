from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from funcions.utilities.s3_utiles import download_from_s3


def download_data_from_s3(dag: DAG, config) -> TaskGroup:
    
    download_data_from_s3 = PythonOperator(
            task_id='download_data_from_s3',
            python_callable=download_from_s3,
            op_kwargs={
                "file_path": config["file_path"],
                "bucket_name": config["bucket_name"],
                "file_name": config["file_name"]
            },
            dag=dag,
        )
    
    return download_data_from_s3

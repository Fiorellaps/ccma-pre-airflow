from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from funcions.utilities.filesystem_utiles import remove_folder
import os
from airflow.models import Variable

def remove_local_folder(dag: DAG, file_path:str):
    local_temporal_path = Variable.get("local_temporal_path")
    print("local_temporal_path", local_temporal_path)
    path_to_remove = os.path.join(local_temporal_path, file_path)
    print("path_to_remove", path_to_remove)

    remove_tmp = PythonOperator(
            task_id='remove_temporal_dir',
            python_callable=remove_folder,
            trigger_rule="all_done",
            op_kwargs={'path': path_to_remove },
            dag=dag,
    )
    return remove_tmp

from datetime import datetime, timedelta
from airflow import DAG
from funcions.download_data_from_s3 import download_data_from_s3
from funcions.remove_local_folder import remove_local_folder
from funcions.execute_trino_query import execute_trino_query
import sys
sys.path.insert(0,'./')
sys.path.insert(0,'/dags/dags')
print(sys.path)

global_dag_config = {
    "job_name": "ETL-GFK",
    "description":"Ingesta GFK",
    "owner":"ccma",
    "email_dest":["fpa@nextret.net"]
}
current_path = "casos_uso/territori/"

dag_arguments =  {
    #"end_date": datetime(),
    #"depends_on_past": False,
    #"email": ['fpa@nextret.net'],
    #"email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    #"dagrun_timeout":timedelta(minutes=60)
    "max_active_runs": 1,
    "start_date": datetime(2022, 12, 1),
    "provide_context": True
}


with DAG(
   global_dag_config["job_name"],
   default_args=dag_arguments,
   description=global_dag_config["description"],
   catchup=False,
   schedule_interval='@weekly', #timedelta(days=1)
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:

    # Download jar from s3
    config_download_jar_from_s3 = {
        "file_path": "gfk",
        "bucket_name": "airflowdags",
        "file_name": "ccma-etl-0.2311.0-SNAPSHOT-jar-with-dependencies.jar"
    }
    download_jar_from_s3_task = download_data_from_s3(dag=dag, config=config_download_jar_from_s3)
    
    
    # Remove temporal file
    remove_local_folder_task = remove_local_folder(dag=dag, file_path=config_download_jar_from_s3["file_path"])

    # Execute gfk repair tables
    config_gfk_repair_tables = {
        "file_path": "gfk",
        "bucket_name": "airflowdags",
        "file_name": "gfk_repair_tables.hql"
    }
    execute_gfk_repair_tables = execute_trino_query(dag=dag, config=config_gfk_repair_tables)

    # Execute insert incremental gfk
    config_insert_incremental_gfk = {
        "file_path": "gfk",
        "bucket_name": "airflowdags",
        "file_name": "insert_incremental_gfk.hql"
    }
    execute_insert_incremental_gfk = execute_trino_query(dag=dag, config=config_insert_incremental_gfk)

    download_jar_from_s3_task >>  config_gfk_repair_tables >> config_insert_incremental_gfk >> remove_local_folder_task

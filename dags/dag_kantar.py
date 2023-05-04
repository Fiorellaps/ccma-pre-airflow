#v1.6
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

import os
import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.task_group_execute_spark_application import execute_spark_application
from functions.task_group_execute_trino_file import execute_trino_file
from datetime import datetime, timedelta

ENTORNO = Variable.get("ccma_entorn")
global_dag_config = {
    "job_name": "ETL-Kantar",
    "description":"Ingesta Kantar",
    "owner":"ccma",
    "email_dest":["fpa@nextret.net", "cduran.b@ccma.cat"],
    "application_s3_location": "s3a://" + ENTORNO + "/enterprise/zapping/etl/ccma-etl-0.2314.4-jar-with-dependencies.jar",
    "application_main_class": "com.pragsis.ccma.etl.control.ControlProcess"
}
current_path = "dags"

dag_arguments =  {
    #"end_date": datetime(),
    #"depends_on_past": False,
    "email": global_dag_config['email_dest'],
    "email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    #"dagrun_timeout": timedelta(minutes=60)
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
    
    # Execute jar for kantar_iaad
    spark_config_kantar_iaad = {
        "use_case": "kantar_iaad",
        "namespace": ENTORNO, # ccma-pre | ccma-pro
        "code_type": "Java", # Java, R or Python
        "application_s3_location": global_dag_config['application_s3_location'],
        "application_main_class": global_dag_config['application_main_class'],
        "application_arguments": ["kantar_iaad"]
    }

    spark_application_kantar_iaad = execute_spark_application(
                                        dag=dag, 
                                        config=spark_config_kantar_iaad, 
                                        current_path=current_path
                                        )

    # Execute jar for kantar_iapd
    spark_config_kantar_iapd = {
        "use_case": "kantar_iapd",
        "namespace": ENTORNO, # ccma-pre | ccma-pro
        "code_type": "Java", # Java, R or Python
        "application_s3_location": global_dag_config['application_s3_location'],
        "application_main_class": global_dag_config['application_main_class'],
        "application_arguments": ["kantar_iapd"]
    }
    spark_application_kantar_iapd = execute_spark_application(
                                        dag=dag, 
                                        config=spark_config_kantar_iapd, 
                                        current_path=current_path
                                        )
    
    # Execute jar for kantar_iasd
    spark_config_kantar_iasd = {
        "use_case": "kantar_iasd",
        "namespace": ENTORNO, # ccma-pre | ccma-pro
        "code_type": "Java", # Java, R or Python
        "application_s3_location": global_dag_config['application_s3_location'],
        "application_main_class": global_dag_config['application_main_class'],
        "application_arguments": ["kantar_iasd"]
    }
    spark_application_kantar_iasd = execute_spark_application(
                                        dag=dag, 
                                        config=spark_config_kantar_iasd, 
                                        current_path=current_path
                                        )

    # Execute jar for kantar_ma
    spark_config_kantar_ma = {
        "use_case": "kantar_ma",
        "namespace": ENTORNO, # ccma-pre | ccma-pro
        "code_type": "Java", # Java, R or Python
        "application_s3_location": global_dag_config['application_s3_location'],
        "application_main_class": global_dag_config['application_main_class'],
        "application_arguments": ["kantar_ma"]
    }
    spark_application_kantar_ma = execute_spark_application(
                                        dag=dag, 
                                        config=spark_config_kantar_ma, 
                                        current_path=current_path
                                        )

    # Execute jar for kantar_mp
    spark_config_kantar_mp = {
        "use_case": "kantar_mp",
        "namespace": ENTORNO, # ccma-pre | ccma-pro
        "code_type": "Java", # Java, R or Python
        "application_s3_location": global_dag_config['application_s3_location'],
        "application_main_class": global_dag_config['application_main_class'],
        "application_arguments": ["kantar_mp"]
    }
    spark_application_kantar_mp = execute_spark_application(
                                        dag=dag, 
                                        config=spark_config_kantar_mp, 
                                        current_path=current_path
                                        )

    # Repair kantar tables
    trino_config_kantar_repair_tables = {
        "query_file_path": "enterprise/zapping/queries/kantar_repair_tables.hql",
        "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
    }
    trino_config_kantar_repair_tables["query_name"] = trino_config_kantar_repair_tables['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').lower()
    
    trino_execute_kantar_repair_tables = execute_trino_file(
                                        dag=dag, 
                                        config=trino_config_kantar_repair_tables, 
                                        )

    # Sembla que la limitació d'un sol statement per file s'ha corregit aquí: https://github.com/apache/airflow/issues/27610
    
    #  Insert incremental graella kantar
    trino_config_incremental_graella_kantar = {
        "query_file_path": "enterprise/zapping/queries/insert_incremental_graella_kantar.hql",
        "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
    }
    trino_config_incremental_graella_kantar["query_name"] = trino_config_incremental_graella_kantar['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').replace(' ', '').lower()
    
    trino_execute_incremental_graella_kantar = execute_trino_file(
                                        dag=dag, 
                                        config=trino_config_incremental_graella_kantar, 
                                        )

    #  Insert incremental kantar sortides
    trino_config_incremental_kantar_sortides = {
        "query_file_path": "enterprise/zapping/queries/insert_incremental_kantar_sortides.hql",
        "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
    }
    trino_config_incremental_kantar_sortides["query_name"] = trino_config_incremental_kantar_sortides['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').replace(' ', '').lower()
    
    trino_execute_incremental_kantar_sortides = execute_trino_file(
                                        dag=dag, 
                                        config=trino_config_incremental_kantar_sortides, 
                                        )

    #  Insert incremental kantar rebots
    trino_config_incremental_kantar_rebots = {
        "query_file_path": "enterprise/zapping/queries/insert_incremental_kantar_rebots.hql",
        "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
    }
    trino_config_incremental_kantar_rebots["query_name"] = trino_config_incremental_kantar_rebots['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').replace(' ', '').lower()
    
    trino_execute_incremental_kantar_rebots = execute_trino_file(
                                        dag=dag, 
                                        config=trino_config_incremental_kantar_rebots, 
                                        )

    #  Insert incremental kantar abandonament
    trino_config_incremental_kantar_abandonament = {
        "query_file_path": "enterprise/zapping/queries/insert_incremental_kantar_abandonament.hql",
        "query_bucket_name": ENTORNO # ccma-pre | ccma-pro
    }
    trino_config_incremental_kantar_abandonament["query_name"] = trino_config_incremental_kantar_abandonament['query_file_path'].split('/')[-1].split('.hql')[0].replace('_', '').replace(' ', '').lower()
    
    trino_execute_incremental_kantar_abandonament = execute_trino_file(
                                        dag=dag, 
                                        config=trino_config_incremental_kantar_abandonament, 
                                        )

    # Send success email 
    success_email = EmailOperator(
        task_id='send_email',
        to=global_dag_config['email_dest'],
        subject=ENTORNO+' - DAG Kantar Success',
        html_content="""<h3>DAG Kantar</h3> <p>El dag """ + global_dag_config["job_name"] +  """se ha ejecutado correctamente</p> """,
        dag=dag
)
    
    
spark_application_kantar_iaad >> spark_application_kantar_iapd >> [spark_application_kantar_iasd, spark_application_kantar_ma, spark_application_kantar_mp] >> trino_execute_kantar_repair_tables >> trino_execute_incremental_graella_kantar >> trino_execute_incremental_kantar_sortides >> trino_execute_incremental_kantar_rebots >> trino_execute_incremental_kantar_abandonament >> success_email
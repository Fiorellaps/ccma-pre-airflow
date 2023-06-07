from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

import os
import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from datetime import datetime, timedelta

ENTORNO = Variable.get("ccma_entorn")
global_dag_config = {
    "job_name": "MREC2_BATCH",
    "description":"Ingesta consolidació",
    "owner":"ccma",
    "email_dest":["fpa@nextret.net", "jmarco.q@ccma.cat", "fbigorra.s@ccma.cat"],
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
    "retries": 3,
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
   schedule_interval=None,
   tags=[global_dag_config["job_name"], global_dag_config["owner"], "s3", "jar"]
) as dag:
    
    # Send success email 
    success_email = EmailOperator(
        task_id='send_email',
        to=global_dag_config['email_dest'],
        subject=ENTORNO + ' - DAG PCAR_PROCESS Success',
        html_content="""<h3>DAG MREC2_BATCH</h3> <p>El dag """ + global_dag_config["job_name"] +  """se ha ejecutado correctamente</p> """,
        dag=dag
    )
    
    
    (
    success_email
    )
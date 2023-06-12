from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

import os
import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_A_INICIALITZACIO.pcar_a_inicialitzacio import pcar_a_inicialitzacio
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_B_VALIDACIO.pcar_b_validacio  import pcar_b_validacio
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_C_REPRODUCCIONS.pcar_c_reproduccions import pcar_c_reproduccions
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_D_WARNINGS.pcar_d_warnings import pcar_d_warnings
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_E_CONSOLIDACIO.pcar_e_consolidacio import pcar_e_consolidacio
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_EA_VISTOS.pcar_ea_vistos import pcar_ea_vistos
from dags.PCAR_MREC2.PCAR_PROCESS.PCAR_F_FINALITZACIO.pcar_f_finalitzacio import pcar_f_finalitzacio

from datetime import datetime, timedelta

ENTORNO = Variable.get("ccma_entorn")
global_dag_config = {
    "job_name": "PCAR_PROCESS",
    "description":"Ingesta consolidació",
    "owner":"ccma",
    "email_dest":["fpa@nextret.net"],
    #"email_dest":["fpa@nextret.net", "jmarco.q@ccma.cat", "fbigorra.s@ccma.cat"],

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
    #"retries": 3,
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
    
    # pcar_a_inicialitzacio
    config_pcar_a_inicialitzacio = {
        "parameter": "",
    }
    task_pcar_a_inicialitzacio = pcar_a_inicialitzacio(
                                        dag=dag, 
                                        config=config_pcar_a_inicialitzacio, 
                                        current_path=current_path
                                    )
    

    # Send success email 
    success_email = EmailOperator(
        task_id='send_email',
        to=global_dag_config['email_dest'],
        subject=ENTORNO + ' - DAG PCAR_PROCESS Success',
        html_content="""<h3>DAG PCAR_PROCESS</h3> <p>El dag """ + global_dag_config["job_name"] +  """se ha ejecutado correctamente</p> """,
        dag=dag
    )
    
    
    (task_pcar_a_inicialitzacio >>
    success_email
    )
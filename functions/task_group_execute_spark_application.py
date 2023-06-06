
#  DAG v1.0
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow import DAG

import sys
sys.path.insert(0, '/opt/bitnami/airflow/dags/git_dags/')
sys.path.insert(0,'/opt/bitnami/airflow/dags/git_dags/functions')

from functions.utiles.filesystem_utiles import remove_file
from functions.utiles.create_yaml import create_yaml



import os

def execute_spark_application(dag: DAG, config, current_path="") -> TaskGroup:

    # Import Variables
    #temporal_path = os.path.join(Variable.get("dags_temporal_path"), config["use_case"])
    base_path = Variable.get("dags_base_path")
    yaml_template_path = Variable.get("yaml_template_path")

    # Define Job metadata
    job_config = {
        "job_name": "app",
    }
    
    job_config['task_name'] = job_config["job_name"] + "_" + config['use_case'] 

    
    '''
    spark_config = {
        "application_yaml": job_config['task_name'] + ".yaml",
        "aws_access_key_id": config["aws_access_key_id"],
        "aws_secret_access_key": config["aws_secret_access_key"],
        "temporal_path": temporal_path,
        #"temporal_path_to_remove": temporal_path,
        "bucket_name": config["bucket_name"],
        "namespace": "spark-operator",
        "main_class": config["application_main_class"],
        "main_application_file":  config["application_s3_location"]
        #"spark_temporal_path": "/opt/spark/files/airflowdags/tmp/" + job_config["system"] + "/" + job_config["entity"] + "/" + job_config["job_name"]
    }
    '''


    with TaskGroup(
        job_config['task_name'], tooltip="Tarea para " + job_config["task_name"] 
    ) as taskgroup:

        # Define parameters for the spark application
        spark_config = {
            "template_path": yaml_template_path,
            "yaml_dest_path_global": os.path.join(base_path, current_path, job_config['task_name'] + ".yaml"),
            "yaml_dest_path_relative": job_config['task_name'] + ".yaml",
            "application_name": job_config["task_name"].replace("_", "").replace(" ", "").lower(),
            "code_type": config["code_type"],
            "image": "fps99/sparkr-s3:v6.8",
            "main_application_file": config["application_s3_location"],
            "arguments": config["application_arguments"],
            "main_class": config["application_main_class"],
            "namespace": config["namespace"]
        }

        # Assign memory and cores parameters if given or give defautl values instead
        if 'spark_driver_cores' in config.keys():
            spark_config['driver_cores'] = config["spark_driver_cores"]
        else:
            spark_config['driver_cores'] = 2
        
        if 'spark_driver_memory' in config.keys():
            spark_config['driver_memory'] = config["spark_driver_memory"]
        else:
            spark_config['driver_memory'] = "6144m"

        if 'spark_executor_cores' in config.keys():
            spark_config['executor_cores'] = config["spark_executor_cores"]
        else:
            spark_config['executor_cores'] = 1

        if 'spark_executor_memory' in config.keys():
            spark_config['executor_memory'] = config["spark_executor_memory"]
        else:
            spark_config['executor_memory'] = "2049m"
        
        if 'spark_executor_instances' in config.keys():
            spark_config['executor_instances'] = config["spark_executor_instances"]
        else:
            spark_config['executor_instances'] = 1

        create_yaml_spark = PythonOperator(
            task_id='create_yaml_spark',
            python_callable=create_yaml,
            op_kwargs={
                "template_path": spark_config["template_path"],
                "dest_path": spark_config["yaml_dest_path_global"],
                "application_name": spark_config["application_name"],
                "code_type": spark_config["code_type"],
                "image": spark_config["image"],
                "main_application_file": spark_config["main_application_file"],
                "main_class": spark_config["main_class"],
                "arguments": spark_config["arguments"]
            },
            dag=dag,
        )
        

        kubernetesOperator = SparkKubernetesOperator(
            task_id='spark_submit',
            namespace=spark_config["namespace"],
            application_file=spark_config["yaml_dest_path_relative"],
            do_xcom_push=True,
            dag=dag,
        )


        kubernetesSensor = SparkKubernetesSensor(
            task_id='spark_monitor',
            namespace=spark_config["namespace"],
            application_name= spark_config["application_name"],
            dag=dag,
            attach_log=True,
        )

        
        remove_yaml = PythonOperator(
            task_id='remove_yaml',
            python_callable=remove_file,
            trigger_rule="all_done",
            op_kwargs={'path': spark_config["yaml_dest_path_global"]},
            dag=dag,
        )

        (
            create_yaml_spark
            >> kubernetesOperator
            >> [kubernetesSensor, remove_yaml]
        )

    return taskgroup

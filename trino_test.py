from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from trino.dbapi import trino


def query_trino(**kwargs):
    conn = trino.dbapi.connect(
            host='trino.apps.k8spro.nextret.net',
            port=31858,
            catalog="hive",
            user='admin')
    query = "SHOW catalogs"    
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    print(rows)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'query_trino',
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily',
    tags=['trino', 'test']
) as dag:
  
  trino_query = TrinoOperator(
        task_id="trino_templated_query",
        sql="SHOW CATALOGS",
        handler=list
  )
  #CALL system.sync_partition_metadata('ccma_matr', 'hbbtv_ip_aud_raw', 'ADD', true);

  '''t1 = PythonOperator(
        task_id='run_query',
        python_callable=query_trino,
  )'''

trino_query

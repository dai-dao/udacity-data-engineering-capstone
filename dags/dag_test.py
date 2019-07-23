import lib.emr_lib as emr
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import ClusterCheckSensor, ETLDAGCheckCompleteSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook


dag = DAG('dag_test', description='Test DAG',
          schedule_interval=None,
          start_date=datetime(2016, 1, 1))


def verify_cluster(**kwargs):
    logging.info("YAY GOOD")


def query_metadata(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    records = pg_hook.get_records("""
        select count(1) from task_instance 
            where state IS NOT NULL
            AND state NOT IN ('scheduled','queued')
    """)
    logging.info(records[0][0])


# op_verify_cluster = PythonOperator(
#     task_id='verify_cluster',
#     python_callable = verify_cluster,
#     dag=dag
# )
# sensor_task = ClusterCheckSensor(task_id='cluster_check', poke_interval=60, dag=dag)
# sensor_task = ETLDAGCheckCompleteSensor(task_id='dag_check_complete', poke_interval=10, dag=dag)
# sensor_task >> op_verify_cluster

query = PythonOperator(
    task_id='query_metadata',
    python_callable=query_metadata,
    dag=dag
)
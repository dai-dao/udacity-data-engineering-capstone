import lib.emr_lib as emr
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import ETLDAGCheckCompleteSensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

# Initialize the DAG
dag = DAG('dag_cluster', concurrency=1, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='cluster', num_core_nodes=2)
    Variable.set("cluster_id", cluster_id)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)
    #
    Variable.set("cluster_id", "na")
    Variable.set("dag_analytics_state", "na")
    Variable.set("dag_normalize_state", "na")

#
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

etl_dag_check_complete_task = ETLDAGCheckCompleteSensor(
    task_id='etl_dag_check_complete', 
    poke_interval=120, 
    dag=dag)

#
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> etl_dag_check_complete_task 
etl_dag_check_complete_task >> terminate_cluster
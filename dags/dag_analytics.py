import lib.emr_lib as emr
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import ClusterCheckSensor, NormalizeDAGCheckCompleteSensor
from airflow import AirflowException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1, 0, 0, 0 ,0),
    'end_date' : datetime(2016, 12, 1, 0, 0, 0 ,0),
    'retries': 1,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True,
}

# Initialize the DAG
dag = DAG('dag_analytics4', concurrency=2, schedule_interval="@monthly", 
            default_args=default_args, max_active_runs=1)
region = emr.get_region()
emr.client(region_name=region)


# 
def submit_to_emr(**kwargs):
    cluster_id = Variable.get("cluster_id")
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    # Get execution date format
    execution_date = kwargs["execution_date"]
    month = execution_date.strftime("%b").lower()
    year = execution_date.strftime("%y")
    logging.info(month+year)
    statement_response = emr.submit_statement(session_url, 
                                kwargs['params']['file'], "month_year = '{}'\n".format(month+year))
    logs = emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)
    if kwargs['params']['log']:
        for line in logs:
            if 'FAIL' in line:
                logging.info(line)
                raise AirflowException("Analytics data Quality check Fail!")

#
def dag_done(**kwargs):
    execution_date = kwargs["execution_date"]
    if execution_date == kwargs["end_date"]:
        Variable.set("dag_analytics_state", "done")

#
transform_immigration = PythonOperator(
    task_id='transform_immigration',
    python_callable = submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/immigration.py', "log" : False},
    dag=dag
)

transform_immigration_demographics = PythonOperator(
    task_id='transform_immigration_demographics',
    python_callable = submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/immigration_demographics.py', "log" : False},
    dag=dag
)

cluster_check_task = ClusterCheckSensor(
    task_id='cluster_check', 
    poke_interval=60, 
    dag=dag)

quality_check = PythonOperator(
    task_id = 'analytics_quality_check',
    python_callable = submit_to_emr,
    params = {"file" : '/root/airflow/dags/transform/analytics_quality_check.py', "log" : True},
    dag=dag
)

normalize_dag_check_task = NormalizeDAGCheckCompleteSensor(
    task_id='normalize_dag_check_complete', 
    poke_interval=60, 
    dag=dag)

done = PythonOperator(
    task_id = "done",
    python_callable=dag_done,
    dag=dag
)

cluster_check_task >> normalize_dag_check_task 
normalize_dag_check_task >> transform_immigration
transform_immigration >> transform_immigration_demographics 
transform_immigration_demographics >> quality_check >> done
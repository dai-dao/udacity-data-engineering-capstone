import lib.emr_lib as emr
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import ClusterCheckSensor
from airflow.operators import ClusterCheckSensor
from airflow import AirflowException



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
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('dag_normalize', concurrency=2, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# 
def submit_to_emr(**kwargs):
    cluster_id = Variable.get("cluster_id")
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url, kwargs['params']['file'])
    logs = emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)
    if kwargs['params']['log']:
        for line in logs:
            logging.info(line)
            if 'FAIL' in str(line):
                logging.info(line)
                raise AirflowException("Normalize data Quality check Fail!")

#
def dag_done(**kwargs):
    Variable.set("dag_normalize_state", "done")


transform_weather = PythonOperator(
    task_id='transform_weather',
    python_callable = submit_to_emr,
    params={
        "file" : '/root/airflow/dags/transform/weather.py',
        "log" : False
    },
    dag=dag
)

transform_airport_weather = PythonOperator(
    task_id='transform_airport_weather',
    python_callable = submit_to_emr,
    params={
        "file" : '/root/airflow/dags/transform/airport_weather.py',
        "log" : False
    },
    dag=dag
)

transform_codes = PythonOperator(
    task_id='transform_codes',
    python_callable=submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/codes.py', "log" : False},
    dag=dag)

transform_city = PythonOperator(
    task_id='transform_city',
    python_callable=submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/city.py', "log" : False},
    dag=dag)


transform_demographics = PythonOperator(
    task_id='transform_demographics',
    python_callable = submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/demographics.py', "log" : False},
    dag=dag
)

quality_check = PythonOperator(
    task_id = 'normalize_quality_check',
    python_callable = submit_to_emr,
    params = {"file" : '/root/airflow/dags/transform/normalize_quality_check.py', "log" : True},
    dag=dag
)

sensor_task = ClusterCheckSensor(
    task_id='cluster_check', 
    poke_interval=60, 
    dag=dag)

done = PythonOperator(
    task_id = "done",
    python_callable=dag_done,
    dag=dag
)

#
sensor_task >> transform_city >> transform_codes
#
transform_codes >> transform_demographics >> transform_airport_weather
transform_codes >> transform_weather >> transform_airport_weather 
transform_airport_weather >> quality_check >> done
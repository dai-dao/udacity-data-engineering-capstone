import logging 

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from airflow.models import Variable
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance
import boto3, json, pprint, requests, textwrap, time, requests
from datetime import datetime

def get_region():
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    return response_json.get('region')

def client(region_name):
    return boto3.client('emr', region_name=region_name)

def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

region = get_region()
emr = client(region)


# Check if all ETL are finished, not necessarily successful
class ETLDAGCheckCompleteSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(ETLDAGCheckCompleteSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        dag_normalize_state = Variable.get("dag_normalize_state")
        dag_analytics_state = Variable.get("dag_analytics_state")
        if dag_normalize_state != 'na' and dag_analytics_state != 'na':
            return True
        else:
            return False


# Check if normalized tables are successfully created
class NormalizeDAGCheckCompleteSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(NormalizeDAGCheckCompleteSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        dag_normalize_state = Variable.get("dag_normalize_state")
        if dag_normalize_state == 'done':
            return True
        else:
            return False


# Check if cluster is up and available
class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ti = context['ti']
        try:
            cluster_id = Variable.get("cluster_id")
            status = get_cluster_status(emr, cluster_id)
            logging.info(status)
            if status == 'WAITING':
                return True
            else:
                return False
        except Exception as e:
            logging.info(e)
            return False



class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [ClusterCheckSensor, ETLDAGCheckCompleteSensor, NormalizeDAGCheckCompleteSensor]
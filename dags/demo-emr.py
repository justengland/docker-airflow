"""
Sample Enterprise Big Data integration With Airflow
"""
import os
from os import walk
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.config import Config as botoConfig
import getpass
from libraries.big_data_operator import BigDataOperator
from libraries.big_data_sensor import BigDataSensor

def get_boto_client(client):
    boto_config = botoConfig(
        retries=dict(
            max_attempts=50
        )
    )

    return boto3.client(client, config=boto_config)

CONFIG_BASE_DIR = 'test/calculate/pi'
#DATA_SOURCE_NAME = 'test_calculate_pi'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["jengland@sjrb.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("demo-emr",
         max_active_runs=3,
         start_date=datetime(2020, 2, 20, 13, 0),
         schedule_interval='@once',
         default_args=default_args)


def setup(**context):
    filenames = next(walk('./plugins'), (None, None, []))[2]
    print(f'filenames: {filenames}')



def log_aws_identity():
    print('Get System')
    print(getpass.getuser())

    print('Get AWS IDENTITY')
    client = get_boto_client('sts')
    response = client.get_caller_identity()
    print(response)


# # t1, t2 and t3 are examples of tasks created by instantiating operators
# aws_identity = PythonOperator(
#     task_id='get_caller_identity',
#     python_callable=log_aws_identity,
#     dag=dag,
# )
#
# setup = PythonOperator(
#     task_id='setup',
#     python_callable=setup,
#     provide_context=True,
#     dag=dag,
# )

waitForEmrToComplete = BigDataSensor(
    task_id='waitForEmrToComplete',
    data_source_name='test_calculate_pi',
    s3_url="{{ ti.xcom_pull(task_ids='launchCalculatePI', key='s3_url') }}",
    provide_context=True,
    dag=dag
)

launchCalculatePI = BigDataOperator(
    task_id='launchCalculatePI',
    config_base_directory=CONFIG_BASE_DIR,
    provide_context=True,
    dag=dag
)

launchCalculatePI >> waitForEmrToComplete



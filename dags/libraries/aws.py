'''
A set of helper functions to facilitate calling aws api's
'''
import boto3
import os
import uuid
import logging
import datetime
import json
from botocore.config import Config as botoConfig
from datetime import datetime, timedelta


def get_cfn_parameters():
    airflow_name = os.environ['AIRFLOW_ENV_NAME']
    return get_ssm_parameters_by_path(f'/stc/{airflow_name}/variables/')


def get_ssm_parameters_by_path(path, parameters=None, next_token=None):
    ssm_client = get_boto_client('ssm')

    print(f'get_ssm_parameters_by_path: {path}')

    if next_token:
        response = ssm_client.get_parameters_by_path(
            Path=path, Recursive=True, WithDecryption=True, NextToken=next_token)
    else:
        response = ssm_client.get_parameters_by_path(Path=path, Recursive=True, WithDecryption=True)

    if not parameters:
        parameters = dict()

    for parameter in response['Parameters']:
        key = parameter['Name'].replace(path, '', 1)
        if key.startswith('/'):
            key = key[1:]

        parameters[key] = parameter

    token = response.get('NextToken', None)
    if token:
        print(f'token {token}')
        get_ssm_parameters_by_path(path, parameters, token)

    return parameters


def create_alarm(alias, subject, description):
    """
    Create an alarm in ops genie.
    :param alias: Used for closing the alarm Ops Genie alarms
    :param subject: The title of an alarm that displace in the ops genie
    :param description: The description in the alarm
    :return:
    """
    sns_client = get_boto_client('sns')
    topic_arn = os.environ['OPS_GENIE_ENDPOINT']
    sns_client.publish(
        TopicArn=topic_arn,
        Subject=f'Prefect-POC-{subject}',
        Message=description,
        MessageAttributes={
            'X-Alert-Key': {
                'DataType': 'String',
                'StringValue': str(uuid.uuid4())
            },
            'alias': {
                'DataType': 'String',
                'StringValue': f'Prefect-POC-{alias}',
            },
            'eventType': {
                'DataType': 'String',
                'StringValue': 'create'
            }
        }
    )
    print('create_alarm fired')


def close_alarm(alias):
    """
    Close an alarm in ops genie
    :param alias: the alarm alias used for creation
    :return:
    """
    sns_client = get_boto_client('sns')
    topic_arn = os.environ['OPS_GENIE_ENDPOINT']
    sns_client.publish(
        TopicArn=topic_arn,
        Subject='closed',
        Message='closed',
        MessageAttributes={
            'X-Alert-Key': {
                'DataType': 'String',
                'StringValue': str(uuid.uuid4())
            },
            'alias': {
                'DataType': 'String',
                'StringValue': alias
            },
            'eventType': {
                'DataType': 'String',
                'StringValue': 'close'
            }
        }
    )
    print('close_alarm fired')


def get_boto_client(client):
    boto_config = botoConfig(
        retries=dict(
            max_attempts=50
        )
    )

    return boto3.client(client, config=boto_config)


def put_task_state(task_state_id, task_key, task_state, table_name, dynamodb=None):
    # DynamoDB Schema
    #   TableName: !Sub ${AirflowName}-task-state
    #       task_state_id: S          - dag_id 'manual__2021-02-06T00:26:43.686197+00:00__demo_dag__ECS_TASK1__20210206'
    #       task_key: S
    #       value: S
    #       expiration_time: N - Unix epoch timestamp on put, used for ttl
    local_args = locals()
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    expiration_time = _get_expiration_time()
    serialized_task_state = json.dumps(task_state, default=default_serializer)

    item = {
        'task_state_id': task_state_id,
        'task_key': task_key,
        'expiration_time': expiration_time,
        'task_state': serialized_task_state
    }

    table = dynamodb.Table(table_name)

    try:
        table.put_item(
            Item=item
        )
    except:
        logging.error('get_task_state.local_args: %s', local_args)
        raise
    finally:
        logging.info('push_task_state complete')


def _get_expiration_time():
    expiration_time = (datetime.now() + timedelta(hours=24)).strftime('%s')
    return expiration_time


def get_task_state_item(task_state_id, task_key, table_name, dynamodb=None):
    # DynamoDB Schema
    #   TableName: !Sub ${AirflowName}-task-state
    #       task_key: S          - dag_id 'manual__2021-02-06T00:26:43.686197+00:00__demo_dag__ECS_TASK1__20210206'
    #       dag_run: S
    #       value: S
    #       expiration_time: N - Unix epoch timestamp on put, used for ttl

    local_args = locals()
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)
    response = None
    try:
        response = table.get_item(
            Key={
                'task_state_id': task_state_id,
                'task_key': task_key
            }
        )

        # Check if there are results, if so return the parsed value
        item = response.get('Item')
        if item:
            result = item['task_state']
            return json.loads(result)
    except:
        logging.error('get_task_state.local_args: %s', local_args)
        raise
    finally:
        logging.info('get_task_state complete')


def update_task_state(task_state_id, task_key, task_state, table_name, dynamodb=None):
    local_args = locals()
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)
    expiration_time = _get_expiration_time()

    try:
        serialized_task_state = json.dumps(task_state, default=default_serializer)
        response = table.update_item(
            Key={
                'task_state_id': task_state_id,
                'task_key': task_key
            },
            UpdateExpression="set task_state=:t, expiration_time=:e",
            ExpressionAttributeValues={
                ':t': serialized_task_state,
                ':e': expiration_time,
            },
            ReturnValues="UPDATED_NEW"
        )
        result = response['Attributes']['task_state']
        return json.loads(result)
    except:
        logging.error('get_task_state.local_args: %s', local_args)
        raise
    finally:
        logging.info('get_task_state complete')


def default_serializer(o):
    if isinstance(o, datetime):
        return o.isoformat()

    raise TypeError("Type %s not serializable" % type(o))


def get_etl_status():
    credentials = session.get_credentials()

    session = boto3.Session()
import boto3
import requests
import json
import time
import urllib
from aws_requests_auth.aws_auth import AWSRequestsAuth

EDL_API = 'edl.playpen.stc.aws.shaw.ca'
DEFAULT_REGION = 'us-west-2'



def get_auth_token():
    session = boto3.Session()
    credentials = session.get_credentials()

    return AWSRequestsAuth(aws_access_key=credentials.access_key,
                           aws_secret_access_key=credentials.secret_key,
                           aws_token=credentials.token,
                           aws_host=EDL_API,
                           aws_region=DEFAULT_REGION,
                           aws_service='execute-api')


def get_status(data_source_name, s3_url, auth: None):
    if not auth:
        auth = get_auth_token()

    assert data_source_name, 'data_source_name: required'
    assert s3_url, 's3_url: required'

    query_params = {
        's3_url': s3_url,
        'data_source_name': data_source_name
    }
    query_string = urllib.parse.urlencode(query_params)
    url = f'https://{EDL_API}/playpen/datalake-queue/etl-status?{query_string}'
    print(f'status url: {url}')

    response = requests.get(url, auth=auth)
    print('--------------------------------------------')
    print(f'EDL_API: {url}')
    print(f'status_code: {response.status_code}')
    print(response.content)
    print('--------------------------------------------')

    assert response.status_code == 200, 'status call failed'
    return json.loads(response.content.decode('utf8'))


def post_job(config_base_dir, file_path: None, auth: None):
    if not auth:
        auth = get_auth_token();

    url = f'https://{EDL_API}/playpen/datalake-queue/etl-preparation'
    request = {
        'config_base_dir': config_base_dir
    }

    if file_path:
        request['file_path'] = file_path

    response = requests.post(url, auth=auth, json=request)
    print('--------------------------------------------')
    print(f'EDL_API: {url}')
    print(f'status_code: {response.status_code}')
    print(response.content)
    print('--------------------------------------------')

    assert response.status_code == 200, 'post_job call failed'
    return json.loads(response.content.decode('utf8'))



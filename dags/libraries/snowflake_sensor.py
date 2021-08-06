from builtins import str

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from big_data_helpers import get_status, get_auth_token, post_job


class SnowflakeSensor(BaseSensorOperator):
    """
    Use snowflake as a sensor
    """

    template_fields = ['query', 'connection']

    @apply_defaults
    def __init__(self,
                 sql,
                 snowflake_conn_id=None,
                 warehouse=None,
                 database=None,
                 schema=None,
                 *args, **kwargs):
        super(SnowflakeSensor, self).__init__(*args, **kwargs)
        self.query = query
        self.query = connenction

    def poke(self, context):



        # self.log.info('Poking: %s', self.data_source_name)
        # self.log.info('s3_url: %s', self.s3_url)
        #
        # assert self.s3_url.startswith('s3://'), 's3_url: must be in s3 uri format "s3://{bucket}/{key}'
        #
        # auth = get_auth_token()
        # status = get_status(self.data_source_name, self.s3_url, auth)
        #
        # print('--------  poke  ---------')
        # print(status.get('is_success'))
        #
        # return status.get('is_success')

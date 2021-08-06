# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from builtins import str

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from big_data_helpers import get_status, get_auth_token, post_job


class BigDataSensor(BaseSensorOperator):
    """
    Calls the arch-big-data

    :param http_conn_id: The connection to run the sensor against
    :type http_conn_id: str
    :param method: The HTTP request method to use
    :type method: str
    :param endpoint: The relative part of the full url
    :type endpoint: str
    :param request_params: The parameters to be added to the GET url
    :type request_params: a dictionary of string key/value pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        Returns True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ['data_source_name', 's3_url']

    @apply_defaults
    def __init__(self,
                 data_source_name,
                 s3_url=None,
                 *args, **kwargs):
        super(BigDataSensor, self).__init__(*args, **kwargs)
        self.data_source_name = data_source_name
        self.s3_url = s3_url

    def poke(self, context):
        self.log.info('Poking: %s', self.data_source_name)
        self.log.info('s3_url: %s', self.s3_url)

        assert self.s3_url.startswith('s3://'), 's3_url: must be in s3 uri format "s3://{bucket}/{key}'

        auth = get_auth_token()
        status = get_status(self.data_source_name, self.s3_url, auth)

        print('--------  poke  ---------')
        print(status.get('is_success'))

        return status.get('is_success')

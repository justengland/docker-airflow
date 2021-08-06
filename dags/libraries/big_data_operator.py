from airflow.models.baseoperator import BaseOperator
from big_data_helpers import get_status, get_auth_token, post_job

BASE_DIR = 'test/calculate/pi'
DATA_SOURCE_NAME = 'test_calculate_pi'

# template_fields = (data_source_name, s3_url)
class BigDataOperator(BaseOperator):
    template_fields = ['config_base_directory', 'file_path']
    def __init__(
            self,
            config_base_directory,
            file_path=None,
            **kwargs) -> None:
        super().__init__(**kwargs)

        self.config_base_directory = config_base_directory
        self.file_path = file_path

    def execute(self, context):
        auth = get_auth_token()
        response = post_job(self.config_base_directory, self.file_path, auth)

        ti = context['ti']
        ti.xcom_push(
            key="s3_url",
            value=response['s3_url']
        )

        print(f'response: {response}')

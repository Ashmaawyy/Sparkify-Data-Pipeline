from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from psycopg2 import Error

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 region = '',
                 test_count_query = '',
                 expected_result = 0,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.test_count_query = test_count_query
        self.expected_result = expected_result

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        try:
            self.log.info('Executing data quality query...')
            query_result = redshift.run(self.test_count_query)
            if query_result == self.expected_result:
                self.log.info('Data qauality checks completed successfully :)')
                self.log.info('test query result = {}\nexpected result = {}'.format(query_result, self.expected_result))
            else:
                self.log.error('Data quality check failed expected result = {}\nreturned result = {}'.format(query_result, self.expected_result))
        except Error as e:
            self.log.error(e)
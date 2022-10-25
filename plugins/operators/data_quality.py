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
                 table = '',
                 query = '',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.table = table
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        try:
            self.log.info('Executing data quality queries...')
            redshift.run(self.query)
            self.log.info('Data qauality checks completed successfully :)')
        except Error as e:
            self.log.info(e)
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
                 create_sql = '',
                 load_sql = '',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.table = table
        self.create_sql = create_sql
        self.load_sql = load_sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

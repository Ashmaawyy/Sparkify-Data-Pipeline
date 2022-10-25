from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from psycopg2 import Error

class LoadDimensionsOperator(BaseOperator):
    ui_color = '#80BD9E'

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
        
        try:
            # Creating tables
            self.log.info('Creating {} dimention table...'.format(self.table))
            redshift.run(self.create_sql)
            self.log.info('Created {} dimention table successfully :)'.format(self.table))

            # Loading tables
            self.log.info('Loading {} dimention table...'.format(self.table))
            redshift.run(self.load_sql)
            self.log.info('Loaded {} dimention table successfully :)'.format(self.table))

        except Error as e:
            self.log.error(e)

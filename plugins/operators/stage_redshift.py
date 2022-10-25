from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from psycopg2 import Error

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    def __init__(self,
                 # Operators params (with defaults) here
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 region = '',
                 table = '',
                 schema = '',
                 s3_bucket = '',
                 s3_key = '',
                 delimiter = ',',
                 ignore_headers = '1',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.table = table
        self.schema = schema
        self.ignore_headers = ignore_headers

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        try:
            self.log.info('Creating {} table...'.format(self.table))
            redshift.run(self.schema)
            self.log.info('{} table created successfully :)'.format(self.table))

        except Error as e:
            self.log.info(e)

        self.log.info('Copying data from S3 bucket {} to Redshift table {}'.format(self.s3_bucket, self.table))
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        formatted_copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.ignore_headers,
            self.delimiter
        )
        try:
            redshift.run(formatted_copy_sql)
            self.log.info('Data copied to {} susseccfully :)'.format(self.table))

        except Error as e:
            self.log.info(e)

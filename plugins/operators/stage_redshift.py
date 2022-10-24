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
        REGION 'us-west-2'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    def __init__(self,
                 # Operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 delimiter = ',',
                 ignore_headers = '1',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.table = table
        self.ignore_headers = ignore_headers

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info('Creating staging Redshift tables if they do not exist')
        try:
            redshift.run('''
            CREATE TABLE public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstName varchar(256),
            gender varchar(256),
            itemInSession int4,
            lastName varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionId int4,
            song varchar(256),
            status int4,
            ts int8,
            userAgent varchar(256),
            userId int4);
            ''')
            redshift.run('''
            CREATE TABLE public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4);
            ''')
            self.log.info('Staging tables created successfully :)')

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
            self.ignore_headers,
            self.delimiter
        )
        try:
            redshift.run(formatted_copy_sql)
            self.log.info('Data copied to {} susseccfully :)'.format(self.table))

        except Error as e:
            self.log.info(e)

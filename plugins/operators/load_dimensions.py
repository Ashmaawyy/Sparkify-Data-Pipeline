from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from psycopg2 import Error

class LoadDimensionsOperator(BaseOperator):
    ui_color = '#80BD9E'

    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 region = '',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        songs_table_create_statement = '''
        CREATE TABLE public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
        '''
        artists_table_create_statement = '''
        CREATE TABLE public.artists (
        artistid varchar(256) NOT NULL,
        name varchar(256),
        location varchar(256),
        lattitude numeric(18,0),
        longitude numeric(18,0)
        );
        '''
        users_table_create_statement = '''
        CREATE TABLE public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
        '''
        time_table_create_statement = '''
        CREATE TABLE public."time" (
        start_time timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );
        '''
        
        try:
            # Creating Tables
            self.log.info('Creating songs dimention table...')
            redshift.run(songs_table_create_statement)
            self.log.info('Created songs dimention table successfully :)')
            self.log.info('Creating artists dimention table...')
            redshift.run(artists_table_create_statement)
            self.log.info('Created artists dimention table successfully :)')
            self.log.info('Creating users dimention table...')
            redshift.run(users_table_create_statement)
            self.log.info('Created users dimention table successfully :)')
            self.log.info('Creating time dimention table...')
            redshift.run(time_table_create_statement)
            self.log.info('Created time dimention table successfully :)')
            # Loading tables
            self.log.info('Loading songs dimention table...')
            redshift.run()
            self.log.info('Loaded songs dimention table successfully :)')
            self.log.info('Loading artists dimention table...')
            redshift.run()
            self.log.info('Loaded artists dimention table successfully :)')
            self.log.info('Loading users dimention table...')
            redshift.run()
            self.log.info('Loaded users dimention table successfully :)')
            self.log.info('Loading time dimention table...')
            redshift.run()
            self.log.info('Loaded time dimention table successfully :)')
        except Error as e:
            self.log.info(e)

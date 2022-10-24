import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_facts import LoadFactsOperator
from plugins.operators.load_dimensions import LoadDimensionsOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'ashmawy',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('sparkify_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@yearly'
        )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

staging_events_create_sql = '''
CREATE TABLE IF NOT EXISTS public.stage_events (
    artist varchar(256),
    auth varchar(256),
    firstName varchar(256),
    gender varchar(256),
    itemInSession int4,
    lastName varchar(256),
    length numeric(18,0),
    level varchar(256),
    location varchar(256),
    method varchar(256),
    page varchar(256),
    registration numeric(18,0),
    sessionId int4,
    song varchar(256),
    status int4,
    ts int8,
    userAgent varchar(256),
    userId int4
    );
'''
staging_songs_create_sql = '''
CREATE TABLE IF NOT EXISTS public.staging_songs (
    num_songs int4,
    artist_id varchar(256),
    artist_name varchar(256),
    artist_latitude numeric(18,0),
    artist_longitude numeric(18,0),
    artist_location varchar(256),
    song_id varchar(256),
    title varchar(256),
    duration numeric(18,0),
    "year" int4
    );
'''
songplays_table_create_sql = '''
CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
    );
'''
users_table_create_sql = '''
CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
'''
songs_table_create_sql = '''
CREATE TABLE IF NOT EXISTS public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
'''
artists_table_create_sql = '''
CREATE TABLE IF NOT EXIISTS public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
    );
'''
time_table_create_sql = '''
CREATE TABLE IF NOT EXISTS public."time" (
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

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'staging_events',
    schema = staging_events_create_sql,
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'staging_songs',
    schema = staging_songs_create_sql,
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data'
)

load_songplays_fact_table = LoadFactsOperator(
    task_id = 'load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = songplays_table_create_sql,
    load_sql = SqlQueries.songplays_table_insert
)

load_users_dimension_table = LoadDimensionsOperator(
    task_id = 'load_user_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'users',
    create_sql = users_table_create_sql,
    load_sql = SqlQueries.users_table_insert
)

load_songs_dimension_table = LoadDimensionsOperator(
    task_id = 'load_song_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songs',
    create_sql = songs_table_create_sql,
    load_sql = SqlQueries.songs_table_insert
)

load_artists_dimension_table = LoadDimensionsOperator(
    task_id = 'load_artist_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'artists',
    create_sql = artists_table_create_sql,
    load_sql = SqlQueries.artists_table_insert
)

load_time_dimension_table = LoadDimensionsOperator(
    task_id = 'load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'time',
    create_sql = time_table_create_sql,
    load_sql = SqlQueries.time_table_insert
)

run_data_quality_checks = DataQualityOperator(
    task_id = 'run_data_quality_checks',
    dag = dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
#                     Stage #1                           Stage #2                           Stage #3                                    Stage #4                        Stage #5
#
#                                                                                              =======> load_songs_dimention_table 
#                      ======> satge_events_to_redshift                                        =======> load_users_dimention_table
#                   ||                                 \\                                  ||                                        \\
#   start_operator                                        =====> load_songplays_fact_table                                              ======> run_data_quality_checks ======> end_operator
#                   \\                                 ||                                  \\                                        ||
#                      ======> stage_songs_to_redshift                                         ======> load_artists_dimention_table
#                                                                                              ======> load_time_dimention_table
#

# First stage in DAG
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Second Stage in DAG
stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

# Third stage in DAG
load_songplays_fact_table >> load_songs_dimension_table
load_songplays_fact_table >> load_users_dimension_table
load_songplays_fact_table >> load_artists_dimension_table
load_songplays_fact_table >> load_time_dimension_table

# Fourth stage in DAG
load_songs_dimension_table >> run_data_quality_checks
load_users_dimension_table >> run_data_quality_checks
load_artists_dimension_table >> run_data_quality_checks
load_time_dimension_table >> run_data_quality_checks

# Fifth (and final) stage in DAG
run_data_quality_checks >> end_operator

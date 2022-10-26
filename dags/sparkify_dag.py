import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_facts import LoadFactsOperator
from plugins.operators.load_dimensions import LoadDimensionsOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.creat_table import CreateTableOperator
from plugins.helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'ashmawy',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False
}

dag = DAG('sparkify_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly'
        )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

create_staging_events_table = CreateTableOperator(
    task_id = 'create_staging_events_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'staging_events',
    create_sql = SqlQueries.staging_events_create_sql
)

create_staging_songs_table = CreateTableOperator(
    task_id = 'create_staging_songs_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = SqlQueries.staging_songs_create_sql
)

create_songplays_fact_table = CreateTableOperator(
    task_id = 'create_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = SqlQueries.songplays_table_create_sql
)

create_songs_table = CreateTableOperator(
    task_id = 'create_songs_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = SqlQueries.songs_table_create_sql
)

create_users_table = CreateTableOperator(
    task_id = 'create_users_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = SqlQueries.users_table_create_sql
)

create_artists_table = CreateTableOperator(
    task_id = 'create_artists_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = SqlQueries.artists_table_create_sql
)

create_time_table = CreateTableOperator(
    task_id = 'create_time_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songplays',
    create_sql = SqlQueries.time_table_create_sql
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_events',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'staged_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'staged_songs',
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
    load_sql = SqlQueries.songplays_table_insert
)

load_users_dimension_table = LoadDimensionsOperator(
    task_id = 'load_users_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'users',
    load_sql = SqlQueries.users_table_insert
)

load_songs_dimension_table = LoadDimensionsOperator(
    task_id = 'load_songs_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'songs',
    load_sql = SqlQueries.songs_table_insert
)

load_artists_dimension_table = LoadDimensionsOperator(
    task_id = 'load_artists_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'artists',
    load_sql = SqlQueries.artists_table_insert
)

load_time_dimension_table = LoadDimensionsOperator(
    task_id = 'load_time_dim_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    table = 'time',
    load_sql = SqlQueries.time_table_insert
)

userId_data_quality_check = DataQualityOperator(
    task_id = 'userId_data_quality_check',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    region = 'us-west-2',
    test_count_query = SqlQueries.userId_data_quality_check,
    expected_result = 0
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)

#
#                     Stage #1                           Stage #2                           Stage #3                                    Stage #4                        Stage #5
#
#                                                                                              =======> load_songs_dimention_table 
#                      ======> satge_events_to_redshift                                        =======> load_users_dimention_table
#                   ||                                 \\                                  ||                                        \\
#   start_operator                                        =====> load_songplays_fact_table                                              ======> userId_data_quality_check ======> end_operator
#                   \\                                 ||                                  \\                                        ||
#                      ======> stage_songs_to_redshift                                         ======> load_artists_dimention_table
#                                                                                              ======> load_time_dimention_table
#

# First stage in DAG
start_operator >> create_staging_events_table
start_operator >> create_staging_songs_table
start_operator >> create_songplays_fact_table
start_operator >> create_songs_table
start_operator >> create_artists_table
start_operator >> create_users_table
start_operator >> create_time_table

create_staging_events_table >> stage_events_to_redshift
create_staging_songs_table >> stage_songs_to_redshift
create_songplays_fact_table >> stage_events_to_redshift
create_songplays_fact_table >> stage_songs_to_redshift
create_songs_table >> stage_songs_to_redshift
create_artists_table >> stage_songs_to_redshift
create_users_table >> stage_events_to_redshift
create_time_table >> stage_events_to_redshift

# Second Stage in DAG
stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

# Third stage in DAG
load_songplays_fact_table >> load_songs_dimension_table
load_songplays_fact_table >> load_users_dimension_table
load_songplays_fact_table >> load_artists_dimension_table
load_songplays_fact_table >> load_time_dimension_table

# Fourth stage in DAG
load_songs_dimension_table >> userId_data_quality_check
load_users_dimension_table >> userId_data_quality_check
load_artists_dimension_table >> userId_data_quality_check
load_time_dimension_table >> userId_data_quality_check

# Fifth (and final) stage in DAG
userId_data_quality_check >> end_operator

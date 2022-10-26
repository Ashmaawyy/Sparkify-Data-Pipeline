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
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'depends_on_past': False,
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': True
}

dag = DAG('sparkify_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly'
        )

start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

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
load_songs_dimension_table >> userId_data_quality_check
load_users_dimension_table >> userId_data_quality_check
load_artists_dimension_table >> userId_data_quality_check
load_time_dimension_table >> userId_data_quality_check

# Fifth (and final) stage in DAG
userId_data_quality_check >> end_operator

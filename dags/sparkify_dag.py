import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import \
    StageToRedshiftOperator, \
    LoadFactsOperator, \
    LoadDimensionsOperator, \
    DataQualityOperator
from plugins.helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

load_songplays_fact_table = LoadFactsOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_users_dimension_table = LoadDimensionsOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_songs_dimension_table = LoadDimensionsOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artists_dimension_table = LoadDimensionsOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionsOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#                                                                                            
#                                                                                              ======> load_songs_dimention_table ===========> 
#                      ======> satge_events_to_redshift                                        ======> load_users_dimention_table ============>
#                   ||                                 \\                                  ||                                                     \\
#   start_operator                                        =====> load_songplays_fact_table                                                           ========> run_data_quality_checks ===========> end_operator
#                   \\                                 ||                                  \\                                                     ||
#                      ======> stage_songs_to_redshift                                         =======> load_artists_dimention_table =========>
#                                                                                              =======> load_time_dimention_table ===========>
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
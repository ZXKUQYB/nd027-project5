from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    json_format='auto'
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    append_mode=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    append_mode=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    append_mode=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    append_mode=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_list=['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Use >> to denote Airflow tasks dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_user_dimension_table
load_songplays_fact_table >> load_song_dimension_table
load_songplays_fact_table >> load_artist_dimension_table
load_songplays_fact_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
    
# Tasks dependencies can also be set with “set_downstream” or “set_upstream”
#start_operator.set_downstream(stage_events_to_redshift)
#start_operator.set_downstream(stage_songs_to_redshift)

#stage_events_to_redshift.set_downstream(load_songplays_fact_table)
#stage_songs_to_redshift.set_downstream(load_songplays_fact_table)

#load_songplays_fact_table.set_downstream(load_user_dimension_table)
#load_songplays_fact_table.set_downstream(load_song_dimension_table)
#load_songplays_fact_table.set_downstream(load_artist_dimension_table)
#load_songplays_fact_table.set_downstream(load_time_dimension_table)

#load_user_dimension_table.set_downstream(run_quality_checks)
#load_song_dimension_table.set_downstream(run_quality_checks)
#load_artist_dimension_table.set_downstream(run_quality_checks)
#load_time_dimension_table.set_downstream(run_quality_checks)

#run_quality_checks.set_downstream(end_operator)
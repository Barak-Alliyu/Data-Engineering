from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 7, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
    'depends_on_past': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables=PostgresOperator(
    task_id='Create_tables_in_Redshift',
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_option='s3://udacity-dend/log_json_path.json',
    aws_region='us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_option="auto",
    aws_region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    fact_sql_query=f"""
        INSERT INTO public.songplays(playid,start_time,userid,
            level,songid,artistid,sessionid,location,user_agent)
        {SqlQueries.songplay_table_insert}
    """
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    clear_table=True,
    dim_sql_query=f"""
        INSERT INTO users(userid, first_name, last_name, gender, level)
        {SqlQueries.user_table_insert}
    """
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    clear_table=True,
    dim_sql_query=f"""
        INSERT INTO songs(songid, title, artistid, year, duration)
        {SqlQueries.song_table_insert}
    """
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    clear_table=True,
    dim_sql_query=f"""
        INSERT INTO artists(artistid, name, location, lattitude, longitude)
        {SqlQueries.artist_table_insert}
    """
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    clear_table=True,
    dim_sql_query=f"""
        INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        {SqlQueries.time_table_insert}
    """
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables={'users':'userid', 'songs':'songid', 'artists':'artistid', 'time':'start_time'}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables 
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
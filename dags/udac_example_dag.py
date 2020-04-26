from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator,
                               LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'apozuelog',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 4, 25),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# STAGING
stage_bike_trips_to_redshift = StageToRedshiftOperator(
    task_id='Stage_bike_trips',
    dag=dag,
    table="bikes.staging_trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="aws-logs-004583112324-us-west-2",
    s3_key="fp/csv/",
    file_type="csv"
)

# DIMENSION
load_bikes_table = LoadDimensionOperator(
    task_id='Load_bikes_dim_table',
    dag=dag,
    table='bikes.bikes_table',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.bikes_table_insert
)

load_stations_table = LoadDimensionOperator(
    task_id='Load_stations_dim_table',
    dag=dag,
    table='bikes.stations_table',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.stations_table_insert
)

load_time_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='bikes.time_table',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert
)

load_users_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='bikes.users_table',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.users_table_insert
)

# FACT
load_bike_trips_table = LoadFactOperator(
    task_id='Load_bike_trips_fact_table',
    dag=dag,
    table='bikes.bike_trips_table',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.bike_trips_table_insert
)

# QUALITY
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['bikes.bike_trips_table', 'bikes.users_table', 'bikes.stations_table', 'bikes.bikes_table', 'bikes.time_table'],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# DAG tasks

start_operator >> stage_bike_trips_to_redshift

stage_bike_trips_to_redshift >> load_bikes_table
stage_bike_trips_to_redshift >> load_stations_table
stage_bike_trips_to_redshift >> load_time_table
stage_bike_trips_to_redshift >> load_users_table

load_bikes_table >> load_bike_trips_table
load_stations_table >> load_bike_trips_table
load_time_table >> load_bike_trips_table
load_users_table >> load_bike_trips_table

load_bike_trips_table >> run_quality_checks

run_quality_checks >> end_operator

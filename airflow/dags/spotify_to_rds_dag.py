from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from plugins.operators.spotify_to_rds_operator import SpotifyToRDSOperator
from plugins.operators.app.extract_data_app import run_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'spotify_to_rds_dag',
    default_args=default_args,
    description='Extract data from Spotify API and upload to RDS',
    schedule_interval=timedelta(days=1),  # Run daily
)


# def spotify_api_call(**kwargs):
#     python_api_call = PythonOperator(
#         task_id='run_flask_app',
#         python_callable=run_extract,
#         xcom_push=True,
#         dag=dag
#     )

# def convert_json_to_dataframe(**kwargs):
#     ti = kwargs['ti']
#     response_data = ti.xcom_pull(task_ids='extract_data_task', key='return_value')
#     # Convert JSON to DataFrame
#     df = pd.read_json(response_data, orient='records')
#     # Push DataFrame to XCom
#     ti.xcom_push(key='spotify_data_df', value=df)

# Task to extract data from Spotify API using HTTP request
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=run_etl,
    provide_context=True,
    dag=dag,
)

# # Task to convert JSON to DataFrame and save as csv
# convert_json_task = PythonOperator(
#     task_id='convert_json_task',
#     python_callable=convert_json_to_dataframe,
#     provide_context=True,
#     dag=dag,
# )


# # Task to upload data to RDS
# upload_to_rds_task = SpotifyToRDSOperator(
#     task_id='upload_to_rds_task',
#     provide_context=True,  # To access XComs
#     dag=dag,
# )

# Set task dependencies
extract_data_task
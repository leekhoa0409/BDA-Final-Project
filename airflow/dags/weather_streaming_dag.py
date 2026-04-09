"""
Weather Streaming DAG

Manually triggered DAG that starts Spark Structured Streaming job.
Reads from Kafka topic 'weather-raw' and writes to Bronze Delta table.
This is a long-running job - trigger once and it runs continuously.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from common import SPARK_SUBMIT_STREAMING

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='weather_streaming',
    default_args=default_args,
    description='Start Spark Structured Streaming: Kafka -> Bronze (weather)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lakehouse', 'weather', 'streaming', 'kafka'],
) as dag:

    start_streaming = BashOperator(
        task_id='start_weather_streaming',
        bash_command=f'{SPARK_SUBMIT_STREAMING} /opt/spark/jobs/streaming_bronze_weather.py',
        execution_timeout=timedelta(hours=24),
    )

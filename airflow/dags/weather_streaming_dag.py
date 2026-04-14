"""
Weather Streaming DAG (Supervisor Pattern)

Runs every 30 minutes and checks if Spark Streaming is already active.
- If not running: starts a new streaming job (Kafka -> Bronze Delta)
- If already running: skips to avoid duplicate consumers

This ensures continuous streaming even after crashes or timeouts.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from common import SPARK_SUBMIT_STREAMING

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def check_streaming_active(**kwargs):
    """Check if Spark Streaming job is already running on the cluster."""
    import json
    import urllib.request

    try:
        req = urllib.request.urlopen("http://spark-master:8080/json/", timeout=5)
        data = json.loads(req.read().decode())
        active_apps = [
            app for app in data.get("activeapps", [])
            if "streaming" in app.get("name", "").lower()
        ]
        if active_apps:
            return 'streaming_already_running'
    except Exception:
        pass
    return 'start_weather_streaming'


with DAG(
    dag_id='weather_streaming',
    default_args=default_args,
    description='Supervisor: ensures Spark Streaming (Kafka -> Bronze) is always running',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'weather', 'streaming', 'kafka'],
) as dag:

    check_active = BranchPythonOperator(
        task_id='check_streaming_active',
        python_callable=check_streaming_active,
    )

    already_running = EmptyOperator(
        task_id='streaming_already_running',
    )

    start_streaming = BashOperator(
        task_id='start_weather_streaming',
        bash_command=f'{SPARK_SUBMIT_STREAMING} /opt/spark/jobs/streaming_bronze_weather.py',
        execution_timeout=timedelta(hours=12),
    )

    check_active >> [start_streaming, already_running]

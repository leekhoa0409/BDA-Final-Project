from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='weather_etl_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_etl = SparkSubmitOperator(
        task_id='run_weather_etl',
        application='/opt/spark/jobs/weather_etl.py',  # path trong container
        conn_id='spark_default'
    )
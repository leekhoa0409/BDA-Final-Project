"""
Unified Weather Pipeline DAG

Hourly batch pipeline that:
1. Checks for new CSV files in landing zone
2. Ingests CSV to Bronze (if found)
3. Validates Bronze data quality
4. Runs weather ETL (Silver + Gold + Dim/Fact)
5. Validates Silver/Gold layers in parallel
6. Registers all Delta tables in Trino
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from common import SPARK_SUBMIT_BASE

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_landing_zone(**kwargs):
    """Check if CSV files exist in MinIO landing zone."""
    import os
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
    )
    try:
        resp = s3.list_objects_v2(Bucket="landing", Prefix="weather/", MaxKeys=10)
        files = [
            o["Key"] for o in resp.get("Contents", [])
            if o["Key"].endswith(".csv") and "/processed/" not in o["Key"]
        ]
        if len(files) > 0:
            return 'ingest_csv_to_bronze'
    except Exception:
        pass
    return 'skip_csv_ingest'


with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='Weather ETL: CSV + Streaming Bronze -> Silver -> Gold -> Trino',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lakehouse', 'weather', 'etl', 'pipeline'],
) as dag:

    # --- Stage 1: Check landing zone & ingest ---
    check_csv = BranchPythonOperator(
        task_id='check_csv_in_landing',
        python_callable=check_landing_zone,
    )

    ingest_csv = BashOperator(
        task_id='ingest_csv_to_bronze',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/batch_bronze_weather.py',
        execution_timeout=timedelta(hours=2),
    )

    skip_csv = EmptyOperator(
        task_id='skip_csv_ingest',
    )

    # --- Stage 2: Validate Bronze (before ETL reads it) ---
    validate_bronze = BashOperator(
        task_id='validate_bronze',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/validate_weather_bronze.py',
        trigger_rule='none_failed_min_one_success',
    )

    # --- Stage 3: ETL (Bronze -> Silver -> Gold + Dim/Fact) ---
    run_etl = BashOperator(
        task_id='run_weather_etl',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/weather_etl.py',
        execution_timeout=timedelta(hours=2),
    )

    # --- Stage 4: Validate Silver & Gold in parallel ---
    with TaskGroup(group_id='validate_outputs') as validate_outputs:
        validate_silver = BashOperator(
            task_id='validate_silver',
            bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/validate_weather_silver.py',
        )

        validate_gold = BashOperator(
            task_id='validate_gold',
            bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/validate_weather_gold.py',
        )

    # --- Stage 5: Register tables in Trino (after validation) ---
    register_tables = BashOperator(
        task_id='register_trino_tables',
        bash_command='python /opt/spark/jobs/register_trino_tables.py',
        trigger_rule='none_failed_min_one_success',
    )

    # Task dependencies:
    #
    # check_csv ─┬─> ingest_csv ─┐
    #            └─> skip_csv ───┤
    #                            ▼
    #                    validate_bronze
    #                            │
    #                            ▼
    #                        run_etl
    #                            │
    #                 ┌──────────┼──────────┐
    #                 ▼                     ▼
    #         validate_outputs      register_tables
    #         (silver + gold)
    #
    check_csv >> [ingest_csv, skip_csv]
    [ingest_csv, skip_csv] >> validate_bronze >> run_etl
    run_etl >> validate_outputs
    run_etl >> register_tables

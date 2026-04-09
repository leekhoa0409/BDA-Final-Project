"""
Unified Weather Pipeline DAG

Hourly batch pipeline that:
1. Checks for new CSV files in landing zone
2. Ingests CSV to Bronze (if found)
3. Runs weather ETL (Silver + Gold)
4. Validates Bronze/Silver/Gold layers
5. Registers tables in Trino
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

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
    import subprocess
    result = subprocess.run(
        ['python', '-c', '''
import boto3
s3 = boto3.client("s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123456")
try:
    resp = s3.list_objects_v2(Bucket="landing", Prefix="weather/", MaxKeys=10)
    files = [o["Key"] for o in resp.get("Contents", [])
             if o["Key"].endswith(".csv") and "/processed/" not in o["Key"]]
    print(len(files))
except Exception:
    print(0)
'''],
        capture_output=True, text=True
    )
    count = int(result.stdout.strip() or "0")
    if count > 0:
        return 'ingest_csv_to_bronze'
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

    run_etl = BashOperator(
        task_id='run_weather_etl',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/weather_etl.py',
        trigger_rule='none_failed_min_one_success',
        execution_timeout=timedelta(hours=2),
    )

    validate_bronze = BashOperator(
        task_id='validate_bronze',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/validate_weather_bronze.py',
    )

    validate_silver = BashOperator(
        task_id='validate_silver',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/validate_weather_silver.py',
    )

    validate_gold = BashOperator(
        task_id='validate_gold',
        bash_command=f'{SPARK_SUBMIT_BASE} /opt/spark/jobs/validate_weather_gold.py',
    )

    register_tables = BashOperator(
        task_id='register_trino_tables',
        bash_command='python /opt/spark/jobs/register_trino_tables.py',
    )

    # Task dependencies
    check_csv >> [ingest_csv, skip_csv]
    ingest_csv >> run_etl
    skip_csv >> run_etl
    run_etl >> validate_bronze >> [validate_silver, validate_gold]
    run_etl >> register_tables

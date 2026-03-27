
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lakehouse_etl_pipeline',
    default_args=default_args,
    description='Lakehouse ETL: Bronze → Silver → Gold with Delta Lake',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lakehouse', 'etl', 'delta-lake'],
) as dag:

    submit_etl_job = BashOperator(
        task_id='submit_spark_etl',
        bash_command=(
            '/home/airflow/.local/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '--deploy-mode client '
            '--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 '
            '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            '--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 '
            '--conf spark.hadoop.fs.s3a.access.key=admin '
            '--conf spark.hadoop.fs.s3a.secret.key=admin123456 '
            '--conf spark.hadoop.fs.s3a.path.style.access=true '
            '--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
            '--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
            '/opt/spark/jobs/example_etl.py'
        ),
    )

    check_bronze = BashOperator(
        task_id='check_bronze_data',
        bash_command='echo "Checking Bronze layer data..." && sleep 2',
    )

    check_silver = BashOperator(
        task_id='check_silver_data',
        bash_command='echo "Checking Silver layer data..." && sleep 2',
    )

    check_gold = BashOperator(
        task_id='check_gold_data',
        bash_command='echo "Checking Gold layer data..." && sleep 2',
    )

    submit_etl_job >> [check_bronze, check_silver, check_gold]

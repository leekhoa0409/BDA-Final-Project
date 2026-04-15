"""Shared Airflow DAG configuration."""
import os

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', '')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', '')

SPARK_SUBMIT = '/home/airflow/.local/bin/spark-submit'
SPARK_MASTER = 'spark://spark-master:7077'

SPARK_SUBMIT_BASE = (
    f'{SPARK_SUBMIT} '
    f'--master {SPARK_MASTER} '
    f'--deploy-mode client '
    f'--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
    f'--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
    f'--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} '
    f'--conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} '
    f'--conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} '
    f'--conf spark.hadoop.fs.s3a.path.style.access=true '
    f'--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem '
    f'--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false '
    f'--conf spark.cores.max=2'
)

SPARK_SUBMIT_STREAMING = SPARK_SUBMIT_BASE

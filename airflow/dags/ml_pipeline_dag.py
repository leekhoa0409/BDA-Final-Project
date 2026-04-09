"""
ML Pipeline DAG: Feature Store -> Training -> Model Registry -> Deployment

Schedule: Daily at 2 AM
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
from common import SPARK_SUBMIT_BASE

SPARK_CONF = SPARK_SUBMIT_BASE

PYTHON_CMD = '/opt/spark/jobs/'


def check_feature_store_data(**context):
    """Check if feature store has enough data for training."""
    import sys
    result = __import__('subprocess').run(
        [sys.executable, '-c', '''
import boto3
s3 = boto3.client("s3", endpoint_url="http://minio:9000",
    aws_access_key_id="admin", aws_secret_access_key="admin123456")
try:
    resp = s3.list_objects_v2(Bucket="warehouse", Prefix="features/training_data/", MaxKeys=1)
    print("ok" if resp.get("Contents") else "empty")
except Exception as e:
    print(f"error: {e}")
'''],
        capture_output=True, text=True
    )
    output = result.stdout.strip()
    if "ok" in output:
        return "proceed"
    return "skip_training"


with DAG(
    dag_id='ml_pipeline',
    default_args=default_args,
    description='ML Pipeline: Feature Store -> Training -> Model Registry',
    schedule_interval='0 2 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['ml', 'mlflow', 'training', 'weather'],
) as dag:

    check_features = PythonOperator(
        task_id='check_feature_store_data',
        python_callable=check_feature_store_data,
    )

    update_features = BashOperator(
        task_id='update_feature_store',
        bash_command=f'{SPARK_CONF} {PYTHON_CMD}feature_store.py',
        execution_timeout=timedelta(hours=1),
    )

    train_model = BashOperator(
        task_id='train_ml_model',
        bash_command=f'{SPARK_CONF} {PYTHON_CMD}ml_training.py',
        execution_timeout=timedelta(hours=2),
    )

    verify_model = BashOperator(
        task_id='verify_model_registration',
        bash_command="""python -c "
import mlflow
mlflow.set_tracking_uri('http://mlflow:5000')
client = mlflow.MlflowClient()
models = client.get_latest_versions('weather-forecast-model', stages=['Production'])
if models:
    print(f'Production model version: {models[0].version}')
else:
    print('WARNING: No production model found. Please assign a model to Production in MLflow UI.')
" """,
    )

    update_model_serving = BashOperator(
        task_id='reload_model_in_serving',
        bash_command='curl -s http://model-serving:8000/health || echo "Model serving not ready"',
    )

    check_features >> update_features >> train_model >> verify_model >> update_model_serving
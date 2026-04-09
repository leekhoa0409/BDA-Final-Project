import os
import shutil
import tempfile
import json

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit tests (no MinIO/S3 needed)."""
    warehouse_dir = tempfile.mkdtemp(prefix="spark_warehouse_")
    builder = (SparkSession.builder
               .master("local[2]")
               .appName("LakehouseTests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", warehouse_dir)
               .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + warehouse_dir)
               .config("spark.ui.enabled", "false"))
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture
def tmp_delta_path(tmp_path):
    """Temporary directory path for writing Delta tables in tests."""
    return str(tmp_path / "delta_output")


@pytest.fixture
def sample_weather_json():
    """Sample OpenWeather One Call API 3.0 response JSON."""
    return json.dumps({
        "lat": 21.0245,
        "lon": 105.8412,
        "timezone": "Asia/Ho_Chi_Minh",
        "timezone_offset": 25200,
        "current": {
            "dt": 1711540800,
            "sunrise": 1711496400,
            "sunset": 1711540200,
            "temp": 32.5,
            "feels_like": 35.2,
            "pressure": 1010,
            "humidity": 65,
            "dew_point": 24.5,
            "uvi": 8.5,
            "clouds": 10,
            "visibility": 10000,
            "wind_speed": 3.5,
            "wind_deg": 180,
            "wind_gust": 5.2,
            "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        },
    })


# E2E helper fixtures

SERVICE_PORTS = {
    "minio": 9000,
    "spark_master": 8080,
    "airflow": 8082,
    "trino": 8085,
    "superset": 8088,
    "kafka": 29092,
    "nifi": 8443,
}


@pytest.fixture(scope="session")
def service_urls():
    """URLs for Docker services (localhost mapped ports)."""
    host = os.environ.get("DOCKER_HOST_IP", "localhost")
    return {
        "minio": f"http://{host}:9000",
        "minio_console": f"http://{host}:9001",
        "spark_master": f"http://{host}:8080",
        "airflow": f"http://{host}:8082",
        "trino": f"http://{host}:8085",
        "superset": f"http://{host}:8088",
        "kafka": f"{host}:29092",
        "nifi": f"https://{host}:8443",
    }

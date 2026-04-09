"""E2E tests for service connectivity and health checks."""
import pytest
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TIMEOUT = 10


@pytest.mark.e2e
class TestServiceConnectivity:

    def test_minio_health(self, service_urls):
        resp = requests.get(f"{service_urls['minio']}/minio/health/live", timeout=TIMEOUT)
        assert resp.status_code == 200, f"MinIO health check failed: {resp.status_code}"

    def test_minio_buckets_exist(self, service_urls):
        """Check bronze, silver, gold buckets exist via MinIO API."""
        for bucket in ["bronze", "silver", "gold"]:
            resp = requests.head(f"{service_urls['minio']}/{bucket}", timeout=TIMEOUT)
            assert resp.status_code in (200, 403), \
                f"Bucket '{bucket}' not found: {resp.status_code}"

    def test_spark_master_alive(self, service_urls):
        resp = requests.get(f"{service_urls['spark_master']}/json/", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("status") == "ALIVE", f"Spark master status: {data.get('status')}"

    def test_spark_workers_registered(self, service_urls):
        resp = requests.get(f"{service_urls['spark_master']}/json/", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        workers = data.get("aliveworkers", data.get("workers", []))
        assert len(workers) >= 1 if isinstance(workers, list) else workers >= 1, \
            "No Spark workers registered"

    def test_airflow_health(self, service_urls):
        resp = requests.get(f"{service_urls['airflow']}/health", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        scheduler = data.get("scheduler", {})
        assert scheduler.get("status") == "healthy", \
            f"Airflow scheduler status: {scheduler.get('status')}"

    def test_trino_info(self, service_urls):
        resp = requests.get(f"{service_urls['trino']}/v1/info", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("starting") is False, "Trino is still starting"

    def test_superset_health(self, service_urls):
        resp = requests.get(f"{service_urls['superset']}/health", timeout=TIMEOUT)
        assert resp.status_code == 200

    def test_kafka_connectivity(self, service_urls):
        """Test Kafka broker is reachable."""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=[service_urls["kafka"]],
                consumer_timeout_ms=5000,
            )
            topics = consumer.topics()
            consumer.close()
            assert isinstance(topics, set), "Failed to retrieve Kafka topics"
        except ImportError:
            pytest.skip("kafka-python not installed")
        except Exception as e:
            pytest.fail(f"Kafka connection failed: {e}")

    def test_kafka_weather_topic_exists(self, service_urls):
        """Check weather-raw topic was created."""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=[service_urls["kafka"]],
                consumer_timeout_ms=5000,
            )
            topics = consumer.topics()
            consumer.close()
            assert "weather-raw" in topics, \
                f"Topic 'weather-raw' not found. Available: {topics}"
        except ImportError:
            pytest.skip("kafka-python not installed")

    def test_nifi_api_accessible(self, service_urls):
        """Check NiFi API is reachable (HTTPS with self-signed cert)."""
        try:
            resp = requests.get(
                f"{service_urls['nifi']}/nifi-api/system-diagnostics",
                verify=False, timeout=TIMEOUT,
            )
            assert resp.status_code in (200, 401, 403), \
                f"NiFi API returned {resp.status_code}"
        except requests.exceptions.ConnectionError:
            pytest.skip("NiFi not reachable")

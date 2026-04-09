"""E2E test for the weather pipeline (Kafka -> Bronze -> Silver -> Gold)."""
import json
import time

import pytest
import requests

TIMEOUT = 10
AIRFLOW_API = "http://localhost:8082/api/v1"
KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC = "weather-raw"
DAG_ID = "weather_batch_etl"

SAMPLE_WEATHER_MESSAGES = [
    {
        "coord": {"lon": 105.8412, "lat": 21.0245},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
        "base": "stations",
        "main": {"temp": 32.5, "feels_like": 35.2, "temp_min": 30.1, "temp_max": 34.0,
                 "pressure": 1010, "humidity": 65, "sea_level": 1010, "grnd_level": 1008},
        "visibility": 10000,
        "wind": {"speed": 3.5, "deg": 180, "gust": 5.2},
        "clouds": {"all": 10},
        "dt": 1711540800,
        "sys": {"type": 2, "id": 9308, "country": "VN", "sunrise": 1711496400, "sunset": 1711540200},
        "timezone": 25200, "id": 1581130, "name": "Hanoi", "cod": 200,
    },
    {
        "coord": {"lon": 106.6297, "lat": 10.8231},
        "weather": [{"id": 802, "main": "Clouds", "description": "scattered clouds", "icon": "03d"}],
        "base": "stations",
        "main": {"temp": 35.0, "feels_like": 38.1, "temp_min": 33.0, "temp_max": 36.5,
                 "pressure": 1008, "humidity": 55, "sea_level": 1008, "grnd_level": 1006},
        "visibility": 10000,
        "wind": {"speed": 4.2, "deg": 210, "gust": 6.1},
        "clouds": {"all": 40},
        "dt": 1711540800,
        "sys": {"type": 2, "id": 9314, "country": "VN", "sunrise": 1711496800, "sunset": 1711540600},
        "timezone": 25200, "id": 1566083, "name": "Ho Chi Minh City", "cod": 200,
    },
    {
        "coord": {"lon": 108.2208, "lat": 16.0544},
        "weather": [{"id": 500, "main": "Rain", "description": "light rain", "icon": "10d"}],
        "base": "stations",
        "main": {"temp": 28.3, "feels_like": 31.5, "temp_min": 26.8, "temp_max": 29.5,
                 "pressure": 1011, "humidity": 75, "sea_level": 1011, "grnd_level": 1009},
        "visibility": 8000,
        "wind": {"speed": 5.0, "deg": 160, "gust": 7.3},
        "clouds": {"all": 75},
        "dt": 1711540800,
        "sys": {"type": 2, "id": 9316, "country": "VN", "sunrise": 1711496600, "sunset": 1711540400},
        "timezone": 25200, "id": 1583992, "name": "Da Nang", "cod": 200,
    },
]


def airflow_auth():
    return ("airflow", "airflow")


def publish_to_kafka(messages):
    """Publish weather JSON messages to Kafka topic."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        pytest.skip("kafka-python not installed")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for msg in messages:
        producer.send(KAFKA_TOPIC, value=msg)
    producer.flush()
    producer.close()
    return len(messages)


def trigger_dag(dag_id):
    resp = requests.post(
        f"{AIRFLOW_API}/dags/{dag_id}/dagRuns",
        json={"conf": {}},
        auth=airflow_auth(),
        timeout=TIMEOUT,
    )
    assert resp.status_code in (200, 409), f"Failed to trigger DAG: {resp.status_code} {resp.text}"
    return resp.json().get("dag_run_id")


def wait_for_dag_completion(dag_id, run_id, max_wait=300, poll_interval=10):
    elapsed = 0
    while elapsed < max_wait:
        resp = requests.get(
            f"{AIRFLOW_API}/dags/{dag_id}/dagRuns/{run_id}",
            auth=airflow_auth(),
            timeout=TIMEOUT,
        )
        if resp.status_code == 200:
            state = resp.json().get("state")
            if state == "success":
                return "success"
            elif state in ("failed", "upstream_failed"):
                return state
        time.sleep(poll_interval)
        elapsed += poll_interval
    return "timeout"


@pytest.mark.e2e
class TestWeatherPipelineE2E:

    def test_publish_weather_to_kafka(self):
        """Step 1: Publish sample weather data to Kafka topic."""
        count = publish_to_kafka(SAMPLE_WEATHER_MESSAGES)
        assert count == 3, f"Expected 3 messages published, got {count}"

    def test_kafka_topic_has_messages(self):
        """Verify Kafka topic has messages."""
        try:
            from kafka import KafkaConsumer
        except ImportError:
            pytest.skip("kafka-python not installed")

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        messages = list(consumer)
        consumer.close()
        assert len(messages) >= 3, f"Expected >= 3 messages, got {len(messages)}"

        # Verify message structure
        for msg in messages[:3]:
            data = msg.value
            assert "name" in data, "Missing 'name' field in weather message"
            assert "main" in data, "Missing 'main' field in weather message"
            assert "weather" in data, "Missing 'weather' field in weather message"

    def test_trigger_weather_batch_etl(self):
        """Step 3: Trigger weather batch ETL DAG and wait for completion."""
        run_id = trigger_dag(DAG_ID)
        assert run_id is not None

        state = wait_for_dag_completion(DAG_ID, run_id)
        assert state == "success", f"Weather batch DAG ended with state: {state}"

    def test_weather_batch_tasks_succeeded(self):
        """Verify all weather batch DAG tasks succeeded."""
        resp = requests.get(
            f"{AIRFLOW_API}/dags/{DAG_ID}/dagRuns",
            params={"order_by": "-execution_date", "limit": 1},
            auth=airflow_auth(),
            timeout=TIMEOUT,
        )
        if resp.status_code != 200 or not resp.json().get("dag_runs"):
            pytest.skip("No weather batch DAG runs found")

        run_id = resp.json()["dag_runs"][0]["dag_run_id"]
        resp = requests.get(
            f"{AIRFLOW_API}/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances",
            auth=airflow_auth(),
            timeout=TIMEOUT,
        )
        assert resp.status_code == 200
        tasks = resp.json().get("task_instances", [])
        failed = [t["task_id"] for t in tasks if t["state"] != "success"]
        assert len(failed) == 0, f"Failed weather tasks: {failed}"

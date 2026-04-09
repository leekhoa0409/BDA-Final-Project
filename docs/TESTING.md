# Huong dan Testing - Weather Data Lakehouse

## Muc luc

1. [Tong quan](#1-tong-quan)
2. [Cai dat moi truong test](#2-cai-dat-moi-truong-test)
3. [Unit Tests](#3-unit-tests)
4. [E2E Tests](#4-e2e-tests)
5. [Chay test](#5-chay-test)
6. [Chi tiet tung test case](#6-chi-tiet-tung-test-case)
7. [Viet them test moi](#7-viet-them-test-moi)
8. [Xu ly test fail](#8-xu-ly-test-fail)

---

## 1. Tong quan

Project co 2 loai test:

| Loai | Thu muc | Can Docker? | Muc dich |
|------|---------|-------------|----------|
| **Unit Tests** | `tests/unit/` | Khong | Kiem tra logic ETL, DAG syntax |
| **E2E Tests** | `tests/e2e/` | Co | Kiem tra ket noi services, pipeline end-to-end |

### Cau truc thu muc

```
tests/
├── conftest.py                  # Fixtures chung (SparkSession, sample data, service URLs)
├── requirements.txt             # Dependencies cho test
├── unit/
│   ├── test_dag_integrity.py    # Kiem tra DAG syntax va config
│   └── test_weather_etl.py      # Kiem tra logic Silver/Gold transformations
└── e2e/
    ├── test_service_connectivity.py   # Kiem tra services dang chay
    └── test_weather_pipeline_e2e.py   # Kiem tra pipeline Kafka -> Bronze -> Silver -> Gold
```

### Cong cu su dung

| Tool | Muc dich |
|------|----------|
| **pytest** | Framework chay test |
| **chispa** | So sanh Spark DataFrames trong test |
| **pyspark** | Chay Spark local cho unit tests |
| **kafka-python** | Gui/doc message Kafka trong E2E tests |
| **minio** | Kiem tra buckets MinIO trong E2E tests |
| **requests** | Goi API cac services trong E2E tests |

---

## 2. Cai dat moi truong test

### 2.1 Tao virtual environment

```bash
cd /path/to/BDA-Final-Project

# Tao .venv (chi can lam 1 lan)
python3 -m venv .venv

# Kich hoat
source .venv/bin/activate
```

### 2.2 Cai dat dependencies

```bash
pip install -r tests/requirements.txt
```

### 2.3 Kiem tra cai dat thanh cong

```bash
# Kiem tra pytest
pytest --version

# Kiem tra PySpark
python3 -c "import pyspark; print(f'PySpark {pyspark.__version__}')"

# Kiem tra Delta
python3 -c "import delta; print(f'Delta {delta.__version__}')"
```

---

## 3. Unit Tests

Unit tests chay **hoan toan local**, khong can Docker. PySpark se khoi tao mot SparkSession local de test.

### 3.1 test_dag_integrity.py — Kiem tra Airflow DAGs

Kiem tra cac file DAG trong `airflow/dags/` co hop le khong:

| Test | Kiem tra gi |
|------|------------|
| `test_dag_folder_exists` | Thu muc `airflow/dags/` ton tai |
| `test_dag_files_exist` | Co it nhat 1 file `.py` trong thu muc |
| `test_dag_files_have_valid_syntax` | Tat ca DAG files compile duoc (khong loi syntax) |
| `test_all_dags_have_required_fields` | Moi DAG co `dag_id` va `start_date` |
| `test_no_hardcoded_credentials_in_dags` | Khong chua mat khau hardcode (admin123456, airflow123...) |
| `test_expected_dags_present` | 3 files bat buoc: `weather_pipeline_dag.py`, `weather_streaming_dag.py`, `common.py` |

### 3.2 test_weather_etl.py — Kiem tra logic ETL

Test logic transform du lieu Silver va Gold bang du lieu mau:

**Du lieu mau** (4 records):
- Hanoi: 32.5°C, 30.1°C (2 records)
- Ho Chi Minh City: 35.0°C (1 record)
- Da Nang: 100°C (1 record — invalid, se bi loc)

#### Silver Transformation Tests

| Test | Kiem tra gi |
|------|------------|
| `test_flattens_fields_correctly` | Silver co du 14 columns sau transform |
| `test_filters_invalid_temperature` | Nhiet do > 60°C bi loc bo (Da Nang 100°C) |
| `test_filters_invalid_humidity` | Do am > 100% bi loc bo |
| `test_null_city_filtered` | Records khong co ten thanh pho bi loc bo |
| `test_recorded_date_derived` | Cot `recorded_date` duoc tao tu unix timestamp |

#### Gold Aggregation Tests

| Test | Kiem tra gi |
|------|------------|
| `test_groups_by_city_and_date` | Group dung theo city + date (3 nhom) |
| `test_measurement_count_matches_silver` | Tong so do luong Gold = tong records Silver |
| `test_temperature_consistency` | min_temp <= avg_temp <= max_temp |
| `test_hanoi_aggregation_values` | Kiem tra gia tri cu the: Hanoi avg = 31.3°C tu (32.5 + 30.1)/2 |

---

## 4. E2E Tests

E2E tests **yeu cau Docker dang chay** (`docker compose up -d`). Chung kiem tra cac services thuc te.

### 4.1 test_service_connectivity.py — Kiem tra services

| Test | Service | Kiem tra gi |
|------|---------|------------|
| `test_minio_health` | MinIO | Endpoint `/minio/health/live` tra ve 200 |
| `test_minio_buckets_exist` | MinIO | Buckets `bronze`, `silver`, `gold` ton tai |
| `test_spark_master_alive` | Spark | Status = ALIVE qua `/json/` |
| `test_spark_workers_registered` | Spark | Co it nhat 1 worker dang ky |
| `test_airflow_health` | Airflow | Scheduler status = healthy |
| `test_trino_info` | Trino | `starting = false` (da san sang) |
| `test_superset_health` | Superset | Endpoint `/health` tra ve 200 |
| `test_kafka_connectivity` | Kafka | Ket noi duoc toi Kafka broker |
| `test_kafka_weather_topic_exists` | Kafka | Topic `weather-raw` da tao |
| `test_nifi_api_accessible` | NiFi | API endpoint phan hoi (HTTPS, self-signed cert) |

### 4.2 test_weather_pipeline_e2e.py — Kiem tra pipeline end-to-end

| Test | Kiem tra gi |
|------|------------|
| `test_publish_weather_to_kafka` | Gui 3 messages JSON vao Kafka topic `weather-raw` |
| `test_kafka_topic_has_messages` | Topic co >= 3 messages voi schema dung (name, main, weather) |
| `test_trigger_weather_batch_etl` | Trigger DAG qua Airflow API, doi hoan thanh (max 300s) |
| `test_weather_batch_tasks_succeeded` | Tat ca tasks trong DAG run deu thanh cong |

---

## 5. Chay test

### 5.1 Chay unit tests (khong can Docker)

```bash
source .venv/bin/activate

# Chay tat ca unit tests
pytest tests/unit/ -v

# Chay 1 file cu the
pytest tests/unit/test_weather_etl.py -v

# Chay 1 test case cu the
pytest tests/unit/test_weather_etl.py::TestWeatherSilverTransformation::test_filters_invalid_temperature -v
```

**Ket qua mong doi**:
```
tests/unit/test_dag_integrity.py::TestDagIntegrity::test_dag_folder_exists PASSED
tests/unit/test_dag_integrity.py::TestDagIntegrity::test_dag_files_exist PASSED
tests/unit/test_dag_integrity.py::TestDagIntegrity::test_dag_files_have_valid_syntax PASSED
...
tests/unit/test_weather_etl.py::TestWeatherGoldAggregation::test_temperature_consistency PASSED
```

### 5.2 Chay E2E tests (can Docker)

```bash
# Dam bao Docker dang chay
docker compose ps

# Chay tat ca E2E tests
pytest tests/e2e/ -v

# Chi chay connectivity tests
pytest tests/e2e/test_service_connectivity.py -v

# Chi chay pipeline tests
pytest tests/e2e/test_weather_pipeline_e2e.py -v
```

### 5.3 Chay tat ca tests

```bash
# Tat ca (unit + e2e)
pytest -v

# Chi unit (dung marker)
pytest -m unit -v

# Chi e2e (dung marker)
pytest -m e2e -v
```

### 5.4 Cac options huu ich

```bash
# Hien thi chi tiet khi fail
pytest tests/unit/ -v --tb=long

# Dung ngay khi co test fail
pytest tests/unit/ -v -x

# Chay tests co ten chua "temperature"
pytest tests/unit/ -v -k "temperature"

# Hien thi thoi gian chay tung test
pytest tests/unit/ -v --durations=10
```

---

## 6. Chi tiet tung test case

### 6.1 Fixtures (conftest.py)

Fixtures la cac "du lieu/doi tuong dung chung" giua cac tests:

| Fixture | Scope | Muc dich |
|---------|-------|----------|
| `spark` | session | SparkSession local voi Delta Lake (tao 1 lan, dung cho tat ca tests) |
| `tmp_delta_path` | function | Thu muc tam de ghi Delta tables (xoa sau moi test) |
| `sample_weather_json` | session | JSON mau theo schema OpenWeather API 3.0 |
| `service_urls` | session | Dict chua URLs cac Docker services (chi dung cho E2E) |

**Vi du su dung fixture trong test:**

```python
# test nhan fixture "spark" tu conftest.py
def test_example(spark):
    df = spark.createDataFrame([("New York", 25.0)], ["city", "temp"])
    assert df.count() == 1
```

### 6.2 Sample data trong test

**Unit test du lieu mau** (tao bang Spark DataFrame):
```
| city           | temp  | humidity | weather_main |
|----------------|-------|----------|--------------|
| Hanoi          | 32.5  | 75       | Clouds       |
| Hanoi          | 30.1  | 80       | Rain         |
| Ho Chi Minh    | 35.0  | 65       | Clear        |
| Da Nang        | 100.0 | 55       | Clear        |  <- invalid temp, se bi loc
```

**E2E test du lieu mau** (gui vao Kafka dang JSON):
```json
{
  "name": "Hanoi",
  "coord": {"lon": 105.8412, "lat": 21.0245},
  "main": {"temp": 32.5, "humidity": 75, "pressure": 1012},
  "weather": [{"main": "Clouds", "description": "scattered clouds"}],
  "wind": {"speed": 3.5, "deg": 180},
  "dt": 1700000000
}
```

---

## 7. Viet them test moi

### 7.1 Them unit test

Tao file moi trong `tests/unit/` hoac them test class/function vao file co san:

```python
# tests/unit/test_my_feature.py
import pytest

class TestMyFeature:
    def test_something(self, spark):
        """Mo ta test lam gi."""
        # Arrange — chuan bi du lieu
        data = [("New York", 25.0), ("New York", 30.0)]
        df = spark.createDataFrame(data, ["city", "temp"])

        # Act — thuc hien transform
        result = df.groupBy("city").avg("temp")

        # Assert — kiem tra ket qua
        row = result.collect()[0]
        assert abs(row["avg(temp)"] - 27.5) < 0.01
```

### 7.2 Them E2E test

```python
# tests/e2e/test_my_service.py
import pytest
import requests

@pytest.mark.e2e
class TestMyService:
    def test_service_responds(self, service_urls):
        """Kiem tra service co phan hoi."""
        resp = requests.get(f"{service_urls['minio_s3']}/minio/health/live", timeout=10)
        assert resp.status_code == 200
```

### 7.3 Them fixture moi

Them vao `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def my_fixture(spark):
    """Mo ta fixture."""
    df = spark.createDataFrame(...)
    return df
```

### 7.4 Quy tac dat ten

- File test: `test_<feature>.py`
- Class test: `Test<Feature>`
- Function test: `test_<hanh_vi_can_kiem_tra>`
- Fixture: ten mo ta ro muc dich (vd: `bronze_weather_df`, `sample_weather_json`)

---

## 8. Xu ly test fail

### 8.1 Unit test fail

**Loi thuong gap:**

#### `ModuleNotFoundError: No module named 'pyspark'`

```bash
# Kiem tra da activate .venv chua
source .venv/bin/activate
pip install -r tests/requirements.txt
```

#### `Java not found` hoac `JAVA_HOME not set`

PySpark can Java. Cai dat JDK:
```bash
# Ubuntu/Debian
sudo apt install default-jdk

# Mac
brew install openjdk

# Kiem tra
java -version
```

#### `test_dag_files_have_valid_syntax FAILED`

Co loi syntax trong file DAG. Doc output de biet file nao loi:
```bash
pytest tests/unit/test_dag_integrity.py::TestDagIntegrity::test_dag_files_have_valid_syntax -v --tb=long
```

#### `test_filters_invalid_temperature FAILED`

Logic loc nhiet do trong ETL bi thay doi. Kiem tra dieu kien filter trong `weather_etl.py`:
```python
# Dieu kien dung: nhiet do phai nam trong [-80, 60]
.filter((col("temperature") >= -80) & (col("temperature") <= 60))
```

### 8.2 E2E test fail

#### `ConnectionError` hoac `ConnectionRefusedError`

Service chua start hoac chua san sang:
```bash
# Kiem tra services
docker compose ps

# Doi services healthy
docker compose up -d
sleep 30
```

#### `test_kafka_weather_topic_exists FAILED`

Topic `weather-raw` chua duoc tao:
```bash
# Kiem tra kafka-init da chay
docker compose logs kafka-init

# Tao thu cong neu can
docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --topic weather-raw \
  --partitions 3 \
  --replication-factor 1
```

#### `test_minio_buckets_exist FAILED`

Buckets chua duoc tao:
```bash
# Kiem tra minio-init da chay
docker compose logs minio-init

# Tao thu cong neu can
docker exec minio mc alias set local http://localhost:9000 admin admin123456
docker exec minio mc mb local/bronze local/silver local/gold local/landing local/warehouse
```

#### `test_trigger_weather_batch_etl FAILED` (timeout)

Spark job chay qua lau hoac fail:
```bash
# Kiem tra Airflow logs
docker compose logs airflow-scheduler --tail 20

# Kiem tra Spark master co san sang
curl -s http://localhost:8080/json/ | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])"
```

### 8.3 Meo debug

```bash
# Chay 1 test voi output chi tiet
pytest tests/unit/test_weather_etl.py::TestWeatherSilverTransformation::test_filters_invalid_temperature -v --tb=long -s

# -s: hien thi print() output
# --tb=long: hien thi full traceback

# Chay test voi timeout (tranh treo)
pytest tests/e2e/ -v --timeout=60
```

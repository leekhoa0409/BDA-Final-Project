# Weather Data Lakehouse — New York City Analysis

A complete Data Lakehouse system on Docker implementing **Medallion Architecture** (Bronze - Silver - Gold) with Delta Lake. Ingests weather data from **Kaggle Historical CSV** (batch) and **OpenWeather API 2.5** (streaming) to analyze New York City weather patterns.

## System Architecture

```
                          +------------------------+
                          |    Apache Airflow       |
                          |    (Orchestration)      |
                          |    Port: 8082           |
                          +-----------|------------+
                                      | triggers spark-submit
                                      v
+--------------+          +------------------------+          +------------------------+
| Apache NiFi  |          |    Apache Spark         |          |       MinIO            |
| (Ingestion)  |--------->|    Master + Worker      |<-------->|  (S3-compatible        |
| Port: 8443   | Kafka    |    Port: 8080, 7077     |  Delta   |   Object Storage)      |
+--------------+          +------------------------+  Lake    |  Port: 9000, 9001      |
                                                      |       |                        |
+--------------+                 |         |          |       |  +------------------+  |
|   Kafka +    |                 |         |          |       |  | landing (CSV)    |  |
|  Zookeeper   |    +------------v---+     |          |       |  | bronze  (Raw)    |  |
| (Streaming)  |    | ML Components  |     |          |       |  | silver  (Clean)  |  |
| Port: 29092  |    | - MLflow (5000)|     |          |       |  | gold    (Agg)    |  |
+--------------+    | - FastAPI(8000)|     |          |       |  | warehouse (ML)   |  |
                    | - Redis (6379) |     |          |       |  +------------------+  |
                    +----------------+     |          |
                                                      |       |  +------------------+  |
                          +------------------------+  |       +-----------|------------+
                          |       Trino             |--+                   |
                          |   (SQL Query Engine)    |<--------------------+
                          |   Port: 8085            |   reads Delta tables
                          +-----------|------------+
                                      |
                                      v
                          +------------------------+
                          |  Apache Superset        |
                          |  (BI & Dashboards)      |
                          |  Port: 8088             |
                          +------------------------+

                    +---------------+  +------------------+
                    | PostgreSQL    |  |  PostgreSQL       |
                    | (Airflow)     |  |  (Superset)       |
                    | Port: 5432    |  |  Port: 5433       |
                    +---------------+  +------------------+
```

## Data Sources

| Source | Type | Coverage | Description |
|:-------|:-----|:---------|:------------|
| **Kaggle Historical** | Batch (CSV) | 36 cities, hourly 2012-2017 | [Historical Hourly Weather Data](https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data) |
| **OpenWeather API 2.5** | Streaming (JSON) | New York, every 5 min | [Current Weather Data](https://openweathermap.org/current) — free tier, 60 calls/min |

Pipeline focus: **New York City** weather analysis. Bronze ingests all 36 Kaggle cities (raw data principle), Silver filters for New York only.

## Medallion Architecture

| Layer | Path | Records | Description |
|:------|:-----|:--------|:------------|
| **Bronze** (batch) | `s3://bronze/weather_batch/` | 1,629,108 | Raw Kaggle CSV data (36 cities), unpivoted from wide to long format |
| **Bronze** (streaming) | `s3://bronze/weather_streaming/` | Growing | Raw OpenWeather API 2.5 JSON responses (New York) |
| **Silver** | `s3://silver/weather_clean/` | ~43,000+ | New York only, deduplicated, validated (temp, humidity ranges) |
| **Gold** daily | `s3://gold/weather_daily_stats/` | 1,854 | Daily aggregations: avg/min/max temp, humidity, wind, dominant weather |
| **Gold** monthly | `s3://gold/weather_monthly_stats/` | 61 | Monthly aggregations: seasonal patterns, rainy/clear day counts |
| **Gold** summary | `s3://gold/weather_city_summary/` | 1 | New York overview: latest conditions + historical averages |

All layers use **Delta Lake** format (ACID transactions, schema enforcement, time travel).

## Tech Stack

| Component | Technology | Version | Purpose |
|:----------|:-----------|:--------|:--------|
| Object Storage | MinIO | 2024-01-01 | S3-compatible data lake storage |
| Data Processing | Apache Spark | 3.5.3 | Distributed ETL engine |
| Table Format | Delta Lake | 3.1.0 | ACID transactions on data lake |
| Orchestration | Apache Airflow | 2.7.3 | Workflow scheduling & monitoring |
| SQL Query Engine | Trino | 435 | SQL queries on Delta tables |
| Visualization | Apache Superset | 3.1.0 | BI dashboards & data exploration |
| Data Ingestion | Apache NiFi | 1.24.0 | API data routing to Kafka |
| Event Streaming | Apache Kafka | 7.5.3 | Real-time message streaming |
| Metadata DB | PostgreSQL | 15 | Airflow & Superset metadata |
| Experiment Tracking | MLflow | 2.12.1 | ML model lifecycle management |
| Feature Store Cache | Redis | 7 | Online feature store / cache |
| Model Serving API | FastAPI | 0.109.0 | Real-time prediction API |

## Project Structure

```
.
├── docker-compose.yml                  # All 15 services orchestration
├── .env                                # Environment variables (not in git)
├── airflow/
│   ├── Dockerfile                      # Airflow image with Java + Spark
│   ├── requirements.txt                # Python dependencies
│   └── dags/
│       ├── common.py                   # Shared spark-submit config
│       ├── weather_pipeline_dag.py     # Hourly batch pipeline DAG
│       ├── weather_streaming_dag.py    # Streaming DAG (manual trigger)
│       └── ml_pipeline_dag.py          # Daily ML pipeline orchestration
├── spark/
│   ├── Dockerfile                      # Spark image with Delta + Kafka JARs
│   ├── spark-defaults.conf.template    # Config template (env var injection)
│   ├── entrypoint.sh                   # Runtime config substitution
│   └── jobs/
│       ├── config.py                   # Centralized paths & config
│       ├── batch_bronze_weather.py     # Kaggle CSV -> Bronze
│       ├── streaming_bronze_weather.py # Kafka -> Bronze (Spark Streaming)
│       ├── weather_etl.py              # Bronze -> Silver -> Gold (3 tables)
│       ├── feature_store.py            # Feature engineering pipeline
│       ├── ml_training.py              # Model training + MLflow logging
│       ├── validate_weather_bronze.py  # Bronze validation
│       ├── validate_weather_silver.py  # Silver validation
│       ├── validate_weather_gold.py    # Gold validation + cross-check
│       └── register_trino_tables.py    # Auto-register tables in Trino
├── model-serving/
│   ├── Dockerfile                      # FastAPI serving image
│   ├── main.py                         # Prediction API endpoints
│   └── requirements.txt                # Python dependencies
├── docs/
│   ├── GUIDE.md                        # Setup and configuration guide
│   └── ML_DOCS.md                      # Machine Learning architecture
├── superset/
│   ├── superset_config.py              # Superset configuration
│   └── setup_trino_connection.sh       # Create Trino datasource
├── trino/
│   └── catalog/
│       └── delta.properties            # Delta Lake connector config
├── nifi/
│   └── scripts/
│       └── create-weather-flow.py      # NiFi flow generator
├── kafka/
│   └── create-topics.sh                # Create weather-raw topic
├── minio/
│   └── init-buckets.sh                 # Create 5 buckets
├── data/
│   ├── README.md                       # Data source documentation
│   └── batch/                          # Kaggle CSV files (not in git)
│       ├── temperature.csv
│       ├── humidity.csv
│       ├── pressure.csv
│       ├── wind_speed.csv
│       ├── wind_direction.csv
│       ├── weather_description.csv
│       └── city_attributes.csv
├── tests/
│   ├── conftest.py                     # Pytest fixtures
│   ├── requirements.txt                # Test dependencies
│   ├── unit/                           # Unit tests (no Docker needed)
│   └── e2e/                            # E2E tests (Docker required)
└── scripts/
    └── run_tests.sh                    # Test runner
```

## Prerequisites

- **Docker** & **Docker Compose** installed
- **RAM:** Minimum 12-16 GB
- **Disk:** ~2 GB for Docker images + data
- **Kaggle dataset** downloaded (see [Data Setup](#step-2-download--upload-kaggle-data))
- **OpenWeather API key** (free at [openweathermap.org](https://openweathermap.org/api)) — optional, for streaming only

## Getting Started

### Step 1: Configure Environment

Create `.env` file in project root (copy from example):

```env
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123456
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=admin123456

# PostgreSQL - Airflow
POSTGRES_AIRFLOW_USER=airflow
POSTGRES_AIRFLOW_PASSWORD=airflow123
POSTGRES_AIRFLOW_DB=airflow

# PostgreSQL - Superset
POSTGRES_SUPERSET_USER=superset
POSTGRES_SUPERSET_PASSWORD=superset123
POSTGRES_SUPERSET_DB=superset

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__SECRET_KEY=lakehouse_secret_key_2024

# Superset
SUPERSET_SECRET_KEY=lakehouse_superset_secret_2024

# OpenWeather API (optional, for streaming)
OPENWEATHER_API_KEY=your_api_key_here
```

### Step 2: Download & Upload Kaggle Data

1. Download from [Kaggle](https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data)
2. Extract CSV files to `data/batch/`
3. Upload to MinIO after services start (Step 4)

### Step 3: Start All Services

```bash
docker compose up -d
```

Wait 1-2 minutes for all services to initialize. Check status:

```bash
docker compose ps
```

All services should show `Up` or `Up (healthy)`.

### Step 4: Upload Kaggle Data to MinIO

```bash
# Copy files to Spark container and upload via S3 API
for f in data/batch/*.csv; do
    docker cp "$f" spark-master:/tmp/$(basename $f)
done

docker exec -u root spark-master pip3 install boto3 -q

docker exec spark-master python3 -c "
import boto3, os
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='admin', aws_secret_access_key='admin123456',
    region_name='us-east-1')
for f in ['city_attributes.csv','humidity.csv','pressure.csv',
          'temperature.csv','weather_description.csv',
          'wind_direction.csv','wind_speed.csv']:
    s3.upload_file('/tmp/' + f, 'landing', 'weather/' + f)
    print(f'Uploaded {f}')
"
```

### Step 5: Run Batch Pipeline

```bash
# 1. Ingest Kaggle CSV to Bronze
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/batch_bronze_weather.py

# 2. Run ETL: Bronze -> Silver (New York) -> Gold (3 tables)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/weather_etl.py

# 3. Register tables in Trino
docker exec spark-master python3 /opt/spark/jobs/register_trino_tables.py
```

### Step 6: Verify via Trino

```bash
# Query Gold daily stats
docker exec trino trino --execute \
  "SELECT recorded_date, avg_temperature, dominant_weather_condition
   FROM delta.default.gold_weather_daily_stats
   ORDER BY recorded_date DESC LIMIT 10"

# Query Gold monthly stats
docker exec trino trino --execute \
  "SELECT year_month, avg_temperature, rainy_day_count, clear_day_count
   FROM delta.default.gold_weather_monthly_stats
   ORDER BY year_month DESC LIMIT 12"

# Query city summary
docker exec trino trino --execute \
  "SELECT * FROM delta.default.gold_weather_city_summary"
```

### Step 7: Setup Superset

```bash
# Create admin user (if not exists)
docker exec superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@lakehouse.local --password admin

# Create Trino database connection
bash superset/setup_trino_connection.sh
```

Open **http://localhost:8088** -> Login `admin` / `admin` -> **SQL Lab** -> Database: "Trino Lakehouse"

Example queries:
```sql
-- Daily temperature trend
SELECT recorded_date, avg_temperature, min_temperature, max_temperature
FROM gold_weather_daily_stats
ORDER BY recorded_date;

-- Monthly seasonal patterns
SELECT year_month, avg_temperature, rainy_day_count, clear_day_count
FROM gold_weather_monthly_stats
ORDER BY year_month;
```

### Step 8: Setup Streaming (Optional)

```bash
# Create NiFi flow for OpenWeather API
python3 nifi/scripts/create-weather-flow.py \
  --nifi-url https://localhost:8443 \
  --api-key YOUR_OPENWEATHER_API_KEY
```

Then:
1. Open **https://localhost:8443/nifi/** -> Login `admin` / `admin123456789`
2. Go to process group "OpenWeather One Call to Kafka"
3. Select all processors (Ctrl+A) -> Click Start

```bash
# Start Spark Streaming (reads from Kafka -> Bronze)
docker exec airflow-webserver airflow dags unpause weather_streaming
docker exec airflow-webserver airflow dags trigger weather_streaming
```

### Step 9: Run Machine Learning Pipeline

```bash
# 1. Create Feature Store
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/jobs/feature_store.py

# 2. Train Model
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/jobs/ml_training.py

# 3. Transition Model to Production
curl -X POST "http://localhost:8000/models/1/transition?stage=Production"

# 4. Restart Model Serving (to load the new model)
docker restart model-serving

# 5. Predict
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "temperature": 25.0,
    "humidity": 60.0,
    "pressure": 1013.0,
    "wind_speed": 3.5,
    "hour": 14,
    "day_of_week": 3,
    "month": 4
  }'
```

### Step 10: Enable Automated Pipelines

```bash
# Unpause hourly batch pipeline
docker exec airflow-webserver airflow dags unpause weather_pipeline

# Unpause daily ML pipeline
docker exec airflow-webserver airflow dags unpause ml_pipeline
```

The `weather_pipeline` DAG runs `@hourly`:
1. Checks landing zone for new CSV files
2. Ingests new CSV if found
3. Runs ETL (Silver + Gold)
4. Validates all layers
5. Registers Trino tables

## Services & Access

| Service | URL | Credentials |
|:--------|:----|:------------|
| **Airflow** | http://localhost:8082 | `airflow` / `airflow` |
| **MinIO Console** | http://localhost:9001 | `admin` / `admin123456` |
| **Superset** | http://localhost:8088 | `admin` / `admin` |
| **Spark Master** | http://localhost:8080 | — |
| **Trino** | http://localhost:8085 | `trino` |
| **NiFi** | https://localhost:8443 | `admin` / `admin123456789` |
| **MLflow UI** | http://localhost:5000 | — |
| **Model API Docs** | http://localhost:8000/docs | — |

## Pipeline Flow

```
[Kaggle CSV] -> MinIO landing -> batch_bronze_weather.py -> Bronze (all 36 cities)
[OpenWeather] -> NiFi -> Kafka -> streaming_bronze_weather.py -> Bronze (New York)
                                                                     |
                                        weather_etl.py: Filter "New York" + MERGE
                                                                     |
                                                              Silver (clean)
                                                                     |
                                    +------------------+-------------+-------------+
                                    |                  |                           |
                             Gold Daily          Gold Monthly              Gold City Summary
                                    |                  |                           |
                                    +------------------+-------------+-------------+
                                                                     |
                               +-------------------------------------+-----------------------------------+
                               |                                                                         |
                    register_trino_tables.py                                                      feature_store.py
                               |                                                                         |
                       Trino -> Superset                                                        Feature Store (Delta)
                                                                                                         |
                                                                                                   ml_training.py
                                                                                                         |
                                                                                      MLflow Registry (Model) + MinIO (Scaler)
                                                                                                         |
                                                                                             model-serving API (FastAPI)
```

## Testing

```bash
# Install test dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r tests/requirements.txt

# Run unit tests (no Docker needed)
pytest tests/unit/ -v

# Run E2E tests (Docker services must be running)
pytest tests/e2e/ -v

# Run all tests
bash scripts/run_tests.sh
```
## Common Commands

```bash
# Start/stop services
docker compose up -d
docker compose down

# Rebuild after code changes
docker compose up -d --build spark-master spark-worker

# View service logs
docker compose logs -f spark-master
docker compose logs -f airflow-webserver

# Run ETL manually
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/weather_etl.py

# Trigger Airflow DAG
docker exec airflow-webserver airflow dags trigger weather_pipeline

# Query via Trino CLI
docker exec -it trino trino
```

## Notes

- Spark config uses **template substitution** (`entrypoint.sh` + `envsubst`) — credentials injected at runtime from environment variables
- Trino uses `${ENV:MINIO_ACCESS_KEY}` for credential injection
- Superset security features toggle via `SUPERSET_ENV` variable (disabled in development)
- Kaggle temperature data is in **Kelvin** — converted to Celsius during ETL
- All services communicate on Docker bridge network: `lakehouse-network`

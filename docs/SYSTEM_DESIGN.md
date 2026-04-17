# Diễn Giải Chi Tiết: Thiết Kế & Vận Hành Hệ Thống Weather Data Lakehouse

## 1. Tổng Quan Kiến Trúc

Hệ thống là một **Data Lakehouse** hoàn chỉnh, được thiết kế theo **Medallion Architecture** (Bronze → Silver → Gold) trên nền tảng Delta Lake, phục vụ phân tích dữ liệu thời tiết thành phố New York. Toàn bộ 20 service được container hóa bằng Docker và giao tiếp qua mạng bridge chung `lakehouse-network`.

Hệ thống xử lý **hai luồng dữ liệu song song**:

- **Batch**: Dữ liệu lịch sử từ Kaggle (2012–2017, 36 thành phố, ~1.6 triệu bản ghi)
- **Streaming**: Dữ liệu thời gian thực từ OpenWeather API 2.5 (New York, mỗi 5 phút)

### Sơ Đồ Luồng Dữ Liệu End-to-End

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│  Kaggle CSV (batch)          OpenWeather API 2.5 (streaming)    │
└──────┬──────────────────────────────────┬───────────────────────┘
       │                                  │
       ▼                                  ▼
   [Landing Zone]                     [NiFi → Kafka]
   MinIO: landing/                    Topic: weather-raw
       │                                  │
       ▼                                  ▼
   [Spark Batch]                    [Spark Streaming]
   batch_bronze_weather.py          streaming_bronze_weather.py
       │                                  │
       ▼                                  ▼
   ┌──────────────── BRONZE ─────────────────┐
   │  bronze/weather_batch (Delta)           │
   │  bronze/weather_streaming (Delta)       │
   └──────────────────┬──────────────────────┘
                      │ weather_etl.py
                      ▼
   ┌──────────────── SILVER ─────────────────┐
   │  silver/weather_clean (Delta)           │
   │  Deduplicated, validated, cleaned       │
   └──────────────────┬──────────────────────┘
                      │
              ┌───────┴────────┐
              ▼                ▼
   ┌──── GOLD Facts ────┐  ┌── GOLD Dims ──┐
   │ daily_stats         │  │ dim_city       │
   │ monthly_stats       │  │ dim_date       │
   │ city_summary        │  └────────────────┘
   └─────────┬──────────┘
             │
     ┌───────┼────────────┐
     ▼       ▼            ▼
  [Trino]  [Feature    [Superset]
   SQL      Store]      Dashboard
   Engine     │
              ▼
         [ML Training]
         MLflow Registry
              │
              ▼
       [Model Serving]
       FastAPI + Redis
```

---

## 2. Tầng Lưu Trữ (Storage Layer)

### MinIO — Object Storage Tương Thích S3

- **Vai trò**: Thay thế AWS S3 trong môi trường on-premise, lưu trữ toàn bộ dữ liệu của Lakehouse.
- **Thiết kế**: Service `minio` khởi động trước với healthcheck (`mc ready local`), sau đó `minio-init` chạy script `minio/init-buckets.sh` để tạo 5 bucket:

| Bucket | Mục đích |
|--------|----------|
| `landing` | Vùng đệm cho CSV thô upload từ Kaggle |
| `bronze` | Dữ liệu thô đã chuẩn hóa (raw but structured) |
| `silver` | Dữ liệu đã làm sạch, loại bỏ trùng lặp |
| `gold` | Dữ liệu tổng hợp theo mô hình Star Schema |
| `warehouse` | ML artifacts, Feature Store, Trino metastore |

**Lý do thiết kế**: Sử dụng MinIO giúp team phát triển local mà không cần tài khoản AWS, đồng thời giữ nguyên tính tương thích S3A protocol để các service (Spark, MLflow, Trino) giao tiếp bằng cùng SDK `boto3`/`hadoop-aws`.

---

## 3. Tầng Ingestion (Thu Thập Dữ Liệu)

### 3.1. Luồng Batch: CSV → Landing → Bronze

Quy trình được Airflow orchestrate qua DAG `weather_pipeline_dag.py`, chạy **mỗi giờ**:

1. **`check_csv_in_landing`**: Dùng `boto3` kiểm tra bucket `landing/weather/` có file CSV mới không. Nếu có → trigger ingest; nếu không → skip.
2. **`ingest_csv_to_bronze`**: Spark job `batch_bronze_weather.py` đọc 7 file CSV từ Kaggle.

**Xử lý dữ liệu Kaggle**:

Dữ liệu Kaggle ở dạng **wide format** — mỗi file là 1 thuộc tính thời tiết, cột = thành phố, hàng = thời gian. Job sử dụng `stack()` để **unpivot** sang **long format** (city, datetime, value), rồi JOIN 6 thuộc tính (temperature, humidity, pressure, wind_speed, wind_direction, weather_description). Nhiệt độ được convert từ Kelvin → Celsius. File CSV đã xử lý được move sang `landing/weather/processed/` để tránh ingest lại.

### 3.2. Luồng Streaming: API → NiFi → Kafka → Bronze

#### Apache NiFi (port 8443) — Data Ingestion

Được cấu hình qua flow `nifi/scripts/create-weather-flow.py` với 4 processor:

1. **GenerateFlowFile** — Trigger mỗi 5 phút
2. **InvokeHTTP** — GET request đến OpenWeather API 2.5 cho New York
3. **PublishKafka_2_6** — Đẩy JSON response vào Kafka topic `weather-raw`
4. **LogAttribute** — Ghi log lỗi

#### Apache Kafka (port 9092) — Message Broker

Topic `weather-raw` được tạo bởi `kafka/create-topics.sh` với 3 partition, retention 7 ngày. Kafka hoạt động như **buffer** giữa ingestion và processing, đảm bảo dữ liệu không mất khi Spark consumer tạm dừng.

#### Spark Structured Streaming — Consumer

Job `streaming_bronze_weather.py` đọc từ Kafka, parse JSON theo schema của OpenWeather API 2.5 (nested: `main.temp`, `wind.speed`, `weather[0].description`...), flatten thành flat columns, ghi vào Bronze Delta table ở chế độ **append-only** với checkpoint. Job này chạy **vô thời hạn** (`awaitTermination()`).

#### Airflow Supervisor Pattern

DAG `weather_streaming_dag.py` chạy mỗi 30 phút, query Spark Master REST API (`http://spark-master:8080/json/`) để kiểm tra streaming job còn sống không. Nếu đã crash → khởi động lại. Đây là **supervisor pattern** đảm bảo high availability mà không cần Kubernetes.

---

## 4. Tầng Xử Lý (Processing Layer)

### Apache Spark — Compute Engine

Spark cluster gồm 1 master + 1 worker, build từ `spark/Dockerfile` tùy chỉnh với đầy đủ JAR: Delta Lake, Hadoop-AWS, Kafka client. Worker được cấp **4 core, 4GB RAM**.

### ETL Pipeline: Bronze → Silver → Gold

Job `weather_etl.py` thực hiện biến đổi qua 3 bước:

#### Bước 1 — Silver (Làm Sạch)

- Union dữ liệu batch + streaming từ Bronze
- Deduplicate trên (city, recorded_at) — giữ bản ghi mới nhất
- Filter dữ liệu bất thường:
  - Nhiệt độ ngoài khoảng -80°C ~ 60°C
  - Humidity ngoài 0-100%
  - City null
- Kết quả: ~43,000+ bản ghi sạch

#### Bước 2 — Gold Dimensions (Bảng Chiều)

| Bảng | Nội dung |
|------|----------|
| `dim_city` | Metadata 36 thành phố: tên, quốc gia, tọa độ |
| `dim_date` | Thuộc tính ngày: year, month, quarter, week, day_of_week |

#### Bước 3 — Gold Facts (Bảng Sự Kiện)

| Bảng | Mô tả | Partition |
|------|--------|-----------|
| `fact_weather_daily_stats` | Tổng hợp theo ngày — avg/min/max temp, humidity, pressure, wind speed, số lần đo, điều kiện thời tiết phổ biến nhất | year/month |
| `fact_weather_monthly_stats` | Tổng hợp theo tháng — bao gồm số ngày mưa/nắng | — |
| `fact_weather_city_summary` | 1 row/thành phố — điều kiện mới nhất + trung bình 5 năm | — |

**Lý do chọn Star Schema**: Tối ưu cho analytical queries trong Superset/Trino. Bảng fact chứa metrics, bảng dim chứa context — cho phép JOIN linh hoạt khi phân tích.

### Data Quality Validation

Ba job validation chạy sau mỗi bước ETL:

| Job | Kiểm tra |
|-----|----------|
| `validate_weather_bronze.py` | Row count > 0, null check trên key fields, data freshness |
| `validate_weather_silver.py` | Range hợp lệ (temp, humidity), required columns, no null cities |
| `validate_weather_gold.py` | Cross-validate measurement_count giữa Gold và Silver, kiểm tra min ≤ avg ≤ max |

---

## 5. Tầng Query (SQL Engine)

### Trino — Distributed SQL Engine

Trino (port 8085) được cấu hình với **Delta Lake connector** qua `trino/catalog/delta.properties`:

- Sử dụng file-based Hive metastore (lưu trên MinIO tại `s3://warehouse/metastore`)
- S3A path-style access (bắt buộc cho MinIO)
- `register-table-procedure.enabled=true` — cho phép đăng ký Delta table từ S3 path

Job `register_trino_tables.py` gọi Trino REST API để đăng ký 8 Delta table:

```
bronze_weather_streaming, bronze_weather_batch
silver_weather_clean
dim_city, dim_date
fact_weather_daily_stats, fact_weather_monthly_stats, fact_weather_city_summary
```

**Lý do chọn Trino**: Cho phép Superset và analyst query trực tiếp trên Delta Lake bằng SQL chuẩn, không cần Spark session. Trino đóng vai trò **serving layer** giữa storage và BI.

---

## 6. Tầng ML & MLOps

### 6.1. Feature Store

Job `feature_store.py` tạo 3 feature set:

| Feature Set | Nội dung |
|-------------|----------|
| **Hourly features** | Lag features (temp_lag_1h/3h/6h/12h/24h), moving averages (24h, 168h), standard deviation |
| **Daily features** | Lag ngày/tuần/2 tuần, moving stats 7 ngày, 30 ngày |
| **Training data** | Kết hợp + tạo label targets (temp sau 6h, 12h, 24h) |

**Redis** (port 6379): Cache features cho real-time prediction, được sync từ Delta bởi `sync_redis_features.py`.

### 6.2. ML Training

Job `ml_training.py` train 3 model:

| Model | Hyperparameters |
|-------|-----------------|
| **RandomForest** | n_estimators=100, max_depth=10, min_samples_split=5 |
| **GradientBoosting** | n_estimators=100, max_depth=5, learning_rate=0.1 |
| **Ridge Regression** | alpha=1.0 |

Model tốt nhất (theo R²) được đăng ký vào **MLflow Model Registry** với stage `Production`. Scaler và feature columns được lưu vào MinIO để model-serving tải lại.

### 6.3. MLflow — Experiment Tracking

MLflow (port 5000) dùng SQLite backend + MinIO artifact store tại `s3://warehouse/mlflow/`. Log params, metrics, model artifacts cho mỗi training run.

### 6.4. Model Serving — FastAPI

Service `model-serving/main.py` cung cấp:

| Endpoint | Mô tả |
|----------|--------|
| `POST /predict` | Dự báo nhiệt độ 24h tới từ input features |
| `POST /predict/batch` | Batch prediction |
| `GET /models/info` | Thông tin model đang serve |
| `POST /models/{version}/transition` | Chuyển model stage (Staging → Production) |
| `GET /health` | Service & model status |
| `GET /metrics` | Prometheus metrics |

**Prometheus metrics**: `prediction_requests_total` (counter), `prediction_latency_seconds` (histogram).

---

## 7. Tầng Orchestration

### Apache Airflow — Workflow Scheduler

3 DAG chính:

| DAG | Schedule | Mục đích |
|-----|----------|----------|
| `weather_pipeline_dag` | `@hourly` | Batch ETL: Landing → Bronze → Silver → Gold → Trino |
| `weather_streaming_dag` | `*/30 * * * *` | Supervisor: Giám sát & khởi động lại Spark Streaming |
| `ml_pipeline_dag` | `0 2 * * *` | ML: Feature Store → Train → Register → Serve |

Airflow dùng **PostgreSQL** làm metadata store, **SequentialExecutor** (đủ cho môi trường dev). Init service tự động chạy `db migrate` + tạo admin user.

**Thiết kế dependency chain trong batch DAG**:

```
check_csv → [ingest / skip] → validate_bronze → ETL → [validate_silver ∥ validate_gold] → register_trino
```

Sử dụng `trigger_rule='none_failed_min_one_success'` để xử lý đúng cả khi skip ingest (không có CSV mới).

---

## 8. Tầng BI & Visualization

### Apache Superset

Superset (port 8088) kết nối Trino qua SQLAlchemy URI `trino://trino@trino:8080/delta/default`.

Cấu hình chính (`superset/superset_config.py`):

- Native filters + cross filters enabled
- Cache 300s với SimpleCache
- Row limit 5,000, SQL max 100,000
- Cài thêm `trino` + `sqlalchemy-trino` driver lúc startup

Script `superset/setup_trino_connection.sh` tự động tạo datasource "Trino Lakehouse" qua Superset REST API.

---

## 9. Tầng Monitoring

### Prometheus + Grafana + cAdvisor

| Service | Vai trò |
|---------|---------|
| **cAdvisor** (port 8083) | Thu thập metrics từ Docker daemon — CPU, memory, network, disk I/O của mỗi container |
| **Prometheus** (port 9090) | Scrape metrics từ cAdvisor + chính nó mỗi 15 giây |
| **Grafana** (port 3000) | Dashboard visualization, kết nối Prometheus làm datasource |

Model-serving cũng expose `/metrics` endpoint cho Prometheus scrape (prediction count, latency).

---

## 10. Medallion Architecture — Tổng Kết Dữ Liệu

| Layer | Path | Records | Format | Mô tả |
|-------|------|---------|--------|-------|
| **Landing** | `s3a://landing/` | — | CSV | Raw Kaggle CSV files |
| **Bronze Batch** | `s3a://bronze/weather_batch/` | ~1,629,108 | Delta | Kaggle data, unpivoted, 36 cities |
| **Bronze Streaming** | `s3a://bronze/weather_streaming/` | Growing | Delta | OpenWeather API JSON, flattened |
| **Silver** | `s3a://silver/weather_clean/` | ~43,000+ | Delta | Deduplicated, validated |
| **Gold Daily** | `s3a://gold/fact_weather_daily_stats/` | ~1,854 | Delta | Tổng hợp theo ngày |
| **Gold Monthly** | `s3a://gold/fact_weather_monthly_stats/` | ~61 | Delta | Tổng hợp theo tháng |
| **Gold Summary** | `s3a://gold/fact_weather_city_summary/` | 1 | Delta | Tổng quan thành phố |
| **Dimensions** | `s3a://gold/dim_city/`, `dim_date/` | 36, 365 | Delta | City metadata, date attributes |
| **Features** | `s3a://warehouse/features/` | Varies | Delta | ML feature sets |
| **MLflow** | `s3a://warehouse/mlflow/` | — | Parquet | Model artifacts |

---

## 11. Service Ports & Credentials

| Service | URL | Port | Credentials |
|---------|-----|------|-------------|
| Airflow | http://localhost:8082 | 8082 | airflow / airflow |
| MinIO Console | http://localhost:9001 | 9001 | admin / admin123456 |
| Superset | http://localhost:8088 | 8088 | admin / admin |
| Spark Master UI | http://localhost:8080 | 8080 | — |
| Spark Worker UI | http://localhost:8081 | 8081 | — |
| Trino | http://localhost:8085 | 8085 | trino |
| NiFi | https://localhost:8443 | 8443 | admin / admin123456789 |
| MLflow | http://localhost:5000 | 5000 | — |
| Model Serving | http://localhost:8000 | 8000 | — (docs: /docs) |
| Prometheus | http://localhost:9090 | 9090 | — |
| Grafana | http://localhost:3000 | 3000 | admin / admin |
| Kafka (external) | localhost:29092 | 29092 | — |
| PostgreSQL (Airflow) | localhost:5432 | 5432 | airflow / airflow123 |
| PostgreSQL (Superset) | localhost:5433 | 5433 | superset / superset123 |
| Redis | localhost:6379 | 6379 | — |

---

## 12. Điểm Nổi Bật Trong Thiết Kế

### 12.1. Dependency Ordering

`depends_on` kết hợp `condition` (service_healthy, service_completed_successfully) đảm bảo thứ tự khởi động đúng:

- MinIO phải healthy → mới chạy minio-init tạo bucket
- Bucket phải xong → Trino mới start
- PostgreSQL phải healthy → Airflow mới init database
- Airflow init xong → webserver và scheduler mới khởi động

### 12.2. Init Containers Tách Biệt

`minio-init`, `kafka-init`, `airflow-init` chạy một lần rồi thoát — tách setup logic ra khỏi runtime service, giữ image chính sạch và idempotent.

### 12.3. Supervisor Pattern Cho Streaming

Thay vì dùng Kubernetes restart policy, Airflow DAG mỗi 30 phút query Spark REST API để phát hiện streaming job crash và tự khởi động lại. Đảm bảo high availability trong môi trường Docker thuần.

### 12.4. Volume Strategy

- **Named volumes** (minio_data, kafka_data, postgres_*): Dữ liệu persistent qua restart
- **Bind mounts** (dags, jobs, catalog): Code/config mount từ host — cho phép hot-reload khi develop

### 12.5. Biến Môi Trường Tập Trung

Tất cả credentials và endpoints nằm trong file `.env`, inject vào containers qua `${VAR}` syntax — dễ thay đổi khi deploy sang môi trường khác mà không sửa docker-compose.

### 12.6. Network Isolation

Tất cả service nằm trong 1 bridge network `lakehouse-network`. Service giao tiếp qua hostname (kafka, minio, trino...), host chỉ expose port cần thiết cho development access.

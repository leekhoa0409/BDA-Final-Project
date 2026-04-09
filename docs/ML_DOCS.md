# Machine Learning Pipeline - Tài liệu kỹ thuật

## 1. Tổng quan kiến trúc ML

Hệ thống ML được tích hợp trực tiếp vào kiến trúc Data Lakehouse, tận dụng dữ liệu thời tiết đã qua xử lý ở tầng Silver/Gold để huấn luyện mô hình dự báo nhiệt độ.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AIRFLOW ORCHESTRATION                        │
│                    (ml_pipeline DAG - 2:00 AM daily)                │
└──────────┬──────────────┬───────────────┬──────────────┬────────────┘
           │              │               │              │
           v              v               v              v
   ┌──────────────┐ ┌───────────┐ ┌────────────┐ ┌─────────────┐
   │ Check Feature│ │  Feature  │ │    Model   │ │   Verify &  │
   │  Store Data  │ │  Store    │ │  Training  │ │   Deploy    │
   │  (MinIO)     │ │  Update   │ │  (MLflow)  │ │  (Registry) │
   └──────────────┘ └─────┬─────┘ └──────┬─────┘ └──────┬──────┘
                          │              │               │
                          v              v               v
              ┌───────────────────────────────────────────────────┐
              │                    STORAGE LAYER                   │
              │  ┌─────────┐  ┌──────────┐  ┌──────────────────┐ │
              │  │ Delta    │  │  MLflow   │  │     MinIO        │ │
              │  │ Feature  │  │  Tracking │  │  (Model Artifacts│ │
              │  │ Store    │  │  Server   │  │   + Scaler)      │ │
              │  │ (MinIO)  │  │  (SQLite) │  │                  │ │
              │  └─────────┘  └──────────┘  └──────────────────┘ │
              └───────────────────────────────────────────────────┘
                                    │
                                    v
              ┌───────────────────────────────────────────────────┐
              │               MODEL SERVING LAYER                  │
              │  ┌────────────┐  ┌────────┐  ┌────────────────┐  │
              │  │  FastAPI    │  │ Redis  │  │  Prometheus    │  │
              │  │  (Port 8000)│  │ Cache  │  │  Metrics       │  │
              │  └────────────┘  └────────┘  └────────────────┘  │
              └───────────────────────────────────────────────────┘
```

---

## 2. Các thành phần chính

### 2.1 Feature Store (`spark/jobs/feature_store.py`)

Chuyển đổi dữ liệu thô từ tầng Silver/Gold thành các đặc trưng (features) phù hợp cho ML.

**Input:**
- `s3a://silver/weather_clean` (dữ liệu theo giờ)
- `s3a://gold/weather_daily_stats` (dữ liệu tổng hợp theo ngày)

**Output:** Lưu vào `s3a://warehouse/features/` dưới format Delta Lake.

#### Ba loại features được tạo:

| Feature Set       | Mô tả                                      | Nguồn dữ liệu | Số lượng features chính |
|-------------------|---------------------------------------------|----------------|------------------------|
| `hourly_weather`  | Features theo giờ: lag, moving average, temporal | Silver         | ~19 features           |
| `daily_weather`   | Features theo ngày: MA 7/30 ngày, stddev        | Gold           | ~17 features           |
| `training_data`   | Dataset huấn luyện có label (target variable)    | Silver         | ~17 features + labels  |

#### Chi tiết Feature Engineering:

**Temporal Features:**
- `hour`, `day_of_week`, `month`, `week_of_year` — mã hóa thời gian

**Lag Features (độ trễ):**
- `temp_lag_1h`, `temp_lag_3h`, `temp_lag_6h`, `temp_lag_12h`, `temp_lag_24h`
- `humid_lag_1h`, `pres_lag_1h`

**Moving Averages / Statistics:**
- `temp_24h_ma` — Trung bình trượt 24 giờ
- `temp_168h_ma` — Trung bình trượt 7 ngày
- `temp_24h_std` — Độ lệch chuẩn 24 giờ
- `temp_7d_ma`, `temp_7d_std`, `temp_30d_ma` (cho daily features)

**Target Variables (Biến mục tiêu):**
- `label_24h` — Nhiệt độ sau 24 giờ (target chính)
- `label_6h`, `label_12h` — Nhiệt độ sau 6h và 12h

---

### 2.2 Model Training (`spark/jobs/ml_training.py`)

Huấn luyện và so sánh nhiều mô hình ML, tự động ghi log lên MLflow.

#### Pipeline huấn luyện:

```
Load training_data (Delta) → Feature Engineering → Train/Test Split (80/20)
    → StandardScaler → Train 3 Models → Log to MLflow → Register Best Model
```

#### Các mô hình được huấn luyện:

| Model              | Hyperparameters                                              | Mục đích          |
|--------------------|--------------------------------------------------------------|--------------------|
| **RandomForest**   | `n_estimators=100`, `max_depth=10`, `min_samples_split=5`    | Ensemble cơ bản    |
| **GradientBoosting** | `n_estimators=100`, `max_depth=5`, `learning_rate=0.1`     | Ensemble nâng cao  |
| **Ridge**          | `alpha=1.0`                                                  | Linear baseline    |

#### Metrics đánh giá:
- **MAE** (Mean Absolute Error) — Sai số tuyệt đối trung bình
- **RMSE** (Root Mean Squared Error) — Căn bậc hai sai số bình phương trung bình
- **R²** (R-squared) — Hệ số xác định → dùng để chọn best model

#### MLflow Integration:
- **Tracking URI:** `http://mlflow:5000`
- **Experiment:** `weather-forecasting`
- Tự động log: params, metrics, model artifacts
- Mô hình tốt nhất (theo R²) được đăng ký vào **Model Registry** với tên `weather-forecast-model`
- Scaler + feature_cols được lưu vào MinIO tại `s3://warehouse/models/scaler.joblib`

---

### 2.3 Model Serving (`model-serving/main.py`)

API server dùng FastAPI phục vụ dự báo real-time.

#### Endpoints:

| Method | Endpoint                        | Chức năng                          |
|--------|---------------------------------|------------------------------------|
| GET    | `/health`                       | Kiểm tra trạng thái server & model |
| GET    | `/metrics`                      | Prometheus metrics                 |
| POST   | `/predict`                      | Dự báo nhiệt độ 24h tới           |
| POST   | `/predict/batch`                | Dự báo hàng loạt                   |
| GET    | `/models/info`                  | Thông tin model trong registry     |
| POST   | `/models/{version}/transition`  | Chuyển stage model (Staging → Production) |

#### Request/Response Format:

**POST `/predict`:**
```json
// Request
{
  "temperature": 25.0,
  "humidity": 60.0,
  "pressure": 1013.0,
  "wind_speed": 3.5,
  "hour": 14,
  "day_of_week": 3,
  "month": 4,
  "temp_lag_1h": 24.5,      // optional
  "temp_lag_3h": 23.0,      // optional
  "temp_lag_6h": 21.0,      // optional
  "temp_lag_24h": 22.0,     // optional
  "temp_24h_ma": 23.5,      // optional
  "condition_encoded": 1    // optional
}

// Response
{
  "predicted_temperature_24h": 26.35,
  "confidence_lower": 25.62,
  "confidence_upper": 27.08,
  "model_version": "3",
  "timestamp": "2026-04-08T21:00:00"
}
```

#### Kiến trúc runtime:

- **Model Loading:** Tải model từ MLflow Registry (`models:/weather-forecast-model/Production`) khi startup
- **Scaler:** Tải `scaler.joblib` từ MinIO để chuẩn hóa features
- **Redis Cache:** Lấy features mới nhất từ online feature store (`feature:hourly_weather:latest`)
- **Fallback Mode:** Nếu model chưa được train, trả về dự báo đơn giản dựa trên nhiệt độ hiện tại
- **Prometheus Metrics:** `prediction_requests_total`, `prediction_latency_seconds`

---

### 2.4 Airflow DAG (`airflow/dags/ml_pipeline_dag.py`)

Orchestration toàn bộ ML pipeline.

**Schedule:** Chạy hàng ngày lúc 2:00 AM (`0 2 * * *`)

#### Task Flow:

```
check_feature_store_data → update_feature_store → train_ml_model
    → verify_model_registration → reload_model_in_serving
```

| Task                        | Mô tả                                                         |
|-----------------------------|---------------------------------------------------------------|
| `check_feature_store_data`  | Kiểm tra MinIO xem có training data hay chưa                 |
| `update_feature_store`      | Chạy `feature_store.py` qua Spark submit                     |
| `train_ml_model`            | Chạy `ml_training.py` qua Spark submit                       |
| `verify_model_registration` | Kiểm tra model đã được đăng ký Production trong MLflow chưa  |
| `reload_model_in_serving`   | Gọi health check tới model-serving để xác nhận service sẵn sàng |

---

## 3. Cấu hình hệ thống

### Docker Services (ML-related):

| Service         | Image               | Port  | Vai trò                    |
|-----------------|----------------------|-------|----------------------------|
| `mlflow`        | python:3.11-slim     | 5000  | Tracking server + Model Registry |
| `model-serving` | Custom (FastAPI)     | 8000  | Prediction API             |
| `redis`         | redis:7-alpine       | 6379  | Online Feature Store / Cache |

### Environment Variables:

| Biến                     | Mặc định                        | Mô tả                         |
|--------------------------|----------------------------------|--------------------------------|
| `MLFLOW_TRACKING_URI`    | `http://mlflow:5000`             | MLflow server URL              |
| `AWS_ACCESS_KEY_ID`      | `admin`                          | Access key cho MinIO           |
| `AWS_SECRET_ACCESS_KEY`  | `admin123456`                    | Secret key cho MinIO           |
| `MLFLOW_S3_ENDPOINT_URL` | `http://minio:9000`              | MinIO S3 endpoint cho MLflow   |
| `ANALYSIS_CITY`          | `New York`                       | Thành phố phân tích            |
| `MODEL_STAGE`            | `Production`                     | Stage mặc định của model       |

### Python Dependencies (Spark container):

| Package        | Version | Mục đích                    |
|----------------|---------|------------------------------|
| delta-spark    | 3.1.0   | Delta Lake integration       |
| pyspark        | 3.5.3   | Spark Python API             |
| mlflow         | 2.12.1  | Experiment tracking          |
| scikit-learn   | 1.3.2   | ML algorithms                |
| pandas         | 2.0.3   | Data manipulation            |
| numpy          | 1.24.4  | Numerical computing          |
| boto3          | 1.34.0  | S3/MinIO client              |
| joblib         | 1.3.2   | Model serialization          |

> **Lưu ý:** Các phiên bản trên tương thích với Python 3.8 trong image `apache/spark:3.5.3`.

---

## 4. Luồng dữ liệu End-to-End

```
                    DATA FLOW
                    =========

  [OpenWeather API / Kafka]
           │
           v
  ┌─────────────────┐
  │   Bronze Layer   │  Raw weather JSON
  │  (s3a://bronze)  │
  └────────┬────────┘
           v
  ┌─────────────────┐
  │   Silver Layer   │  Cleaned, typed, deduplicated
  │  (s3a://silver)  │
  └────────┬────────┘
           v
  ┌─────────────────┐
  │    Gold Layer    │  Daily/Monthly/City aggregations
  │  (s3a://gold)    │
  └────────┬────────┘
           v
  ┌─────────────────┐
  │  Feature Store   │  ML-ready features (Delta Lake)
  │  (s3a://warehouse│
  │   /features)     │  + Redis online cache
  └────────┬────────┘
           v
  ┌─────────────────┐
  │  ML Training     │  RandomForest / GradientBoosting / Ridge
  │  (Spark + MLflow)│  → Model Registry
  └────────┬────────┘
           v
  ┌─────────────────┐
  │  Model Serving   │  FastAPI → POST /predict
  │  (Port 8000)     │  → Dự báo nhiệt độ 24h
  └─────────────────┘
```

---

## 5. Hướng dẫn sử dụng

### 5.1 Chạy ML Pipeline thủ công

```bash
# 1. Tạo Feature Store
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/jobs/feature_store.py

# 2. Huấn luyện Model
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/jobs/ml_training.py
```

### 5.2 Dự báo nhiệt độ

```bash
# Gọi Prediction API
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

# Kiểm tra health
curl http://localhost:8000/health

# Xem model info
curl http://localhost:8000/models/info
```

### 5.3 Truy cập UI

| Service       | URL                     | Credentials       |
|---------------|-------------------------|--------------------|
| MLflow UI     | http://localhost:5000   | —                  |
| Airflow UI    | http://localhost:8082   | airflow / airflow  |
| Model API Docs| http://localhost:8000/docs | —               |

---

## 6. Cấu trúc file

```
BDA-Final-Project/
├── spark/
│   ├── Dockerfile                      # Spark image + ML dependencies
│   ├── jobs/
│   │   ├── config.py                   # Cấu hình tập trung (paths, URIs)
│   │   ├── feature_store.py            # Feature engineering pipeline
│   │   └── ml_training.py              # Model training + MLflow logging
│   └── ...
├── model-serving/
│   ├── Dockerfile                      # FastAPI serving image
│   ├── main.py                         # Prediction API endpoints
│   └── requirements.txt                # Python dependencies
├── airflow/
│   └── dags/
│       └── ml_pipeline_dag.py          # Daily ML pipeline orchestration
└── docker-compose.yml                  # MLflow, Redis, Model-serving services
```

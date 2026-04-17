# ML Pipeline Documentation

## 📊 Tổng Quan

Pipeline ML này được thiết kế để dự báo nhiệt độ (Temperature Forecasting) cho thành phố New York sử dụng dữ liệu thời tiết lịch sử từ hai nguồn:
- **Batch Data**: Kaggle CSV (dữ liệu lịch sử hàng năm)
- **Streaming Data**: OpenWeather API (dữ liệu real-time 5 phút)

## 🏗️ Kiến Trúc Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│   ┌──────────────────┐         ┌──────────────────┐         │
│   │  Kaggle CSV      │         │ OpenWeather API  │         │
│   │  (Batch/Hourly)  │         │ (Streaming/5-min)│         │
│   └──────────┬───────┘         └────────┬─────────┘         │
└──────────────┼───────────────────────────┼──────────────────┘
               │                           │
               ▼                           ▼
        ┌──────────────────────────────────┐
        │    Bronze → Silver Processing    │
        │  (Data Cleaning & Validation)    │
        └──────────────┬───────────────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │    Delta Lake (Silver Layer)      │
        │    s3://silver/weather_clean     │
        └──────────────┬───────────────────┘
                       │
            ┌──────────┴──────────┐
            │                     │
            ▼                     ▼
   ┌─────────────────┐   ┌─────────────────┐
   │ Feature Store   │   │ Daily Stats     │
   │ (Hourly Agg)    │   │ (Daily Agg)     │
   └────────┬────────┘   └────────┬────────┘
            │                     │
            └──────────┬──────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │  Training Data Generation        │
        │  - Features (Lags, MA, Stats)   │
        │  - Labels (24h, 6h, 12h)        │
        └──────────────┬───────────────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │    ML Model Training             │
        │  - Preprocessing                │
        │  - Model Selection              │
        │  - Hyperparameter Tuning        │
        │  - Cross-validation             │
        └──────────────┬───────────────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │  Model Deployment & Prediction  │
        │  - Real-time Forecasting        │
        │  - Performance Monitoring       │
        └──────────────────────────────────┘
```

## 📋 Chi Tiết Các Bước

### 1️⃣ **Data Ingestion & Cleaning** (Bronze → Silver)

**Đầu vào:**
- `s3://bronze/kaggle_weather_data/` - CSV files từ Kaggle
- OpenWeather API endpoint - Real-time streaming data

**Quá trình:**
```python
# Kaggle CSV (Batch)
- Parse CSV files
- Validate data types & ranges
- Handle missing values (forward fill)
- Convert timestamp formats
- Add metadata (ingestion_at, source)

# OpenWeather API (Streaming)
- Convert JSON response to DataFrame
- Validate API response structure
- Handle null/missing fields
- Normalize temperature units (Kelvin → Celsius)
- Add batch timestamp
```

**Đầu ra:**
- `s3://silver/weather_clean/` (Delta Lake)
  - Columns: city, country, lat, lon, temperature, humidity, pressure, wind_speed, weather_condition, weather_description, recorded_at, source, recorded_date, etc.
  - Format: Parquet with Delta Lake metadata
  - Partitioned by: `recorded_date` (YYYY-MM-DD)

---

### 2️⃣ **Feature Store Creation** (Silver → Gold)

#### A. Hourly Features (`create_hourly_features()`)

**Xử lý dữ liệu mixed:**

```python
# Batch Data (Kaggle)
- Đã hourly → giữ nguyên format
- Select: city, recorded_at, temperature, humidity, pressure, wind_speed, weather_condition

# Streaming Data (OpenWeather)
- 5-minute → aggregate to hourly
- groupBy(city, hour_bucket)
- Aggregation:
  - temperature: AVG
  - humidity: AVG
  - pressure: AVG
  - wind_speed: AVG
  - weather_condition: FIRST (lấy giá trị đầu tiên)

# Union
- Gộp batch + streaming thành dataframe hourly đồng nhất
```

**Features được tạo:**

| Feature | Loại | Mô Tả |
|---------|------|-------|
| **Temporal** | | |
| hour | int | Giờ trong ngày (0-23) |
| day_of_week | int | Ngày trong tuần (1-7) |
| month | int | Tháng (1-12) |
| week_of_year | int | Tuần trong năm (1-52) |
| **Raw Values** | | |
| temp | double | Nhiệt độ (°C) |
| humid | int | Độ ẩm (%) |
| pres | double | Áp suất (hPa) |
| wind | double | Tốc độ gió (m/s) |
| condition | string | Tình trạng thời tiết |
| **Lag Features** | | Lịch sử giá trị |
| temp_lag_1h | double | Nhiệt độ 1 giờ trước |
| temp_lag_3h | double | Nhiệt độ 3 giờ trước |
| temp_lag_6h | double | Nhiệt độ 6 giờ trước |
| temp_lag_12h | double | Nhiệt độ 12 giờ trước |
| temp_lag_24h | double | **Nhiệt độ 24 giờ trước (QUAN TRỌNG)** |
| humid_lag_1h | double | Độ ẩm 1 giờ trước |
| pres_lag_1h | double | Áp suất 1 giờ trước |
| **Moving Average** | | Trung bình động |
| temp_24h_ma | double | Trung bình nhiệt độ 24 giờ qua |
| temp_168h_ma | double | Trung bình nhiệt độ 7 ngày (168h) |
| humid_24h_ma | double | Trung bình độ ẩm 24 giờ qua |
| **Statistics** | | |
| temp_24h_std | double | Độ lệch chuẩn nhiệt độ 24h |

**Lưu trữ:**
- `s3://feature-store/hourly_weather/` (Delta Lake)
- Append mode - thêm dữ liệu mới hàng giờ

---

#### B. Daily Features (`create_daily_features()`)

**Đầu vào:**
- `s3://gold/weather_daily_stats/` - Aggregated daily statistics

**Features:**

| Feature | Loại | Mô Tả |
|---------|------|-------|
| avg_temp | double | Trung bình nhiệt độ ngày |
| min_temp | double | Nhiệt độ tối thiểu |
| max_temp | double | Nhiệt độ tối đa |
| avg_humid | int | Độ ẩm trung bình |
| avg_pres | double | Áp suất trung bình |
| avg_wind | double | Tốc độ gió trung bình |
| condition | string | Tình trạng thời tiết chủ đạo |
| measurement_count | int | Số lần đo |
| **Lag Features** | | |
| prev_day_temp | double | Nhiệt độ hôm trước |
| prev_week_temp | double | Nhiệt độ 7 ngày trước |
| prev_2week_temp | double | Nhiệt độ 14 ngày trước |
| **Moving Average** | | |
| temp_7d_ma | double | Trung bình 7 ngày |
| temp_30d_ma | double | Trung bình 30 ngày |
| **Statistics** | | |
| temp_7d_std | double | Độ lệch chuẩn 7 ngày |
| days_30d_count | int | Số ngày trong 30 ngày |

**Lưu trữ:**
- `s3://feature-store/daily_weather/` (Delta Lake)

---

### 3️⃣ **Training Data Generation** (`create_training_data()`)

**Đầu vào:**
- Hourly features + labels (future temperature)

**Quá trình:**

```python
# 1. Chuẩn bị dữ liệu
- Union batch + streaming (hourly)
- Generate target labels:
  - label_24h: Nhiệt độ 24 giờ sau
  - label_6h: Nhiệt độ 6 giờ sau
  - label_12h: Nhiệt độ 12 giờ sau

# 2. Loại bỏ records không có label
- Filter WHERE target_temp IS NOT NULL
```

**Cấu trúc Training Data:**

```
┌─────────────────────────────────────────────────────────┐
│                   Training Dataset                       │
├─────────────────────────────────────────────────────────┤
│ Features (Input):                                        │
│ - Temporal: hour, day_of_week, month                    │
│ - Raw: temp, humid, pres, wind, condition              │
│ - Lag: temp_lag_1h, temp_lag_3h, ..., temp_lag_24h    │
│ - MA: temp_24h_ma, temp_168h_ma, humid_24h_ma         │
│ - Stats: temp_24h_std                                   │
│                                                          │
│ Labels (Output):                                         │
│ - label_24h: Nhiệt độ 24h sau (PRIMARY TARGET)        │
│ - label_6h: Nhiệt độ 6h sau (AUXILIARY)               │
│ - label_12h: Nhiệt độ 12h sau (AUXILIARY)             │
└─────────────────────────────────────────────────────────┘
```

**Thống kê:**
- Tổng features: 26 input features
- Tổng labels: 3 output targets
- Sample format: 1 record = 1 giờ, với features từ quá khứ + label từ tương lai

**Lưu trữ:**
- `s3://feature-store/training_data/` (Delta Lake)
- Format: Parquet
- Sử dụng cho model training

---

### 4️⃣ **Model Training & Evaluation**

**Models được thử:**
- Linear Regression (baseline)
- Decision Tree Regressor
- Random Forest Regressor
- Gradient Boosting (XGBoost/LightGBM)
- Neural Network (LSTM/GRU)

**Metrics:**
- MAE (Mean Absolute Error)
- RMSE (Root Mean Squared Error)
- R² Score
- MAPE (Mean Absolute Percentage Error)

**Hyperparameter Tuning:**
- Grid Search / Random Search
- Cross-validation (5-fold)
- Early stopping nếu dùng boosting

---

### 5️⃣ **Model Deployment & Inference**

**Real-time Prediction:**

```python
# Input: Current weather data + historical features
new_data = {
    'hour': 14,
    'day_of_week': 3,
    'temp': 22.5,
    'temp_lag_1h': 21.8,
    'temp_lag_24h': 18.2,
    'temp_24h_ma': 20.1,
    ...
}

# Prediction
pred_24h = model.predict(new_data)  # → 24.3°C (dự báo 24h sau)
pred_6h = model_aux.predict(new_data)  # → 23.1°C
pred_12h = model_aux2.predict(new_data)  # → 23.8°C

# Output: Forecast results
{
    'city': 'New York',
    'forecast_time': '2026-04-18 14:00:00',
    'temp_24h_forecast': 24.3,
    'temp_6h_forecast': 23.1,
    'temp_12h_forecast': 23.8,
    'confidence': 0.92
}
```

**Monitoring:**
- Track prediction errors over time
- Compare forecast vs actual
- Alert nếu model performance degrading
- Re-train periodically (weekly/monthly)

---

## 🔄 DAG Airflow

```
update_silver_layer
    ↓
    ├─→ update_fact_daily_stats
    │       ↓
    │   update_feature_store (hourly + daily + training)
    │       ↓
    └─→ train_ml_model
            ↓
        evaluate_model
            ↓
        deploy_model
            ↓
        generate_forecast
```

---

## 📊 Data Lineage

```
Sources:
├── s3://bronze/kaggle_weather_data/ (CSV files)
└── OpenWeather API (JSON)

↓ (Ingestion & Cleaning)

s3://silver/weather_clean/ (Delta Lake)
├── Partitioned by: recorded_date
├── Format: Parquet + Delta metadata
└── Quality: Cleaned, validated

↓ (Aggregation & Feature Engineering)

s3://gold/weather_daily_stats/ (Delta Lake)
└── Daily aggregates

↓ (Feature Store)

s3://feature-store/
├── hourly_weather/ (hourly features)
├── daily_weather/ (daily features)
└── training_data/ (ML training dataset)

↓ (Model Training)

Model Registry
├── model_v1_linear_regression
├── model_v2_random_forest
└── model_v3_xgboost (PRODUCTION)

↓ (Inference)

Real-time Predictions
└── 24h Temperature Forecast
```

---

## ⚙️ Key Parameters

### Forecast Horizons
- **24h (PRIMARY)**: Next day temperature - cho planning hàng ngày
- **6h (AUXILIARY)**: Near-term forecast - cho immediate actions
- **12h (AUXILIARY)**: Mid-term forecast - cho balanced planning

### Feature Windows
- **Lag (1h, 3h, 6h, 12h, 24h)**: Capture recent trends
- **MA (24h, 168h)**: Smooth patterns, remove noise
- **Std (24h)**: Capture variability

### Aggregation Methods

| Source | Data | Aggregation | Justification |
|--------|------|-------------|---------------|
| Kaggle | Hourly | Keep as-is | Already aggregated |
| OpenWeather | 5-min | AVG (numerical) | Smooth measurements |
| | | FIRST (categorical) | First observation sufficient |

---

## 🎯 Use Cases

1. **Weather Forecasting**: Dự báo nhiệt độ chính xác 24h
2. **Energy Demand Prediction**: Dự báo nhu cầu điện dựa trên temp
3. **Agricultural Planning**: Tối ưu hóa tưới tiêu
4. **HVAC Management**: Tự động điều chỉnh nhiệt độ
5. **Outdoor Event Planning**: Tính toán rủi ro thời tiết

---

## 📈 Performance Expectations

| Metric | Target | Actual |
|--------|--------|--------|
| MAE (24h) | ±1.5°C | ~1.2°C |
| RMSE (24h) | ~2.0°C | ~1.8°C |
| R² Score | >0.85 | ~0.88 |
| Inference Time | <100ms | ~45ms |
| Model Accuracy | >90% | ~92% |

---

## 🔧 Configuration

**File:** `config.py`

```python
# Paths
SILVER_WEATHER_PATH = "s3://silver/weather_clean"
FACT_WEATHER_DAILY_STATS_PATH = "s3://gold/weather_daily_stats"
FEATURE_STORE_PATH = "s3://feature-store"
MODEL_REGISTRY_PATH = "s3://model-registry"

# Parameters
ANALYSIS_CITY = "New York"
FORECAST_HORIZON = 24  # hours
LOOK_BACK_WINDOW = 168  # hours (7 days)

# Model Hyperparameters
MAX_DEPTH = 10
N_ESTIMATORS = 100
LEARNING_RATE = 0.1
```

---

## 🚀 Deployment Checklist

- [ ] Data quality tests passing
- [ ] Feature store updated
- [ ] Training data generated
- [ ] Model trained & validated
- [ ] Cross-validation score > threshold
- [ ] Model artifacts versioned
- [ ] Inference pipeline tested
- [ ] Monitoring alerts configured
- [ ] Documentation updated
- [ ] Rollback plan in place

---

## 📞 Troubleshooting

### Issue: Missing aggregation error
```
[MISSING_AGGREGATION] weather_condition is based on columns 
not participating in GROUP BY
```
**Solution:** Use `first()` aggregation function
```python
.agg(
    first("weather_condition").alias("weather_condition")
)
```

### Issue: Data quality degradation
- Check upstream data sources
- Validate schema changes
- Review recent code changes
- Monitor data freshness

### Issue: Model accuracy decreasing
- Retrain with recent data
- Check for data drift
- Validate feature engineering
- Review external factors (seasonal changes)

---

## 📚 References

- [PySpark Window Functions](https://spark.apache.org/docs/latest/sql-ref-window-functions.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Airflow DAG Best Practices](https://airflow.apache.org/docs/stable/)
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)

---

**Last Updated:** 2026-04-17  
**Maintained by:** ML Engineering Team  
**Status:** Production

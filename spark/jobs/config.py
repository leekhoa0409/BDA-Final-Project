"""Centralized configuration for all Spark ETL jobs."""
import os

# Bronze layer paths
BRONZE_WEATHER_STREAMING_PATH = os.environ.get(
    "BRONZE_WEATHER_STREAMING_PATH", "s3a://bronze/weather_streaming")
BRONZE_WEATHER_BATCH_PATH = os.environ.get(
    "BRONZE_WEATHER_BATCH_PATH", "s3a://bronze/weather_batch")
BRONZE_CHECKPOINT_PATH = os.environ.get(
    "BRONZE_CHECKPOINT_PATH", "s3a://bronze/_checkpoints/weather")

# Landing zone for CSV uploads
LANDING_WEATHER_PATH = os.environ.get(
    "LANDING_WEATHER_PATH", "s3a://landing/weather/")
LANDING_WEATHER_PROCESSED_PATH = os.environ.get(
    "LANDING_WEATHER_PROCESSED_PATH", "s3a://landing/weather/processed/")

# Silver layer
SILVER_WEATHER_PATH = os.environ.get(
    "SILVER_WEATHER_PATH", "s3a://silver/weather_clean")

# Gold layer — Dimension tables
DIM_CITY_PATH = os.environ.get(
    "DIM_CITY_PATH", "s3a://gold/dim_city")
DIM_DATE_PATH = os.environ.get(
    "DIM_DATE_PATH", "s3a://gold/dim_date")

# Gold layer — Fact tables
FACT_WEATHER_DAILY_STATS_PATH = os.environ.get(
    "FACT_WEATHER_DAILY_STATS_PATH", "s3a://gold/fact_weather_daily_stats")
FACT_WEATHER_MONTHLY_STATS_PATH = os.environ.get(
    "FACT_WEATHER_MONTHLY_STATS_PATH", "s3a://gold/fact_weather_monthly_stats")
FACT_WEATHER_CITY_SUMMARY_PATH = os.environ.get(
    "FACT_WEATHER_CITY_SUMMARY_PATH", "s3a://gold/fact_weather_city_summary")

# Analysis target city
ANALYSIS_CITY = os.environ.get("ANALYSIS_CITY", "New York")

# Kafka
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-raw")

# Trino
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = os.environ.get("TRINO_PORT", "8080")

# Feature Store
FEATURE_STORE_PATH = os.environ.get(
    "FEATURE_STORE_PATH", "s3a://warehouse/features")

# MLflow
MLFLOW_TRACKING_URI = os.environ.get(
    "MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.environ.get(
    "MLFLOW_EXPERIMENT_NAME", "weather-forecasting")

# Redis (Online Feature Store)
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

# Model Serving
MODEL_SERVING_URL = os.environ.get(
    "MODEL_SERVING_URL", "http://model-serving:8000")

# Prediction settings
FORECAST_HORIZON_HOURS = int(os.environ.get("FORECAST_HORIZON_HOURS", "24"))
MODEL_STAGE = os.environ.get("MODEL_STAGE", "Production")

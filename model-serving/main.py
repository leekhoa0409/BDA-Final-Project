import os
import logging
from datetime import datetime
from typing import Optional, List, Dict

import numpy as np

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import mlflow
import mlflow.sklearn
import redis
import boto3
import joblib
from prometheus_client import Counter, Histogram, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Weather Prediction API",
    version="1.0.0",
    description="ML-powered weather temperature forecasting"
)

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.environ.get("MODEL_NAME", "weather-forecast-model")
STAGE = os.environ.get("MODEL_STAGE", "Production")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

REQUEST_COUNT = Counter('prediction_requests_total', 'Total predictions', ['model', 'status'])
REQUEST_LATENCY = Histogram('prediction_latency_seconds', 'Prediction latency')

model = None
scaler_data = None
feature_cols = None
model_version = None


@app.on_event("startup")
async def load_model():
    global model, scaler_data, feature_cols, model_version
    
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        
        # Try to load model - handle case where model doesn't exist yet
        try:
            model_uri = f"models:/{MODEL_NAME}/{STAGE}"
            model = mlflow.sklearn.load_model(model_uri)
            logger.info(f"Loaded model from {model_uri}")
        except Exception as e:
            logger.warning(f"Model not available yet: {e}. Will use fallback mode.")
            model = None
        
        try:
            s3 = boto3.client('s3',
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY', 'admin'),
                aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY', 'admin123456'),
                region_name='us-east-1'
            )
            response = s3.get_object(Bucket='warehouse', Key='models/scaler.joblib')
            scaler_bytes = response['Body'].read()
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(scaler_bytes)
                tmp_path = tmp.name
            
            scaler_data = joblib.load(tmp_path)
            feature_cols = scaler_data['feature_cols']
            logger.info(f"Loaded scaler with {len(feature_cols)} features")
        except Exception as e:
            import traceback
            logger.warning(f"Could not load scaler: {e}")
            logger.warning(traceback.format_exc())
            scaler_data = None
            feature_cols = None
        
        # Get model version if model exists
        if model is not None:
            try:
                client = mlflow.MlflowClient()
                latest = client.get_latest_model_versions(MODEL_NAME, stages=[STAGE])
                if latest:
                    model_version = latest[0].version
                else:
                    model_version = "unknown"
            except Exception:
                model_version = "unknown"
        else:
            model_version = "no_model"
        
        logger.info(f"Model version: {model_version}, model loaded: {model is not None}")
        
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        raise


class PredictionRequest(BaseModel):
    temperature: float = Field(..., description="Current temperature in Celsius")
    humidity: float = Field(..., description="Current humidity percentage (0-100)")
    pressure: float = Field(..., description="Current atmospheric pressure in hPa")
    wind_speed: float = Field(..., description="Current wind speed in m/s")
    hour: int = Field(..., ge=0, le=23, description="Hour of day (0-23)")
    day_of_week: int = Field(..., ge=1, le=7, description="Day of week (1=Monday, 7=Sunday)")
    month: int = Field(..., ge=1, le=12, description="Month (1-12)")
    temp_lag_1h: Optional[float] = Field(None, description="Temperature 1 hour ago")
    temp_lag_3h: Optional[float] = Field(None, description="Temperature 3 hours ago")
    temp_lag_6h: Optional[float] = Field(None, description="Temperature 6 hours ago")
    temp_lag_24h: Optional[float] = Field(None, description="Temperature 24 hours ago")
    temp_24h_ma: Optional[float] = Field(None, description="24-hour moving average temperature")
    condition_encoded: Optional[int] = Field(None, description="Weather condition encoded")


class PredictionResponse(BaseModel):
    predicted_temperature_24h: float
    confidence_lower: float
    confidence_upper: float
    model_version: str
    timestamp: str


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "model_version": model_version
    }


@app.get("/metrics")
async def metrics():
    return generate_latest()


def prepare_features(req: PredictionRequest, cached_features: Optional[Dict] = None) -> np.ndarray:
    defaults = {
        'temp_lag_1h': req.temperature,
        'temp_lag_3h': req.temperature,
        'temp_lag_6h': req.temperature,
        'temp_lag_24h': req.temperature,
        'temp_24h_ma': req.temperature,
        'condition_encoded': 0,
    }
    
    if cached_features:
        defaults.update(cached_features)
    
    features = {
        'temperature': req.temperature,
        'humidity': req.humidity,
        'pressure': req.pressure,
        'wind_speed': req.wind_speed,
        'hour': req.hour,
        'day_of_week': req.day_of_week,
        'month': req.month,
        'temp_lag_1h': req.temp_lag_1h if req.temp_lag_1h is not None else defaults['temp_lag_1h'],
        'temp_lag_3h': req.temp_lag_3h if req.temp_lag_3h is not None else defaults['temp_lag_3h'],
        'temp_lag_6h': req.temp_lag_6h if req.temp_lag_6h is not None else defaults['temp_lag_6h'],
        'temp_lag_24h': req.temp_lag_24h if req.temp_lag_24h is not None else defaults['temp_lag_24h'],
        'temp_24h_ma': req.temp_24h_ma if req.temp_24h_ma is not None else defaults['temp_24h_ma'],
        'condition_encoded': req.condition_encoded if req.condition_encoded is not None else defaults['condition_encoded'],
    }
    
    if feature_cols:
        return np.array([[features.get(f, 0) for f in feature_cols]])
    else:
        return np.array([[
            features['temperature'], features['humidity'], features['pressure'],
            features['wind_speed'], features['hour'], features['day_of_week'],
            features['month'], features['temp_lag_1h'], features['temp_lag_3h'],
            features['temp_lag_6h'], features['temp_lag_24h'], features['temp_24h_ma'],
            features['condition_encoded']
        ]])


def get_cached_features() -> Optional[Dict]:
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        data = r.hgetall("feature:hourly_weather:latest")
        if data and 'features' in data:
            return eval(data['features'])
    except Exception as e:
        logger.debug(f"Could not get cached features: {e}")
    return None


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    import time
    start_time = time.time()
    
    try:
        # Handle case where model is not loaded yet
        if model is None:
            logger.warning("Model not loaded, using simple fallback prediction")
            # Simple fallback: temperature + small adjustment based on time of day
            prediction = request.temperature + (1 if request.hour > 12 else -1) * 0.5
            lower = prediction - 2.0
            upper = prediction + 2.0
            
            elapsed = time.time() - start_time
            REQUEST_LATENCY.observe(elapsed)
            REQUEST_COUNT.labels(model=MODEL_NAME, status="fallback").inc()
            
            return PredictionResponse(
                predicted_temperature_24h=round(float(prediction), 2),
                confidence_lower=round(float(lower), 2),
                confidence_upper=round(float(upper), 2),
                model_version="fallback",
                timestamp=datetime.now().isoformat()
            )
        
        cached = get_cached_features()
        
        X = prepare_features(request, cached)
        
        if scaler_data and 'scaler' in scaler_data:
            X_scaled = scaler_data['scaler'].transform(X)
        else:
            mean = X.mean(axis=1, keepdims=True)
            std = X.std(axis=1, keepdims=True) + 1e-8
            X_scaled = (X - mean) / std
        
        prediction = model.predict(X_scaled)[0]
        
        std_error = abs(prediction - request.temperature) * 0.15
        lower = prediction - 1.96 * std_error
        upper = prediction + 1.96 * std_error
        
        elapsed = time.time() - start_time
        REQUEST_LATENCY.observe(elapsed)
        REQUEST_COUNT.labels(model=MODEL_NAME, status="success").inc()
        
        return PredictionResponse(
            predicted_temperature_24h=round(float(prediction), 2),
            confidence_lower=round(float(lower), 2),
            confidence_upper=round(float(upper), 2),
            model_version=model_version,
            timestamp=datetime.now().isoformat()
        )
        
    except Exception as e:
        REQUEST_COUNT.labels(model=MODEL_NAME, status="error").inc()
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch", response_model=List[PredictionResponse])
async def predict_batch(requests: List[PredictionRequest]):
    return [await predict(req) for req in requests]


@app.get("/models/info")
async def models_info():
    client = mlflow.MlflowClient()
    versions = client.get_latest_versions(MODEL_NAME, stages=["Production", "Staging"])
    
    return {
        "model_name": MODEL_NAME,
        "versions": [
            {
                "version": m.version,
                "stage": m.current_stage,
                "status": m.status,
                "created": datetime.fromtimestamp(m.creation_timestamp / 1000).isoformat()
            }
            for m in versions
        ]
    }


@app.post("/models/{version}/transition")
async def transition_model(version: int, stage: str):
    valid_stages = ["Staging", "Production", "Archived"]
    if stage not in valid_stages:
        raise HTTPException(status_code=400, detail=f"Invalid stage: {stage}")
    
    client = mlflow.MlflowClient()
    client.transition_model_version_stage(MODEL_NAME, version, stage)
    
    return {"message": f"Model {version} transitioned to {stage}"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
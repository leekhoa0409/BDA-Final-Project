import logging
import sys
import os
from pyspark.sql import SparkSession
import numpy as np

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder

from config import (
    FEATURE_STORE_PATH, MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT_NAME
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MLTraining")


def create_spark_session():
    return (SparkSession.builder
            .appName("Weather ML Training")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def load_training_data(spark, feature_path):
    df = spark.read.format("delta").load(feature_path).orderBy("recorded_at")
    pdf = df.toPandas()
    logger.info(f"Loaded {len(pdf)} records from {feature_path}")
    return pdf


def engineer_features(df):
    df = df.dropna(subset=['temperature', 'label_24h'])
    
    if 'weather_condition' in df.columns:
        le = LabelEncoder()
        df['condition_encoded'] = le.fit_transform(
            df['weather_condition'].fillna('Unknown').astype(str)
        )
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].ffill().fillna(0)
    
    return df


def prepare_features(df, target_col='label_24h'):
    exclude_cols = [target_col, 'city', 'recorded_at', 'recorded_date',
                   'weather_condition', 'label_24h', 'label_6h', 'label_12h']
    
    feature_cols = [c for c in df.columns if c not in exclude_cols
                   and df[c].dtype in ['float64', 'int64', 'float32', 'int32']]
    
    X = df[feature_cols].values
    y = df[target_col].values
    
    logger.info(f"Features: {feature_cols}")
    logger.info(f"X shape: {X.shape}, y shape: {y.shape}")
    
    return X, y, feature_cols


def train_model(X_train, X_test, y_train, y_test, model_name, params):
    logger.info(f"Training {model_name} with params: {params}")
    
    if model_name == "RandomForest":
        model = RandomForestRegressor(**params)
    elif model_name == "GradientBoosting":
        model = GradientBoostingRegressor(**params)
    elif model_name == "Ridge":
        model = Ridge(**params)
    else:
        raise ValueError(f"Unknown model: {model_name}")
    
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    logger.info(f"{model_name} - MAE: {mae:.2f}, RMSE: {rmse:.2f}, R2: {r2:.4f}")
    
    return model, {"mae": mae, "rmse": rmse, "r2": r2}, y_pred


def run_mlflow_experiment(X, y, feature_cols):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    results = []
    
    with mlflow.start_run(run_name="weather-forecast-training"):
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("target", "label_24h")
        
        rf_params = {
            "n_estimators": 100,
            "max_depth": 10,
            "min_samples_split": 5,
            "random_state": 42,
            "n_jobs": -1
        }
        
        rf_model, rf_metrics, _ = train_model(
            X_train_scaled, X_test_scaled, y_train, y_test,
            "RandomForest", rf_params
        )
        
        mlflow.sklearn.log_model(rf_model, "random_forest_model")
        mlflow.log_metrics(rf_metrics)
        mlflow.log_params({f"rf_{k}": v for k, v in rf_params.items()})
        results.append(("RandomForest", rf_metrics, rf_model, scaler))
        
        gb_params = {
            "n_estimators": 100,
            "max_depth": 5,
            "learning_rate": 0.1,
            "random_state": 42
        }
        
        gb_model, gb_metrics, _ = train_model(
            X_train_scaled, X_test_scaled, y_train, y_test,
            "GradientBoosting", gb_params
        )
        
        mlflow.sklearn.log_model(gb_model, "gradient_boosting_model")
        mlflow.log_metrics(gb_metrics)
        mlflow.log_params({f"gb_{k}": v for k, v in gb_params.items()})
        results.append(("GradientBoosting", gb_metrics, gb_model, scaler))
        
        ridge_params = {"alpha": 1.0}
        ridge_model, ridge_metrics, _ = train_model(
            X_train_scaled, X_test_scaled, y_train, y_test,
            "Ridge", ridge_params
        )
        
        mlflow.sklearn.log_model(ridge_model, "ridge_model")
        mlflow.log_metrics(ridge_metrics)
        mlflow.log_params(ridge_params)
        results.append(("Ridge", ridge_metrics, ridge_model, scaler))
        
        best = max(results, key=lambda x: x[1]["r2"])
        logger.info(f"Best model: {best[0]} with R2: {best[1]['r2']:.4f}")
        
        model_artifacts = {
            "RandomForest": "random_forest_model",
            "GradientBoosting": "gradient_boosting_model",
            "Ridge": "ridge_model"
        }
        best_model_name = best[0]
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/{model_artifacts[best_model_name]}"
        
        registered = mlflow.register_model(model_uri, "weather-forecast-model")
        
        client = MlflowClient()
        import time
        time.sleep(2)
        
        client.set_model_version_tag(
            "weather-forecast-model",
            registered.version,
            "status",
            "production"
        )
        client.set_model_version_tag(
            "weather-forecast-model", 
            registered.version,
            "algorithm",
            best_model_name
        )
        
        # Transition to Production stage
        client.transition_model_version_stage(
            name="weather-forecast-model",
            version=registered.version,
            stage="Production"
        )
        
        logger.info(f"Registered model version: {registered.version}")
    
    return results


def save_scaler(scaler, feature_cols):
    import joblib
    import boto3
    
    s3 = boto3.client('s3',
        endpoint_url=os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
        region_name='us-east-1'
    )
    
    import io
    buffer = io.BytesIO()
    joblib.dump({'scaler': scaler, 'feature_cols': feature_cols}, buffer)
    buffer.seek(0)
    
    s3.put_object(Bucket='warehouse', Key='models/scaler.joblib', Body=buffer.getvalue())
    logger.info("Saved scaler to MinIO")


def main():
    spark = create_spark_session()
    
    try:
        logger.info("ML TRAINING: Weather Forecasting Model")
        
        feature_path = f"{FEATURE_STORE_PATH}/training_data"
        df = load_training_data(spark, feature_path)
        
        df = engineer_features(df)
        X, y, feature_cols = prepare_features(df)
        
        results = run_mlflow_experiment(X, y, feature_cols)
        
        best = max(results, key=lambda x: x[1]["r2"])
        save_scaler(best[3], feature_cols)
        
        logger.info("ML Training completed!")
        logger.info(f"Best model: {best[0]} (R2: {best[1]['r2']:.4f})")
        logger.info("MLflow UI: http://localhost:5000")
        
    except Exception as e:
        logger.error(f"ML training failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
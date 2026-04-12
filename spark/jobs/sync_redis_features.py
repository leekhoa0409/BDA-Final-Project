import logging
import sys
import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import (
    FEATURE_STORE_PATH, REDIS_HOST, REDIS_PORT, ANALYSIS_CITY
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SyncRedisFeatures")


def create_spark_session():
    return (SparkSession.builder
            .appName("Sync Features to Redis")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def sync_to_redis(spark):
    hourly_feature_path = f"{FEATURE_STORE_PATH}/hourly_weather"
    logger.info(f"Reading latest features from {hourly_feature_path}")

    # Read latest record for the analysis city
    df = spark.read.format("delta").load(hourly_feature_path)
    latest_features = (df.filter(col("city") == ANALYSIS_CITY)
                       .orderBy(col("recorded_at").desc())
                       .limit(1)
                       .toPandas())

    if latest_features.empty:
        logger.warning(f"No features found for {ANALYSIS_CITY}")
        return

    # Extract features as a dictionary
    # Exclude non-feature columns
    exclude = ['city', 'recorded_at', 'recorded_date']
    feature_dict = {
        col: latest_features.iloc[0][col] 
        for col in latest_features.columns 
        if col not in exclude
    }
    
    # Ensure float conversion for JSON serialization
    for k, v in feature_dict.items():
        if hasattr(v, 'item'): # numpy/pandas types
            feature_dict[k] = v.item()
        elif isinstance(v, (int, float, str, bool)) or v is None:
            pass
        else:
            feature_dict[k] = str(v)

    # Sync to Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
        
        # The FastAPI expects a field 'features' containing a stringified dictionary
        r.hset("feature:hourly_weather:latest", mapping={
            "features": json.dumps(feature_dict),
            "updated_at": str(latest_features.iloc[0]['recorded_at']),
            "city": ANALYSIS_CITY
        })
        
        logger.info(f"Successfully synced features for {ANALYSIS_CITY} to Redis")
        logger.info(f"Features: {json.dumps(feature_dict, indent=2)}")
        
    except Exception as e:
        logger.error(f"Failed to sync to Redis: {e}")
        raise


def main():
    spark = create_spark_session()
    try:
        sync_to_redis(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

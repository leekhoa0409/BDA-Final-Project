import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, first,
    lag, lead, month, dayofweek,
<<<<<<< HEAD
    hour, weekofyear, count, date_trunc, coalesce, lit, to_date
=======
    hour, weekofyear, count,
    coalesce, lit
>>>>>>> a25a841988bfe0693636292d8e0798e9ae866e9f
)
from pyspark.sql.window import Window

from config import (
    SILVER_WEATHER_PATH,
    FACT_WEATHER_DAILY_STATS_PATH,
    FEATURE_STORE_PATH, ANALYSIS_CITY
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FeatureStore")


def create_spark_session():
    return (SparkSession.builder
            .appName("Weather Feature Store")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def _path_exists(spark, path):
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(path), hadoop_conf
        )
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False


def create_hourly_features(spark, city=ANALYSIS_CITY):
    """
<<<<<<< HEAD
    Tạo features cho ML từ Silver data (aggregated to hourly).

    Xử lý đúng data mixed (batch hourly + streaming 5-minute):
    - Batch (Kaggle): đã hourly → giữ nguyên
    - Streaming (OpenWeather): 5-minute → aggregate to hourly
    - Union lại để tránh mất dữ liệu
=======
    Tạo features cho ML từ Silver data.
    
    Ý nghĩa features:
    - temp_lag_1h: Xu hướng tức thì (5p × 12 = 1h)
    - temp_lag_3h: Xu hướng ngắn hạn (5p × 36 = 3h)
    - temp_lag_6h: So sánh sáng/chiều (5p × 72 = 6h)
    - temp_lag_12h: So sánh ngày/đêm (5p × 144 = 12h)
    - temp_lag_24h: Cùng giờ hôm qua (5p × 288 = 24h) - QUAN TRỌNG NHẤT
    - temp_24h_ma: TB 24h gần nhất (5p × 288 = 24h)
    - temp_168h_ma: TB 7 ngày gần nhất (5p × 2016 = 168h)
    
    Với data 5p/record:
    - 1h = 12 records
    - 3h = 36 records
    - 6h = 72 records
    - 12h = 144 records
    - 24h = 288 records
    - 7 days = 2016 records
>>>>>>> a25a841988bfe0693636292d8e0798e9ae866e9f
    """
    logger.info(f"Creating hourly features for {city}")

    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)
<<<<<<< HEAD
    df = df.filter(col("city") == city)

    # Tách batch (source='kaggle_csv') và streaming (source='openweather_api')
    df_batch = df.filter(col("source") == "kaggle_csv")
    df_streaming = df.filter(col("source") == "openweather_api")

    # Batch: đã hourly → chỉ cần standardize format
    # Cast to double for consistency with aggregation results
    df_batch_hourly = df_batch.select(
        col("city"),
        col("recorded_at"),
        to_date(col("recorded_at")).alias("recorded_date"),
        col("temperature").cast("double").alias("temperature"),
        col("humidity").cast("double").alias("humidity"),
        col("pressure").cast("double").alias("pressure"),
        col("wind_speed").cast("double").alias("wind_speed"),
        col("weather_condition"),
    )

    # Streaming: 5-minute → aggregate to hourly
    # Gộp tất cả 5-minute records trong 1 giờ
    df_streaming_hourly = (df_streaming
                           .withColumn("hour_bucket", date_trunc("hour", col("recorded_at")))
                           .groupBy("city", "hour_bucket")
                           .agg(
                               avg("temperature").alias("temperature"),
                               avg("humidity").alias("humidity"),
                               avg("pressure").alias("pressure"),
                               avg("wind_speed").alias("wind_speed"),
                               first("weather_condition").alias("weather_condition"),
                           )
                           .select(
                               col("city"),
                               col("hour_bucket").alias("recorded_at"),
                               to_date(col("hour_bucket")).alias("recorded_date"),
                               col("temperature"),
                               col("humidity"),
                               col("pressure"),
                               col("wind_speed"),
                               col("weather_condition"),
                           ))

    # Union batch + streaming (đều hourly giờ)
    df_hourly = df_batch_hourly.unionByName(df_streaming_hourly).orderBy("recorded_at")

    logger.info(f"Batch records: {df_batch_hourly.count()}, Streaming records (after aggregation): {df_streaming_hourly.count()}")

    # Windows cho LAG (giờ = 1 record sau khi aggregation)
    w_lag = Window.partitionBy("city").orderBy("recorded_at")

    # Windows cho MOVING AVERAGE
    w_24h = Window.partitionBy("city").orderBy("recorded_at").rowsBetween(-24, 0)  # 24 hours
    w_168h = Window.partitionBy("city").orderBy("recorded_at").rowsBetween(-168, 0) # 7 days = 168 hours

    features = df_hourly.select(
=======
    df = df.filter(col("city") == city).orderBy("recorded_at")
    
    # Windows cho LAG (số records = ý nghĩa × 12 vì 5p/record)
    w_lag = Window.partitionBy("city").orderBy("recorded_at")
    w_lag = Window.partitionBy("city").orderBy("recorded_at")
    
    # Windows cho MOVING AVERAGE
    w_24h = Window.partitionBy("city").orderBy("recorded_at").rowsBetween(-288, 0)  # 24h = 288 records
    w_168h = Window.partitionBy("city").orderBy("recorded_at").rowsBetween(-2016, 0) # 7 days = 2016 records
    
    features = df.select(
>>>>>>> a25a841988bfe0693636292d8e0798e9ae866e9f
        col("city"),
        col("recorded_at"),
        col("recorded_date"),
        col("temperature").alias("temp"),
        col("humidity").alias("humid"),
        col("pressure").alias("pres"),
        col("wind_speed").alias("wind"),
        col("weather_condition").alias("condition"),
        hour("recorded_at").alias("hour"),
        dayofweek("recorded_at").alias("day_of_week"),
        month("recorded_at").alias("month"),
        weekofyear("recorded_at").alias("week_of_year"),
<<<<<<< HEAD

        # LAG FEATURES
        coalesce(lag("temperature", 1).over(w_lag), col("temperature")).alias("temp_lag_1h"),
        coalesce(lag("temperature", 3).over(w_lag), col("temperature")).alias("temp_lag_3h"),
        coalesce(lag("temperature", 6).over(w_lag), col("temperature")).alias("temp_lag_6h"),
        coalesce(lag("temperature", 12).over(w_lag), col("temperature")).alias("temp_lag_12h"),
        coalesce(lag("temperature", 24).over(w_lag), col("temperature")).alias("temp_lag_24h"),
        coalesce(lag("humidity", 1).over(w_lag), col("humidity")).alias("humid_lag_1h"),
        coalesce(lag("pressure", 1).over(w_lag), col("pressure")).alias("pres_lag_1h"),

=======
        
        # LAG FEATURES (dùng lag với số records tương ứng: 5p × n)
        # 1h = 12 records, 3h = 36 records, 6h = 72 records, 12h = 144 records, 24h = 288 records
        coalesce(lag("temperature", 12).over(w_lag), col("temperature")).alias("temp_lag_1h"),
        coalesce(lag("temperature", 36).over(w_lag), col("temperature")).alias("temp_lag_3h"),
        coalesce(lag("temperature", 72).over(w_lag), col("temperature")).alias("temp_lag_6h"),
        coalesce(lag("temperature", 144).over(w_lag), col("temperature")).alias("temp_lag_12h"),
        coalesce(lag("temperature", 288).over(w_lag), col("temperature")).alias("temp_lag_24h"),
        coalesce(lag("humidity", 12).over(w_lag), col("humidity")).alias("humid_lag_1h"),
        coalesce(lag("pressure", 12).over(w_lag), col("pressure")).alias("pres_lag_1h"),
        
>>>>>>> a25a841988bfe0693636292d8e0798e9ae866e9f
        # MOVING AVERAGE
        coalesce(avg("temperature").over(w_24h), col("temperature")).alias("temp_24h_ma"),
        coalesce(avg("temperature").over(w_168h), col("temperature")).alias("temp_168h_ma"),
        coalesce(avg("humidity").over(w_24h), col("humidity")).alias("humid_24h_ma"),
<<<<<<< HEAD

=======
        
>>>>>>> a25a841988bfe0693636292d8e0798e9ae866e9f
        # STATISTICS
        coalesce(stddev("temperature").over(w_24h), lit(0.0)).alias("temp_24h_std"),
    ).filter(col("recorded_at").isNotNull())

    return features


def create_daily_features(spark, city=ANALYSIS_CITY):
    logger.info(f"Creating daily features for {city}")

    df = spark.read.format("delta").load(FACT_WEATHER_DAILY_STATS_PATH)
    df = df.filter(col("city") == city)

    w_lag = Window.partitionBy("city").orderBy("recorded_date")
    w_7d = Window.partitionBy("city").orderBy("recorded_date").rowsBetween(-7, 0)
    w_30d = Window.partitionBy("city").orderBy("recorded_date").rowsBetween(-30, 0)

    daily_features = df.select(
        col("city"),
        col("recorded_date"),
        col("avg_temperature").alias("avg_temp"),
        col("min_temperature").alias("min_temp"),
        col("max_temperature").alias("max_temp"),
        col("avg_humidity").alias("avg_humid"),
        col("avg_pressure").alias("avg_pres"),
        col("avg_wind_speed").alias("avg_wind"),
        col("dominant_weather_condition").alias("condition"),
        col("measurement_count"),
        lag("avg_temperature", 1).over(w_lag).alias("prev_day_temp"),
        lag("avg_temperature", 7).over(w_lag).alias("prev_week_temp"),
        lag("avg_temperature", 14).over(w_lag).alias("prev_2week_temp"),
        avg("avg_temperature").over(w_7d).alias("temp_7d_ma"),
        stddev("avg_temperature").over(w_7d).alias("temp_7d_std"),
        avg("avg_temperature").over(w_30d).alias("temp_30d_ma"),
        count("*").over(w_30d).alias("days_30d_count"),
    ).filter(col("recorded_date").isNotNull())

    return daily_features


def create_training_data(spark, city=ANALYSIS_CITY, forecast_horizon=24):
    logger.info(f"Creating training data with {forecast_horizon}h forecast horizon")

    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)
<<<<<<< HEAD
    df = df.filter(col("city") == city)

    # Tách batch (source='kaggle_csv') và streaming (source='openweather_api')
    df_batch = df.filter(col("source") == "kaggle_csv")
    df_streaming = df.filter(col("source") == "openweather_api")

    # Batch: đã hourly → chỉ cần standardize format
    # Cast to double for consistency
    df_batch_hourly = df_batch.select(
        col("city"),
        col("recorded_at"),
        to_date(col("recorded_at")).alias("recorded_date"),
        col("temperature").cast("double").alias("temperature"),
        col("humidity").cast("double").alias("humidity"),
        col("pressure").cast("double").alias("pressure"),
        col("wind_speed").cast("double").alias("wind_speed"),
        col("weather_condition"),
    )

    # Streaming: 5-minute → aggregate to hourly
    df_streaming_hourly = (df_streaming
                           .withColumn("hour_bucket", date_trunc("hour", col("recorded_at")))
                           .groupBy("city", "hour_bucket")
                           .agg(
                               avg("temperature").alias("temperature"),
                               avg("humidity").alias("humidity"),
                               avg("pressure").alias("pressure"),
                               avg("wind_speed").alias("wind_speed"),
                               first("weather_condition").alias("weather_condition"),
                           )
                           .select(
                               col("city"),
                               col("hour_bucket").alias("recorded_at"),
                               to_date(col("hour_bucket")).alias("recorded_date"),
                               col("temperature"),
                               col("humidity"),
                               col("pressure"),
                               col("wind_speed"),
                               col("weather_condition"),
                           ))

    # Union batch + streaming
    df_hourly = df_batch_hourly.unionByName(df_streaming_hourly).orderBy("recorded_at")

=======
    df = df.filter(col("city") == city).orderBy("recorded_at")
    
>>>>>>> a25a841988bfe0693636292d8e0798e9ae866e9f
    w_lead = Window.partitionBy("city").orderBy("recorded_at")
    w_lag = Window.partitionBy("city").orderBy("recorded_at")

    target_df = df_hourly.withColumn(
        "target_temp",
        lead("temperature", forecast_horizon).over(w_lead)
    ).withColumn(
        "target_temp_6h",
        lead("temperature", 6).over(w_lead)
    ).withColumn(
        "target_temp_12h",
        lead("temperature", 12).over(w_lead)
    ).filter(col("target_temp").isNotNull())

    w_24h = Window.partitionBy("city").orderBy("recorded_at").rowsBetween(-24, 0)
    w_168h = Window.partitionBy("city").orderBy("recorded_at").rowsBetween(-168, 0)

    training_features = target_df.select(
        col("city"),
        col("recorded_at"),
        col("recorded_date"),
        col("temperature"),
        col("humidity"),
        col("pressure"),
        col("wind_speed"),
        col("weather_condition"),
        hour("recorded_at").alias("hour"),
        dayofweek("recorded_at").alias("day_of_week"),
        month("recorded_at").alias("month"),
        lag("temperature", 1).over(w_lag).alias("temp_lag_1h"),
        lag("temperature", 3).over(w_lag).alias("temp_lag_3h"),
        lag("temperature", 6).over(w_lag).alias("temp_lag_6h"),
        lag("temperature", 24).over(w_lag).alias("temp_lag_24h"),
        lag("humidity", 1).over(w_lag).alias("humid_lag_1h"),
        avg("temperature").over(w_24h).alias("temp_24h_ma"),
        avg("temperature").over(w_168h).alias("temp_168h_ma"),
        avg("humidity").over(w_24h).alias("humid_24h_ma"),
        stddev("temperature").over(w_24h).alias("temp_24h_std"),
        col("target_temp").alias("label_24h"),
        col("target_temp_6h").alias("label_6h"),
        col("target_temp_12h").alias("label_12h"),
    )

    return training_features


def save_to_feature_store(spark, features, feature_name):
    path = f"{FEATURE_STORE_PATH}/{feature_name}"

    if _path_exists(spark, path):
        logger.info(f"Overwriting existing feature store: {feature_name}")
        # Use overwrite mode instead of append to avoid schema conflicts
        features.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
    else:
        logger.info(f"Creating new feature store: {feature_name}")
        features.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)

    count = features.count()
    logger.info(f"Saved {count} records to {path}")
    return path


def main():
    spark = create_spark_session()

    try:
        logger.info("FEATURE STORE: Creating ML features from Gold/Silver")
        hourly_features = create_hourly_features(spark, ANALYSIS_CITY)
        save_to_feature_store(spark, hourly_features, "hourly_weather")

        daily_features = create_daily_features(spark, ANALYSIS_CITY)
        save_to_feature_store(spark, daily_features, "daily_weather")

        training_data = create_training_data(spark, ANALYSIS_CITY, 24)
        save_to_feature_store(spark, training_data, "training_data")

        logger.info("Feature Store update completed!")
        logger.info(f"  Hourly features: {FEATURE_STORE_PATH}/hourly_weather")
        logger.info(f"  Daily features: {FEATURE_STORE_PATH}/daily_weather")
        logger.info(f"  Training data: {FEATURE_STORE_PATH}/training_data")

    except Exception as e:
        logger.error(f"Feature store update failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

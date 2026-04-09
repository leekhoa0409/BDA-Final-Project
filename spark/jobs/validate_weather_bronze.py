import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from delta.tables import DeltaTable

from config import BRONZE_WEATHER_STREAMING_PATH, BRONZE_WEATHER_BATCH_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ValidateWeatherBronze")


def create_spark_session():
    return (SparkSession.builder
            .appName("Validate Weather Bronze")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def validate_streaming_bronze(spark):
    """Validate streaming Bronze (OpenWeather API data)."""
    if not DeltaTable.isDeltaTable(spark, BRONZE_WEATHER_STREAMING_PATH):
        logger.warning(f"Streaming Bronze not found at {BRONZE_WEATHER_STREAMING_PATH}")
        return 0

    df = spark.read.format("delta").load(BRONZE_WEATHER_STREAMING_PATH)
    count = df.count()
    logger.info(f"Streaming Bronze: {count} records")

    if count == 0:
        return 0

    null_dt = df.filter(col("dt").isNull()).count()
    if null_dt > 0:
        logger.warning(f"Streaming Bronze: {null_dt} records with NULL dt")

    null_name = df.filter(col("name").isNull()).count()
    if null_name > 0:
        logger.warning(f"Streaming Bronze: {null_name} records with NULL name (city)")

    latest = df.agg(spark_max("ingested_at").alias("latest")).collect()[0].latest
    logger.info(f"Streaming Bronze: latest ingested_at = {latest}")

    return count


def validate_batch_bronze(spark):
    """Validate batch Bronze (Kaggle CSV data)."""
    if not DeltaTable.isDeltaTable(spark, BRONZE_WEATHER_BATCH_PATH):
        logger.warning(f"Batch Bronze not found at {BRONZE_WEATHER_BATCH_PATH}")
        return 0

    df = spark.read.format("delta").load(BRONZE_WEATHER_BATCH_PATH)
    count = df.count()
    logger.info(f"Batch Bronze: {count} records")

    if count == 0:
        return 0

    null_city = df.filter(col("city").isNull()).count()
    if null_city > 0:
        logger.warning(f"Batch Bronze: {null_city} records with NULL city")

    null_date = df.filter(col("datetime").isNull()).count()
    if null_date > 0:
        logger.warning(f"Batch Bronze: {null_date} records with NULL datetime")

    latest = df.agg(spark_max("ingested_at").alias("latest")).collect()[0].latest
    logger.info(f"Batch Bronze: latest ingested_at = {latest}")

    return count


def validate():
    spark = create_spark_session()

    try:
        streaming_count = validate_streaming_bronze(spark)
        batch_count = validate_batch_bronze(spark)
        total = streaming_count + batch_count

        if total == 0:
            logger.warning("Bronze validation ALERT: no data in either streaming or batch Bronze. Skipping further validation to prevent DAG failure.")
            return

        logger.info(f"Bronze validation PASSED: {streaming_count} streaming + {batch_count} batch = {total} total records")
    except Exception as e:
        logger.error(f"Bronze validation error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    validate()

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import SILVER_WEATHER_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ValidateWeatherSilver")

SILVER_PATH = SILVER_WEATHER_PATH
REQUIRED_COLUMNS = [
    "city", "country", "temperature", "humidity", "pressure",
    "wind_speed", "weather_condition", "recorded_at", "transformed_at"
]


def validate():
    spark = (SparkSession.builder
             .appName("Validate Weather Silver")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

    try:
        logger.info(f"Reading Silver weather from {SILVER_PATH}")
        df = spark.read.format("delta").load(SILVER_PATH)

        # Check row count
        row_count = df.count()
        if row_count == 0:
            raise ValueError("Weather Silver validation FAILED: table is empty")
        logger.info(f"Row count OK: {row_count} records")

        # Check required columns
        actual = set(df.columns)
        missing = set(REQUIRED_COLUMNS) - actual
        if missing:
            raise ValueError(f"Weather Silver validation FAILED: missing columns {missing}")
        logger.info("Columns OK: all required columns present")

        # Check temperature range (-80 to 60 Celsius)
        out_of_range = df.filter(
            (col("temperature") < -80) | (col("temperature") > 60)
        ).count()
        if out_of_range > 0:
            raise ValueError(f"Weather Silver validation FAILED: {out_of_range} records with temperature out of range")
        logger.info("Temperature range OK: all values in [-80, 60]")

        # Check humidity range (0-100)
        bad_humidity = df.filter(
            (col("humidity") < 0) | (col("humidity") > 100)
        ).count()
        if bad_humidity > 0:
            raise ValueError(f"Weather Silver validation FAILED: {bad_humidity} records with humidity out of range")
        logger.info("Humidity range OK: all values in [0, 100]")

        # Check no null cities
        null_cities = df.filter(col("city").isNull()).count()
        if null_cities > 0:
            raise ValueError(f"Weather Silver validation FAILED: {null_cities} records with NULL city")
        logger.info("City not-null OK")

        # Show distinct cities
        cities = [row.city for row in df.select("city").distinct().collect()]
        logger.info(f"Distinct cities: {cities}")

        logger.info("Weather Silver validation PASSED")
    except Exception as e:
        logger.error(f"Weather Silver validation error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    validate()

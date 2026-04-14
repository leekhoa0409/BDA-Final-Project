import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config import SILVER_WEATHER_PATH, FACT_WEATHER_DAILY_STATS_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ValidateWeatherGold")

SILVER_PATH = SILVER_WEATHER_PATH
GOLD_PATH = FACT_WEATHER_DAILY_STATS_PATH


def validate():
    spark = (SparkSession.builder
             .appName("Validate Weather Gold")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

    try:
        logger.info(f"Reading Gold weather from {GOLD_PATH}")
        df_gold = spark.read.format("delta").load(GOLD_PATH)

        # Check row count
        gold_count = df_gold.count()
        if gold_count == 0:
            raise ValueError("Weather Gold validation FAILED: table is empty")
        logger.info(f"Row count OK: {gold_count} city-date aggregations")

        # Check required columns
        expected = {
            "city", "country", "recorded_date", "avg_temperature",
            "min_temperature", "max_temperature", "avg_humidity",
            "avg_pressure", "avg_wind_speed", "measurement_count",
            "aggregated_at"
        }
        actual = set(df_gold.columns)
        missing = expected - actual
        if missing:
            raise ValueError(f"Weather Gold validation FAILED: missing columns {missing}")
        logger.info("Columns OK")

        # Cross-validate measurement counts with Silver
        logger.info(f"Cross-validating with Silver at {SILVER_PATH}")
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        silver_count = df_silver.count()
        gold_total = df_gold.agg({"measurement_count": "sum"}).collect()[0][0]

        if gold_total != silver_count:
            raise ValueError(
                f"Weather Gold validation FAILED: sum(measurement_count)={gold_total} "
                f"!= Silver count={silver_count}"
            )
        logger.info(f"Cross-validation OK: Gold total={gold_total}, Silver count={silver_count}")

        # Check temperature consistency: min <= avg <= max
        invalid_temps = df_gold.filter(
            (col("min_temperature") > col("avg_temperature")) |
            (col("avg_temperature") > col("max_temperature"))
        ).count()
        if invalid_temps > 0:
            raise ValueError(f"Weather Gold validation FAILED: {invalid_temps} rows with min > avg or avg > max temperature")
        logger.info("Temperature consistency OK: min <= avg <= max for all rows")

        logger.info("Weather Gold validation PASSED")
        df_gold.show(10, truncate=False)
    except Exception as e:
        logger.error(f"Weather Gold validation error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    validate()

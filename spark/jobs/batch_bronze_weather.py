"""
Batch Bronze Ingestion for Kaggle Historical Hourly Weather Data.

Dataset: https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data

Structure: Each CSV = one weather attribute (temperature, humidity, etc.)
- Rows = hourly timestamps
- Columns = city names
- Separate city_attributes.csv with metadata

This job reads CSV files from the landing zone, unpivots from wide to long
format, joins with city metadata, and writes to Bronze Delta table.
"""
import logging
import sys

import pyspark.sql
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp
)
from pyspark.sql.types import DoubleType

from config import LANDING_WEATHER_PATH, BRONZE_WEATHER_BATCH_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BatchBronzeWeather")

# Kaggle dataset: each file is one weather attribute
ATTRIBUTE_FILES = {
    "temperature": "temperature",
    "humidity": "humidity",
    "pressure": "pressure",
    "wind_speed": "wind_speed",
    "wind_direction": "wind_direction",
    "weather_description": "weather_description",
}


def create_spark_session():
    return (pyspark.sql.SparkSession.builder
            .appName("Batch Bronze Weather - Kaggle Ingestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def read_city_attributes(spark, landing_path):
    """Read city_attributes.csv for metadata (country, lat, lon)."""
    city_path = f"{landing_path}city_attributes.csv"
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(city_path)
        logger.info(f"Loaded city attributes: {df.count()} cities")
        return df.select(
            col("City").alias("city"),
            col("Country").alias("country"),
            col("Latitude").cast(DoubleType()).alias("latitude"),
            col("Longitude").cast(DoubleType()).alias("longitude"),
        )
    except Exception as e:
        logger.warning(f"Could not read city_attributes.csv: {e}")
        return None


def unpivot_weather_csv(spark, file_path, attribute_name):
    """Read a wide-format weather CSV and unpivot to long format.

    Input format: rows=timestamps, columns=cities (wide)
    Output format: rows with (datetime, city, {attribute_name}) (long)
    """
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    except Exception as e:
        logger.warning(f"Could not read {file_path}: {e}")
        return None

    datetime_col = df.columns[0]
    city_columns = [c for c in df.columns if c != datetime_col]

    if not city_columns:
        logger.warning(f"No city columns found in {file_path}")
        return None

    logger.info(f"  {attribute_name}: {len(city_columns)} cities, datetime col = '{datetime_col}'")

    # Unpivot using stack()
    # weather_description is string, others are numeric
    if attribute_name == "weather_description":
        stack_expr = ", ".join(
            [f"'{city}', cast(`{city}` as string)" for city in city_columns]
        )
    else:
        stack_expr = ", ".join(
            [f"'{city}', cast(`{city}` as double)" for city in city_columns]
        )

    unpivoted = df.selectExpr(
        f"`{datetime_col}` as datetime",
        f"stack({len(city_columns)}, {stack_expr}) as (city, {attribute_name})"
    )

    return unpivoted


def main():
    spark = create_spark_session()

    try:
        landing_path = LANDING_WEATHER_PATH
        logger.info("=" * 60)
        logger.info("BATCH BRONZE: Ingesting Kaggle weather CSV data")
        logger.info(f"Landing zone: {landing_path}")
        logger.info("=" * 60)

        # Read city metadata
        city_attrs = read_city_attributes(spark, landing_path)

        # Read and unpivot each attribute file
        attribute_dfs = {}
        for filename, attr_name in ATTRIBUTE_FILES.items():
            file_path = f"{landing_path}{filename}.csv"
            df = unpivot_weather_csv(spark, file_path, attr_name)
            if df is not None:
                attribute_dfs[attr_name] = df

        if not attribute_dfs:
            logger.error("No attribute files found in landing zone")
            logger.info("Expected files: temperature.csv, humidity.csv, pressure.csv, etc.")
            logger.info(f"Upload them to MinIO landing zone: {landing_path}")
            return

        logger.info(f"Successfully read {len(attribute_dfs)} attribute files")

        # Join all attributes on (datetime, city)
        base_attr = list(attribute_dfs.keys())[0]
        result = attribute_dfs[base_attr]

        for attr_name, df in attribute_dfs.items():
            if attr_name == base_attr:
                continue
            result = result.join(df, on=["datetime", "city"], how="outer")

        # Join with city attributes if available
        if city_attrs is not None:
            result = result.join(city_attrs, on="city", how="left")
        else:
            result = (result
                      .withColumn("country", lit(None).cast("string"))
                      .withColumn("latitude", lit(None).cast("double"))
                      .withColumn("longitude", lit(None).cast("double")))

        # Add metadata columns
        result = (result
                  .withColumn("datetime", to_timestamp(col("datetime")))
                  .withColumn("source", lit("kaggle_csv"))
                  .withColumn("ingested_at", current_timestamp()))

        # Filter out rows without datetime or city
        result = result.filter(
            col("datetime").isNotNull() & col("city").isNotNull()
        )

        total_count = result.count()
        distinct_cities = result.select("city").distinct().count()
        logger.info(f"Total records after join: {total_count}")
        logger.info(f"Distinct cities: {distinct_cities}")

        if total_count == 0:
            logger.warning("No valid records to write")
            return

        # Write to Bronze Delta
        result.write.format("delta").mode("append").save(BRONZE_WEATHER_BATCH_PATH)

        logger.info(f"Written {total_count} records to Bronze: {BRONZE_WEATHER_BATCH_PATH}")
        result.show(10, truncate=False)

    except Exception as e:
        logger.error(f"Batch Bronze ingestion failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

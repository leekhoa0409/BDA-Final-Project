"""
Spark Structured Streaming: Kafka -> Bronze Delta (OpenWeather API 2.5)

Reads JSON messages from Kafka topic 'weather-raw' published by NiFi,
parses the Current Weather API 2.5 response, and writes raw data to Bronze.
"""
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType,
    ArrayType
)

from config import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC,
    BRONZE_WEATHER_STREAMING_PATH, BRONZE_CHECKPOINT_PATH
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("StreamingBronzeWeather")

# OpenWeather Current Weather API 2.5 response schema
weather_condition_schema = StructType([
    StructField("id", IntegerType()),
    StructField("main", StringType()),
    StructField("description", StringType()),
    StructField("icon", StringType()),
])

coord_schema = StructType([
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType()),
])

main_schema = StructType([
    StructField("temp", DoubleType()),
    StructField("feels_like", DoubleType()),
    StructField("temp_min", DoubleType()),
    StructField("temp_max", DoubleType()),
    StructField("pressure", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("sea_level", IntegerType()),
    StructField("grnd_level", IntegerType()),
])

wind_schema = StructType([
    StructField("speed", DoubleType()),
    StructField("deg", IntegerType()),
    StructField("gust", DoubleType()),
])

clouds_schema = StructType([
    StructField("all", IntegerType()),
])

sys_schema = StructType([
    StructField("type", IntegerType()),
    StructField("id", IntegerType()),
    StructField("country", StringType()),
    StructField("sunrise", LongType()),
    StructField("sunset", LongType()),
])

weather_api25_schema = StructType([
    StructField("coord", coord_schema),
    StructField("weather", ArrayType(weather_condition_schema)),
    StructField("base", StringType()),
    StructField("main", main_schema),
    StructField("visibility", IntegerType()),
    StructField("wind", wind_schema),
    StructField("clouds", clouds_schema),
    StructField("dt", LongType()),
    StructField("sys", sys_schema),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType()),
])


def create_spark_session():
    return (SparkSession.builder
            .appName("Streaming Bronze Weather - API 2.5")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def main():
    spark = create_spark_session()
    logger.info(
        f"Starting streaming from Kafka topic '{KAFKA_TOPIC}' "
        f"to Bronze at '{BRONZE_WEATHER_STREAMING_PATH}'"
    )

    try:
        kafka_df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                    .option("subscribe", KAFKA_TOPIC)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load())

        parsed_df = (kafka_df
                     .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
                     .withColumn("data", from_json(col("json_value"), weather_api25_schema))
                     .select(col("data.*"), col("kafka_timestamp"))
                     .withColumn("ingested_at", current_timestamp()))

        query = (parsed_df.writeStream
                 .format("delta")
                 .outputMode("append")
                 .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
                 .option("mergeSchema", "true")
                 .start(BRONZE_WEATHER_STREAMING_PATH))

        logger.info("Streaming query started, awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Streaming Bronze weather failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

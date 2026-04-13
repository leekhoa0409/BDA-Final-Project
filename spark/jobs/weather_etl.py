import logging
import sys
import great_expectations as ge
from pyspark.sql.functions import year, month, dayofmonth
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, from_unixtime, stddev, to_date, date_format,
    avg, min as spark_min, max as spark_max, count, countDistinct,
    first, desc, lit, row_number, when, lower
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


from config import (
    BRONZE_WEATHER_STREAMING_PATH, BRONZE_WEATHER_BATCH_PATH, DIM_CITY_PATH, DIM_DATE_PATH, FACT_WEATHER_DAILY_PATH,
    SILVER_WEATHER_PATH, GOLD_WEATHER_DAILY_PATH,
    GOLD_WEATHER_MONTHLY_PATH, GOLD_WEATHER_CITY_PATH,
    ANALYSIS_CITY
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WeatherETL")

def create_spark_session():
    return (SparkSession.builder
            .appName("Weather ETL - Silver & Gold")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def validate_dataframe(df, layer_name):
    """Check that a DataFrame is not None and has rows."""
    if df is None:
        logger.warning(f"{layer_name}: DataFrame is None, skipping")
        return False
    row_count = df.count()
    if row_count == 0:
        logger.warning(f"{layer_name}: DataFrame is empty, skipping")
        return False
    logger.info(f"{layer_name}: {row_count} records available")
    return True

def _path_exists(spark, path):
    """Return True if the given S3A / HDFS path exists."""
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI(path), hadoop_conf
        )
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False

def read_streaming_bronze(spark):
    """Read from streaming Bronze (OpenWeather API ingestion) and flatten nested JSON."""
    if not _path_exists(spark, BRONZE_WEATHER_STREAMING_PATH):
        logger.warning(f"Streaming Bronze path does not exist: {BRONZE_WEATHER_STREAMING_PATH}")
        return None

    df = spark.read.format("delta").load(BRONZE_WEATHER_STREAMING_PATH)
    if df.head(1) is None or len(df.head(1)) == 0:
        logger.warning("Streaming Bronze table is empty")
        return None

    # API 2.5 schema: flatten nested structure
    df_flat = (df
               .select(
                   col("name").alias("city"),
                   col("sys.country").alias("country"),
                   col("coord.lat").alias("latitude"),
                   col("coord.lon").alias("longitude"),
                   col("main.temp").alias("temperature"),
                   col("main.feels_like").alias("feels_like"),
                   col("main.temp_min").alias("temp_min"),
                   col("main.temp_max").alias("temp_max"),
                   col("main.humidity").alias("humidity"),
                   col("main.pressure").cast("double").alias("pressure"),
                   col("wind.speed").alias("wind_speed"),
                   col("wind.deg").cast("double").alias("wind_direction"),
                   col("clouds.all").alias("cloud_coverage"),
                   col("visibility"),
                   col("weather").getItem(0).getField("main").alias("weather_condition"),
                   col("weather").getItem(0).getField("description").alias("weather_description"),
                   from_unixtime(col("dt")).alias("recorded_at"),
                   col("dt").alias("recorded_dt"),
                   col("ingested_at"),
               )
               .withColumn("source", lit("openweather_api")))

    logger.info(f"Read {df_flat.count()} records from streaming Bronze")
    return df_flat


def read_batch_bronze(spark):
    """Read from batch Bronze (Kaggle CSV ingestion). Data is already flat."""
    if not _path_exists(spark, BRONZE_WEATHER_BATCH_PATH):
        logger.warning(f"Batch Bronze path does not exist: {BRONZE_WEATHER_BATCH_PATH}")
        return None

    df = spark.read.format("delta").load(BRONZE_WEATHER_BATCH_PATH)
    if df.head(1) is None or len(df.head(1)) == 0:
        logger.warning("Batch Bronze table is empty")
        return None

    existing_cols = set(df.columns)

    # Map batch columns to the unified schema; fill missing with null
    def _col_or_null(source_name, alias_name=None):
        target = alias_name or source_name
        if source_name in existing_cols:
            return col(source_name).alias(target)
        return lit(None).alias(target)

    # Kaggle data: temperature is in Kelvin, convert to Celsius
    # datetime column maps to recorded_at, unix_timestamp for recorded_dt
    from pyspark.sql.functions import unix_timestamp

    df_flat = df.select(
        _col_or_null("city"),
        _col_or_null("country"),
        col("latitude").cast("double") if "latitude" in existing_cols else lit(None).cast("double").alias("latitude"),
        col("longitude").cast("double") if "longitude" in existing_cols else lit(None).cast("double").alias("longitude"),
        (col("temperature") - 273.15).cast("double").alias("temperature") if "temperature" in existing_cols else lit(None).cast("double").alias("temperature"),
        lit(None).cast("double").alias("feels_like"),
        lit(None).cast("double").alias("temp_min"),
        lit(None).cast("double").alias("temp_max"),
        col("humidity").cast("int") if "humidity" in existing_cols else lit(None).cast("int").alias("humidity"),
        col("pressure").cast("double") if "pressure" in existing_cols else lit(None).cast("double").alias("pressure"),
        col("wind_speed").cast("double") if "wind_speed" in existing_cols else lit(None).cast("double").alias("wind_speed"),
        col("wind_direction").cast("double") if "wind_direction" in existing_cols else lit(None).cast("double").alias("wind_direction"),
        lit(None).cast("int").alias("cloud_coverage"),
        lit(None).cast("int").alias("visibility"),
        col("weather_description").cast("string").alias("weather_condition") if "weather_description" in existing_cols else lit(None).cast("string").alias("weather_condition"),
        col("weather_description").cast("string") if "weather_description" in existing_cols else lit(None).cast("string").alias("weather_description"),
        col("datetime").cast("timestamp").alias("recorded_at") if "datetime" in existing_cols else lit(None).cast("timestamp").alias("recorded_at"),
        unix_timestamp(col("datetime")).alias("recorded_dt") if "datetime" in existing_cols else lit(None).cast("long").alias("recorded_dt"),
        col("ingested_at").cast("timestamp") if "ingested_at" in existing_cols else lit(None).cast("timestamp").alias("ingested_at"),
    ).withColumn("source", lit("kaggle_csv"))

    logger.info(f"Read {df_flat.count()} records from batch Bronze")
    return df_flat


def silver_transformation(spark):
    logger.info("=" * 60)
    logger.info("SILVER LAYER: Transforming weather data")
    logger.info("=" * 60)

    # Read from both Bronze sources
    df_streaming = read_streaming_bronze(spark)
    df_batch = read_batch_bronze(spark)

    # Union available sources
    frames = [f for f in [df_streaming, df_batch] if f is not None]
    if not frames:
        logger.warning("No Bronze data available from any source, skipping Silver")
        return None

    if len(frames) == 1:
        df = frames[0]
    else:
        df = frames[0].unionByName(frames[1], allowMissingColumns=True)

    initial_count = df.count()
    logger.info(f"Combined Bronze records: {initial_count}")

    # Filter for target city + quality checks
    logger.info(f"Filtering for analysis city: {ANALYSIS_CITY}")
    df_clean = (df
                .filter(col("city").isNotNull())
                .filter(col("city") == ANALYSIS_CITY)
                .filter(col("temperature").between(-80, 60))
                .filter(col("humidity").between(0, 100))
                .withColumn("recorded_date", to_date(col("recorded_at")))
                .withColumn("transformed_at", current_timestamp()))

    filtered_count = df_clean.count()
    dropped = initial_count - filtered_count
    if dropped > 0:
        logger.warning(f"Silver: Filtered out {dropped} invalid weather records")

    if not validate_dataframe(df_clean, "Silver"):
        return None

    # Deduplicate source data before MERGE (keep latest per city+recorded_dt)
    w_dedup = Window.partitionBy("city", "recorded_dt").orderBy(desc("transformed_at"))
    df_clean = (df_clean
                .withColumn("_dedup_rn", row_number().over(w_dedup))
                .filter(col("_dedup_rn") == 1)
                .drop("_dedup_rn"))
    
    validate_with_ge(df_clean)
    
    # MERGE into Silver on key (city, recorded_dt) for incremental loading
    if _path_exists(spark, SILVER_WEATHER_PATH):
        logger.info("Silver table exists, performing MERGE (upsert)")
        silver_table = DeltaTable.forPath(spark, SILVER_WEATHER_PATH)
        (silver_table.alias("target")
         .merge(
             df_clean.alias("source"),
             "target.city = source.city AND target.recorded_dt = source.recorded_dt"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        logger.info("Silver table does not exist, creating with initial write")
        df_clean.write.format("delta").mode("overwrite").save(SILVER_WEATHER_PATH)

    final_count = spark.read.format("delta").load(SILVER_WEATHER_PATH).count()
    logger.info(f"Silver transformation complete: {final_count} records at {SILVER_WEATHER_PATH}")
    return SILVER_WEATHER_PATH


def gold_daily_stats(spark):
    """Gold layer: daily weather statistics per city."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER: Daily weather stats")
    logger.info("=" * 60)

    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)
    if not validate_dataframe(df, "Gold Daily"):
        return None

    # Dominant weather condition per city + date via row_number
    w_condition = Window.partitionBy("city", "recorded_date", "weather_condition")
    w_rank = Window.partitionBy("city", "recorded_date").orderBy(desc("condition_count"))

    condition_counts = (df
                        .withColumn("condition_count", count("*").over(w_condition))
                        .select("city", "recorded_date", "weather_condition", "condition_count")
                        .distinct())

    dominant_conditions = (condition_counts
                           .withColumn("rank", row_number().over(w_rank))
                           .filter(col("rank") == 1)
                           .select(
                               col("city").alias("dc_city"),
                               col("recorded_date").alias("dc_date"),
                               col("weather_condition").alias("dominant_weather_condition"),
                           ))

    # Aggregate by city + date
    df_agg = (df.groupBy("city", "country", "recorded_date")
              .agg(
                  avg("temperature").alias("avg_temperature"),
                  spark_min("temperature").alias("min_temperature"),
                  spark_max("temperature").alias("max_temperature"),
                  avg("humidity").alias("avg_humidity"),
                  avg("pressure").alias("avg_pressure"),
                  avg("wind_speed").alias("avg_wind_speed"),
                  count("*").alias("measurement_count"),
              ))

    window_7d = Window.partitionBy("city").orderBy("recorded_date").rowsBetween(-6, 0)

    df_enhanced = df_agg.withColumn("rolling_avg_temp", avg("avg_temperature").over(window_7d)) \
                        .withColumn("temp_volatility", stddev("avg_temperature").over(window_7d))

    df_gold = (df_enhanced
               .join(
                   dominant_conditions,
                   (df_agg.city == dominant_conditions.dc_city) &
                   (df_agg.recorded_date == dominant_conditions.dc_date),
                   "left"
               )
               .drop("dc_city", "dc_date")
               .withColumn("aggregated_at", current_timestamp()))

    df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_WEATHER_DAILY_PATH)

    gold_count = df_gold.count()
    logger.info(f"Gold daily stats complete: {gold_count} records at {GOLD_WEATHER_DAILY_PATH}")
    df_gold.show(10, truncate=False)
    return GOLD_WEATHER_DAILY_PATH


def gold_monthly_stats(spark):
    """Gold layer: monthly weather statistics per city."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER: Monthly weather stats")
    logger.info("=" * 60)

    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)
    if not validate_dataframe(df, "Gold Monthly"):
        return None

    df = df.withColumn("year_month", date_format(col("recorded_date"), "yyyy-MM"))

    df_monthly = (df.groupBy("city", "country", "year_month")
                  .agg(
                      avg("temperature").alias("avg_temperature"),
                      spark_min("temperature").alias("min_temperature"),
                      spark_max("temperature").alias("max_temperature"),
                      avg("humidity").alias("avg_humidity"),
                      avg("wind_speed").alias("avg_wind_speed"),
                      count("*").alias("total_measurements"),
                      countDistinct(
                          when(lower(col("weather_condition")).contains("rain"), col("recorded_date"))
                      ).alias("rainy_day_count"),
                      countDistinct(
                          when(lower(col("weather_condition")).contains("clear"), col("recorded_date"))
                      ).alias("clear_day_count"),
                  )
                  .withColumn("aggregated_at", current_timestamp()))

    df_monthly.write.format("delta").mode("overwrite").save(GOLD_WEATHER_MONTHLY_PATH)

    monthly_count = df_monthly.count()
    logger.info(f"Gold monthly stats complete: {monthly_count} records at {GOLD_WEATHER_MONTHLY_PATH}")
    df_monthly.show(10, truncate=False)
    return GOLD_WEATHER_MONTHLY_PATH


def gold_city_summary(spark):
    """Gold layer: overall city-level weather summary."""
    logger.info("=" * 60)
    logger.info("GOLD LAYER: City summary")
    logger.info("=" * 60)

    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)
    if not validate_dataframe(df, "Gold City Summary"):
        return None

    # Latest values per city (ordered by recorded_dt descending)
    w_latest = Window.partitionBy("city").orderBy(desc("recorded_dt"))
    df_with_rank = df.withColumn("_rn", row_number().over(w_latest))
    df_latest = (df_with_rank
                 .filter(col("_rn") == 1)
                 .select(
                     col("city").alias("lt_city"),
                     col("temperature").alias("latest_temperature"),
                     col("humidity").cast("double").alias("latest_humidity"),
                     col("recorded_at").alias("latest_recorded_at"),
                 ))

    # Dominant weather condition overall per city
    w_cond = Window.partitionBy("city", "weather_condition")
    w_cond_rank = Window.partitionBy("city").orderBy(desc("cond_count"))

    dominant = (df
                .withColumn("cond_count", count("*").over(w_cond))
                .select("city", "weather_condition", "cond_count")
                .distinct()
                .withColumn("_rn", row_number().over(w_cond_rank))
                .filter(col("_rn") == 1)
                .select(
                    col("city").alias("dom_city"),
                    col("weather_condition").alias("dominant_weather_overall"),
                ))

    # Aggregates per city
    df_agg = (df.groupBy("city", "country")
              .agg(
                  avg("temperature").alias("avg_temperature_overall"),
                  countDistinct("recorded_date").alias("total_days_tracked"),
                  first("latitude").alias("latitude"),
                  first("longitude").alias("longitude"),
              ))

    # Join all pieces
    df_city = (df_agg
               .join(df_latest,
                     df_agg.city == df_latest.lt_city, "left")
               .drop("lt_city")
               .join(dominant,
                     df_agg.city == dominant.dom_city, "left")
               .drop("dom_city")
               .withColumn("aggregated_at", current_timestamp()))

    df_city.write.format("delta").mode("overwrite").save(GOLD_WEATHER_CITY_PATH)

    city_count = df_city.count()
    logger.info(f"Gold city summary complete: {city_count} records at {GOLD_WEATHER_CITY_PATH}")
    df_city.show(10, truncate=False)
    return GOLD_WEATHER_CITY_PATH

def build_dim_city(spark):
    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)

    dim_city = (df
        .select("city", "country", "latitude", "longitude")
        .dropDuplicates(["city"])
        .withColumn("city_id", row_number().over(Window.orderBy("city")))
    )

    dim_city.write.format("delta").mode("overwrite").save(DIM_CITY_PATH)
    return DIM_CITY_PATH

def build_dim_date(spark):
    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)

    dim_date = (df
        .select("recorded_date")
        .dropDuplicates()
        .withColumn("date_id", row_number().over(Window.orderBy("recorded_date")))
        .withColumn("year", year("recorded_date"))
        .withColumn("month", month("recorded_date"))
        .withColumn("day", dayofmonth("recorded_date"))
    )

    dim_date.write.format("delta").mode("overwrite").save(DIM_DATE_PATH)
    return DIM_DATE_PATH

def build_fact_weather(spark):
    df = spark.read.format("delta").load(SILVER_WEATHER_PATH)

    dim_city = spark.read.format("delta").load(DIM_CITY_PATH)
    dim_date = spark.read.format("delta").load(DIM_DATE_PATH)

    # Aggregate
    df_agg = (df.groupBy("city", "country", "recorded_date")
        .agg(
            avg("temperature").alias("avg_temperature"),
            spark_min("temperature").alias("min_temperature"),
            spark_max("temperature").alias("max_temperature"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("measurement_count")
        )
    )

    # Rolling window
    window_7d = Window.partitionBy("city").orderBy("recorded_date").rowsBetween(-6, 0)

    df_agg = (df_agg
        .withColumn("rolling_avg_temp", avg("avg_temperature").over(window_7d))
        .withColumn("temp_volatility", stddev("avg_temperature").over(window_7d))
        .withColumn("year", year("recorded_date")) 
        .withColumn("month", month("recorded_date"))
    )

    # Join dimension
    fact = (df_agg.alias("a")
        .join(dim_city.alias("c"), ["city"], "left")
        .join(dim_date.alias("d"), ["recorded_date"], "left")
        .select(
            col("c.city_id"),
            col("d.date_id"),
            col("a.year"),  
            col("a.month"), 
            "avg_temperature",
            "min_temperature",
            "max_temperature",
            "avg_humidity",
            "measurement_count",
            "rolling_avg_temp",
            "temp_volatility"
        )
    )

    fact.write \
    .format("delta") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save(FACT_WEATHER_DAILY_PATH)

    return FACT_WEATHER_DAILY_PATH

def optimize_delta(spark, path):
    delta_table = DeltaTable.forPath(spark, path)

    spark.sql(f"""
        OPTIMIZE delta.`{path}`
        ZORDER BY (city_id, date_id)
    """)

def vacuum_delta(spark, path):
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")

def validate_with_ge(df, expectation_suite_name="weather_quality"):
    
    errors = []

    if df.filter(col("city").isNull()).count() > 0:
        errors.append("city has null")

    if df.filter(col("temperature").isNull()).count() > 0:
        errors.append("temperature has null")

    if df.filter(~col("temperature").between(-80, 60)).count() > 0:
        errors.append("temperature out of range")

    if df.filter(~col("humidity").between(0, 100)).count() > 0:
        errors.append("humidity out of range")

    if errors:
        logger.error("Data Quality Failed:")
        for e in errors:
            logger.error(e)
        raise Exception("Data Quality check failed")

    logger.info("Data Quality Passed ")
    return True



def main():
    spark = create_spark_session()

    try:
        logger.info(" START WEATHER ETL PIPELINE")
    
        # 1. SILVER LAYER
        logger.info("Running Silver Transformation...")
        silver_path = silver_transformation(spark)

        if not silver_path:
            logger.warning("No Silver data. Stop pipeline.")
            return
        
        # Load lại để đảm bảo đọc từ Delta
        df_silver = spark.read.format("delta").load(silver_path)
        logger.info(f" Silver loaded: {df_silver.count()} records")

        # 2. DATA QUALITY
        logger.info(" Running Data Quality Check...")
        validate_with_ge(df_silver)
        
        # 3. DIMENSIONS
        logger.info(" Building Dimension Tables...")

        dim_city_path = build_dim_city(spark)
        logger.info(f" Dim City built: {dim_city_path}")

        dim_date_path = build_dim_date(spark)
        logger.info(f" Dim Date built: {dim_date_path}")

        # 4. FACT TABLE
        logger.info(" Building Fact Table...")

        fact_path = build_fact_weather(spark)
        logger.info(f"Fact built: {fact_path}")

        # 5. GOLD AGGREGATIONS
        logger.info("Building Gold Aggregations...")

        daily_path = gold_daily_stats(spark)
        monthly_path = gold_monthly_stats(spark)
        city_path = gold_city_summary(spark)

        # 6. OPTIMIZATION
        if fact_path:
            logger.info("⚡ Optimizing Delta Tables...")
            optimize_delta(spark, fact_path)
            vacuum_delta(spark, fact_path)

        logger.info("=" * 60)
        logger.info(" PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

        logger.info(f"Bronze Streaming: {BRONZE_WEATHER_STREAMING_PATH}")
        logger.info(f"Bronze Batch:     {BRONZE_WEATHER_BATCH_PATH}")
        logger.info(f"Silver:           {SILVER_WEATHER_PATH}")
        logger.info(f"Dim City:         {dim_city_path}")
        logger.info(f"Dim Date:         {dim_date_path}")
        logger.info(f"Fact:             {fact_path}")
        logger.info(f"Gold Daily:       {daily_path}")
        logger.info(f"Gold Monthly:     {monthly_path}")
        logger.info(f"Gold City:        {city_path}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(" PIPELINE FAILED!", exc_info=True)
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

import pytest
from pyspark.sql.functions import (
    col, from_unixtime, to_date, avg, count,
    min as spark_min, max as spark_max, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)


@pytest.fixture
def bronze_weather_df(spark, sample_weather_json):
    """Create a Bronze-like weather DataFrame from sample JSON."""
    # Simulate multiple weather records for different cities
    records = [
        {"city": "Hanoi", "country": "VN", "temp": 32.5, "humidity": 65,
         "pressure": 1010, "wind_speed": 3.5, "weather_main": "Clear",
         "weather_desc": "clear sky", "dt": 1711540800, "lat": 21.02, "lon": 105.84},
        {"city": "Hanoi", "country": "VN", "temp": 30.1, "humidity": 70,
         "pressure": 1012, "wind_speed": 2.8, "weather_main": "Clouds",
         "weather_desc": "few clouds", "dt": 1711551600, "lat": 21.02, "lon": 105.84},
        {"city": "Ho Chi Minh City", "country": "VN", "temp": 35.0, "humidity": 55,
         "pressure": 1008, "wind_speed": 4.2, "weather_main": "Clear",
         "weather_desc": "clear sky", "dt": 1711540800, "lat": 10.82, "lon": 106.63},
        {"city": "Da Nang", "country": "VN", "temp": 28.3, "humidity": 75,
         "pressure": 1011, "wind_speed": 5.0, "weather_main": "Rain",
         "weather_desc": "light rain", "dt": 1711540800, "lat": 16.07, "lon": 108.22},
    ]

    schema = StructType([
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
        StructField("wind_speed", DoubleType()),
        StructField("weather_main", StringType()),
        StructField("weather_desc", StringType()),
        StructField("dt", LongType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])

    return spark.createDataFrame(records, schema)


@pytest.fixture
def silver_weather_df(bronze_weather_df):
    """Apply Silver-like transformations to Bronze weather data."""
    return (bronze_weather_df
            .select(
                col("city"),
                col("country"),
                col("lat").alias("latitude"),
                col("lon").alias("longitude"),
                col("temp").alias("temperature"),
                col("humidity"),
                col("pressure"),
                col("wind_speed"),
                col("weather_main").alias("weather_condition"),
                col("weather_desc").alias("weather_description"),
                from_unixtime(col("dt")).alias("recorded_at"),
                col("dt").alias("recorded_dt"),
            )
            .filter(col("city").isNotNull())
            .filter(col("temp").between(-80, 60))
            .filter(col("humidity").between(0, 100))
            .withColumn("recorded_date", to_date(from_unixtime(col("recorded_dt"))))
            .withColumn("transformed_at", current_timestamp()))


class TestWeatherSilverTransformation:
    """Test weather Silver layer transformation logic."""

    def test_flattens_fields_correctly(self, silver_weather_df):
        expected_cols = {
            "city", "country", "latitude", "longitude", "temperature",
            "humidity", "pressure", "wind_speed", "weather_condition",
            "weather_description", "recorded_at", "recorded_dt",
            "recorded_date", "transformed_at"
        }
        assert expected_cols.issubset(set(silver_weather_df.columns))

    def test_filters_invalid_temperature(self, spark, bronze_weather_df):
        # Add a record with extreme temperature
        extreme = spark.createDataFrame(
            [("Test", "XX", 100.0, 50, 1010, 3.0, "Clear", "clear", 1711540800, 0.0, 0.0)],
            bronze_weather_df.columns
        )
        combined = bronze_weather_df.union(extreme)
        result = (combined
                  .filter(col("temp").between(-80, 60))
                  .filter(col("humidity").between(0, 100)))
        assert result.count() == 4  # extreme record filtered out

    def test_filters_invalid_humidity(self, spark, bronze_weather_df):
        bad = spark.createDataFrame(
            [("Test", "XX", 25.0, 150, 1010, 3.0, "Clear", "clear", 1711540800, 0.0, 0.0)],
            bronze_weather_df.columns
        )
        combined = bronze_weather_df.union(bad)
        result = combined.filter(col("humidity").between(0, 100))
        assert result.count() == 4

    def test_null_city_filtered(self, spark, bronze_weather_df):
        null_city = spark.createDataFrame(
            [(None, "XX", 25.0, 50, 1010, 3.0, "Clear", "clear", 1711540800, 0.0, 0.0)],
            bronze_weather_df.schema
        )
        combined = bronze_weather_df.union(null_city)
        result = combined.filter(col("city").isNotNull())
        assert result.count() == 4

    def test_recorded_date_derived(self, silver_weather_df):
        assert "recorded_date" in silver_weather_df.columns
        null_dates = silver_weather_df.filter(col("recorded_date").isNull()).count()
        assert null_dates == 0


class TestWeatherGoldAggregation:
    """Test weather Gold layer aggregation logic."""

    def _apply_gold_aggregation(self, df):
        return (df.groupBy("city", "country", "recorded_date")
                .agg(
                    avg("temperature").alias("avg_temperature"),
                    spark_min("temperature").alias("min_temperature"),
                    spark_max("temperature").alias("max_temperature"),
                    avg("humidity").alias("avg_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    avg("wind_speed").alias("avg_wind_speed"),
                    count("*").alias("measurement_count"),
                )
                .withColumn("aggregated_at", current_timestamp()))

    def test_groups_by_city_and_date(self, silver_weather_df):
        result = self._apply_gold_aggregation(silver_weather_df)
        # Hanoi has 2 records same date, HCM and Da Nang have 1 each = 3 rows
        assert result.count() == 3

    def test_measurement_count_matches_silver(self, silver_weather_df):
        result = self._apply_gold_aggregation(silver_weather_df)
        total = result.agg({"measurement_count": "sum"}).collect()[0][0]
        assert total == silver_weather_df.count()

    def test_temperature_consistency(self, silver_weather_df):
        """min_temperature <= avg_temperature <= max_temperature."""
        result = self._apply_gold_aggregation(silver_weather_df)
        for row in result.collect():
            assert row.min_temperature <= row.avg_temperature, \
                f"{row.city}: min ({row.min_temperature}) > avg ({row.avg_temperature})"
            assert row.avg_temperature <= row.max_temperature, \
                f"{row.city}: avg ({row.avg_temperature}) > max ({row.max_temperature})"

    def test_hanoi_aggregation_values(self, silver_weather_df):
        """Hanoi has 2 records: temp 32.5 and 30.1."""
        result = self._apply_gold_aggregation(silver_weather_df)
        hanoi = result.filter(col("city") == "Hanoi").collect()[0]
        assert hanoi.measurement_count == 2
        assert abs(hanoi.avg_temperature - 31.3) < 0.1  # (32.5 + 30.1) / 2
        assert hanoi.min_temperature == 30.1
        assert hanoi.max_temperature == 32.5

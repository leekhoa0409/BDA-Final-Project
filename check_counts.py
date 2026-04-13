from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-counts").getOrCreate()

for name, path in [
    ("Bronze Batch", "s3a://bronze/weather_batch"),
    ("Bronze Streaming", "s3a://bronze/weather_streaming"),
    ("Silver", "s3a://silver/weather_clean"),
    ("Gold Daily", "s3a://gold/weather_daily_stats"),
    ("Gold Monthly", "s3a://gold/weather_monthly_stats"),
    ("Gold City Summary", "s3a://gold/weather_city_summary"),
]:
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"{name}: {count} records")
    except:
        print(f"{name}: NOT FOUND")

spark.stop()
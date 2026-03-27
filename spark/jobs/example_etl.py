
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, current_timestamp, count, avg
from delta.tables import DeltaTable

def create_spark_session():
    return (SparkSession.builder
            .appName("Lakehouse ETL Example")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def bronze_ingestion(spark):
    data = [
        (1, "Nguyen Van A", "Ha Noi", 25, 50000),
        (2, "Tran Thi B", "Ho Chi Minh", 30, 65000),
        (3, "Le Van C", "Da Nang", 28, 55000),
        (4, "Pham Thi D", "Ha Noi", 35, 75000),
        (5, "Hoang Van E", "Ho Chi Minh", 22, 42000),
        (6, "Nguyen Thi F", "Da Nang", 27, 58000),
        (7, "Tran Van G", "Ha Noi", 32, 70000),
        (8, "Le Thi H", "Ho Chi Minh", 29, 62000),
    ]

    columns = ["id", "name", "city", "age", "salary"]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn("ingested_at", current_timestamp())

    bronze_path = "s3a://bronze/employees"
    df.write.format("delta").mode("overwrite").save(bronze_path)

    print(f"Written {df.count()} records to Bronze: {bronze_path}")
    df.show()
    return bronze_path

def silver_transformation(spark, bronze_path):
    """
    Silver Layer: Cleaned and transformed data
    Read from Bronze, apply transformations, write to Silver.
    """
    print("=" * 60)
    print("SILVER LAYER: Transforming data")
    print("=" * 60)

    df = spark.read.format("delta").load(bronze_path)

    df_clean = (df
                .withColumn("name", trim(col("name")))
                .withColumn("city", upper(trim(col("city"))))
                .filter(col("salary") > 0)
                .filter(col("age") > 0)
                .withColumn("transformed_at", current_timestamp()))

    silver_path = "s3a://silver/employees_clean"
    df_clean.write.format("delta").mode("overwrite").save(silver_path)

    print(f"Written {df_clean.count()} records to Silver: {silver_path}")
    df_clean.show()
    return silver_path

def gold_aggregation(spark, silver_path):

    df = spark.read.format("delta").load(silver_path)

    df_agg = (df.groupBy("city")
              .agg(
                  count("*").alias("employee_count"),
                  avg("salary").alias("avg_salary"),
                  avg("age").alias("avg_age")
              )
              .withColumn("aggregated_at", current_timestamp()))

    gold_path = "s3a://gold/employee_stats"
    df_agg.write.format("delta").mode("overwrite").save(gold_path)

    print(f"Written {df_agg.count()} records to Gold: {gold_path}")
    df_agg.show()
    return gold_path

def main():
    spark = create_spark_session()

    try:
        bronze_path = bronze_ingestion(spark)
        silver_path = silver_transformation(spark, bronze_path)
        gold_path = gold_aggregation(spark, silver_path)

        print("ETL Pipeline completed successfully!")
        print(f"   Bronze: {bronze_path}")
        print(f"   Silver: {silver_path}")
        print(f"   Gold:   {gold_path}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

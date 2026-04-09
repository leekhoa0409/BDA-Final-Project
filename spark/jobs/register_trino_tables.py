"""Register Delta tables in Trino after ETL writes.

This script uses Trino REST API to register Delta tables.
Run as a Python script (not spark-submit) from within the Docker network.
"""
import logging
import sys
import time
import requests

from config import (
    TRINO_HOST, TRINO_PORT,
    BRONZE_WEATHER_STREAMING_PATH, BRONZE_WEATHER_BATCH_PATH,
    SILVER_WEATHER_PATH, GOLD_WEATHER_DAILY_PATH,
    GOLD_WEATHER_MONTHLY_PATH, GOLD_WEATHER_CITY_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RegisterTrinoTables")

TRINO_URL = f"http://{TRINO_HOST}:{TRINO_PORT}"

# Map table name -> S3 path (Trino uses s3:// not s3a://)
TABLES = {
    "bronze_weather_streaming": BRONZE_WEATHER_STREAMING_PATH.replace("s3a://", "s3://"),
    "bronze_weather_batch": BRONZE_WEATHER_BATCH_PATH.replace("s3a://", "s3://"),
    "silver_weather_clean": SILVER_WEATHER_PATH.replace("s3a://", "s3://"),
    "gold_weather_daily_stats": GOLD_WEATHER_DAILY_PATH.replace("s3a://", "s3://"),
    "gold_weather_monthly_stats": GOLD_WEATHER_MONTHLY_PATH.replace("s3a://", "s3://"),
    "gold_weather_city_summary": GOLD_WEATHER_CITY_PATH.replace("s3a://", "s3://"),
}

SCHEMA_NAME = "default"


def trino_execute(sql):
    """Execute SQL via Trino REST API."""
    resp = requests.post(
        f"{TRINO_URL}/v1/statement",
        data=sql,
        headers={
            "X-Trino-User": "trino",
            "X-Trino-Catalog": "delta",
            "X-Trino-Schema": SCHEMA_NAME,
        },
        timeout=10,
    )
    if resp.status_code != 200:
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}

    result = resp.json()
    while "nextUri" in result:
        time.sleep(0.5)
        resp = requests.get(result["nextUri"], timeout=10)
        result = resp.json()

    return result


def register_table(table_name, s3_path):
    """Register a single Delta table in Trino."""
    sql = (
        f"CALL delta.system.register_table("
        f"schema_name => '{SCHEMA_NAME}', "
        f"table_name => '{table_name}', "
        f"table_location => '{s3_path}')"
    )
    result = trino_execute(sql)

    if result.get("error"):
        error_msg = str(result["error"])
        if "already exists" in error_msg.lower():
            logger.info(f"  {table_name}: already registered (skipping)")
            return True
        elif "not a delta table" in error_msg.lower() or "does not exist" in error_msg.lower():
            logger.warning(f"  {table_name}: path not found or not Delta ({s3_path})")
            return False
        else:
            logger.error(f"  {table_name}: registration failed: {error_msg}")
            return False

    logger.info(f"  {table_name}: registered successfully -> {s3_path}")
    return True


def main():
    logger.info("=" * 60)
    logger.info("Registering Delta tables in Trino")
    logger.info("=" * 60)

    # Ensure schema exists
    trino_execute(f"CREATE SCHEMA IF NOT EXISTS delta.{SCHEMA_NAME}")

    registered = 0
    skipped = 0
    failed = 0

    for table_name, s3_path in TABLES.items():
        success = register_table(table_name, s3_path)
        if success:
            registered += 1
        else:
            skipped += 1

    logger.info(f"Registration complete: {registered} registered/existing, {skipped} skipped")


if __name__ == "__main__":
    main()

# Weather Data Sources — New York City Analysis

Pipeline này tập trung phân tích dữ liệu thời tiết **New York** từ 2 nguồn:

## 1. Batch: Kaggle Historical Hourly Weather Data (2012-2017)

**Dataset**: https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data

### Structure
- Each CSV file = one weather attribute (temperature, humidity, pressure, etc.)
- Rows = hourly timestamps (2012-2017)
- Columns = city names (30 US/Canadian + 6 Israeli cities, bao gồm **New York**)
- Separate `city_attributes.csv` with city metadata (country, lat, lon)
- Bronze layer ingests ALL cities (raw data), Silver layer filters for **New York** only

### Files
- `temperature.csv` - Temperature in Kelvin (converted to Celsius in ETL)
- `humidity.csv` - Relative humidity (%)
- `pressure.csv` - Atmospheric pressure (hPa)
- `wind_speed.csv` - Wind speed (m/s)
- `wind_direction.csv` - Wind direction (degrees)
- `weather_description.csv` - Text weather description
- `city_attributes.csv` - City metadata (City, Country, Latitude, Longitude)

### Upload to MinIO
After downloading from Kaggle, upload to MinIO landing zone:

```bash
# Via MinIO Console UI (http://localhost:9001)
# Upload to: landing/weather/

# Or via MinIO CLI
docker exec minio mc cp /path/to/temperature.csv myminio/landing/weather/
docker exec minio mc cp /path/to/humidity.csv myminio/landing/weather/
docker exec minio mc cp /path/to/pressure.csv myminio/landing/weather/
docker exec minio mc cp /path/to/wind_speed.csv myminio/landing/weather/
docker exec minio mc cp /path/to/wind_direction.csv myminio/landing/weather/
docker exec minio mc cp /path/to/weather_description.csv myminio/landing/weather/
docker exec minio mc cp /path/to/city_attributes.csv myminio/landing/weather/
```

## 2. Streaming: OpenWeather One Call API 3.0 (Real-time)

**API**: One Call API 3.0
**Endpoint**: `https://api.openweathermap.org/data/3.0/onecall?lat=40.7128&lon=-74.0060&units=metric&appid={key}`
**City**: New York (40.7128°N, 74.0060°W)
**Schedule**: Every 5 minutes via NiFi → Kafka → Spark Streaming
**Cost**: Free tier (< 1,000 calls/day, New York only = 288 calls/day)

Data is ingested automatically via NiFi -> Kafka -> Spark Streaming -> Bronze.

## Analysis Focus

All Gold layer tables are filtered for **New York** only:
- `gold_weather_daily_stats` — Daily temperature, humidity, pressure trends
- `gold_weather_monthly_stats` — Monthly/seasonal patterns, rainy vs clear days
- `gold_weather_city_summary` — Latest conditions + historical overview

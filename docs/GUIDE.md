# Huong dan su dung - Weather Data Lakehouse

## Muc luc

1. [Tong quan he thong](#1-tong-quan-he-thong)
2. [Yeu cau he thong](#2-yeu-cau-he-thong)
3. [Buoc 1: Khoi dong he thong](#3-buoc-1-khoi-dong-he-thong)
4. [Buoc 2: Chuan bi du lieu Kaggle](#4-buoc-2-chuan-bi-du-lieu-kaggle)
5. [Buoc 3: Chay Batch Pipeline (Kaggle -> Bronze -> Silver -> Gold)](#5-buoc-3-chay-batch-pipeline)
6. [Buoc 4: Kiem tra ket qua qua Trino](#6-buoc-4-kiem-tra-ket-qua-qua-trino)
7. [Buoc 5: Setup Superset (Truc quan hoa)](#7-buoc-5-setup-superset)
8. [Buoc 6: Setup Streaming Pipeline (NiFi + Kafka)](#8-buoc-6-setup-streaming-pipeline)
9. [Buoc 7: Bat tu dong hoa (Airflow)](#9-buoc-7-bat-tu-dong-hoa)
10. [Buoc 8: Setup Machine Learning Pipeline (Du bao nhiet do)](#10-buoc-8-setup-machine-learning-pipeline)
11. [Quan ly va van hanh](#11-quan-ly-va-van-hanh)
12. [Xu ly su co](#12-xu-ly-su-co)

---

## 1. Tong quan he thong

### Pipeline nay lam gi?

Thu thap va phan tich du lieu thoi tiet thanh pho **New York** tu 2 nguon:

- **Batch (Kaggle)**: Du lieu lich su 2012-2017, 36 thanh pho, moi gio 1 lan do
- **Streaming (OpenWeather API)**: Du lieu thoi gian thuc New York, moi 5 phut

Du lieu di qua 3 tang xu ly (Medallion Architecture) va mo hinh Machine Learning:

```text
Bronze (Du lieu tho) -> Silver (Da lam sach) -> Gold (Da tong hop) -> Trino -> Superset
                                              -> Feature Store -> ML Training -> Model API
```

### He thong gom nhung gi?

| Service | Lam gi | URL |
|---------|--------|-----|
| **MinIO** | Luu tru du lieu (giong Amazon S3) | http://localhost:9001 |
| **Spark** | Xu ly du lieu (ETL) | http://localhost:8080 |
| **Airflow** | Dieu phoi tu dong (scheduler) | http://localhost:8082 |
| **Kafka** | Truyen du lieu thoi gian thuc | - |
| **NiFi** | Lay du lieu tu API | https://localhost:8443 |
| **Trino** | Truy van SQL tren du lieu | http://localhost:8085 |
| **Superset** | Ve bieu do, tao dashboard | http://localhost:8088 |
| **MLflow** | Quan ly model va experiment | http://localhost:5000 |
| **Model API** | API du bao thoi tiet (FastAPI) | http://localhost:8000/docs |
| **PostgreSQL** (x2) | Luu metadata cho Airflow va Superset | - |
| **Redis** | Cache real-time cho ML | - |

---

## 2. Yeu cau he thong

- **Docker** va **Docker Compose** da cai dat
- **RAM**: Toi thieu 12 GB (khuyen nghi 16 GB)
- **Disk**: ~2 GB cho Docker images
- **Kaggle dataset**: Tai tu https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data
- **OpenWeather API key** (mien phi): Dang ky tai https://openweathermap.org/api — chi can cho streaming, co the bo qua

---

## 3. Buoc 1: Khoi dong he thong

### 3.1 Khoi dong tat ca services

```bash
docker compose up -d
```

Lan dau chay se mat 5-10 phut de tai images va build.

### 3.2 Kiem tra trang thai

```bash
docker compose ps
```

**Ket qua mong doi**: Tat ca services hien `Up` hoac `Up (healthy)`. Mot so init services (minio-init, kafka-init, airflow-init) se hien `Exited (0)` — do la binh thuong, chung chi chay 1 lan de khoi tao.

### 3.3 Doi services san sang

Doi khoang 1-2 phut, sau do kiem tra:

```bash
# Kiem tra MinIO
curl -s http://localhost:9000/minio/health/live && echo " -> MinIO OK"

# Kiem tra Airflow
curl -s http://localhost:8082/health | python3 -c "import sys,json; print('Airflow:', json.load(sys.stdin)['scheduler']['status'])"

# Kiem tra Spark Master
curl -s http://localhost:8080/json/ | python3 -c "import sys,json; print('Spark:', json.load(sys.stdin)['status'])"

# Kiem tra Trino
curl -s http://localhost:8085/v1/info | python3 -c "import sys,json; d=json.load(sys.stdin); print('Trino: starting=' + str(d['starting']))"
```

**Tat ca phai tra ve trang thai healthy/OK/ALIVE/starting=False truoc khi tiep tuc.**

### 3.4 Kiem tra buckets MinIO da tao

```bash
docker compose logs minio-init 
```

**Ket qua mong doi**: 5 buckets: `bronze/`, `silver/`, `gold/`, `landing/`, `warehouse/`

---

## 4. Buoc 2: Chuan bi du lieu Kaggle

### 4.1 Tai dataset

Tai tu: https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data

Giai nen va dat cac file CSV vao `data/batch/`:
- `temperature.csv` (nhiet do, don vi Kelvin)
- `humidity.csv` (do am, %)
- `pressure.csv` (ap suat, hPa)
- `wind_speed.csv` (toc do gio, m/s)
- `wind_direction.csv` (huong gio, do)
- `weather_description.csv` (mo ta thoi tiet)
- `city_attributes.csv` (thong tin thanh pho: country, lat, lon)

### 4.2 Upload len MinIO

Du lieu can duoc upload vao MinIO landing zone de Spark co the doc:

```bash
# Cai boto3 trong Spark container (chi can lam 1 lan)
docker exec -u root spark-master pip3 install boto3 -q

# Copy files vao container
for f in data/batch/*.csv; do
    docker cp "$f" spark-master:/tmp/$(basename $f)
done

# Upload len MinIO qua S3 API
docker exec spark-master python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='admin', aws_secret_access_key='admin123456',
    region_name='us-east-1')
files = ['city_attributes.csv','humidity.csv','pressure.csv',
         'temperature.csv','weather_description.csv',
         'wind_direction.csv','wind_speed.csv']
for f in files:
    s3.upload_file('/tmp/' + f, 'landing', 'weather/' + f)
    print(f'Uploaded {f}')
print('Done! Tat ca files da upload.')
"
```

### 4.3 Xac nhan files da upload

```bash
docker exec spark-master python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='admin', aws_secret_access_key='admin123456',
    region_name='us-east-1')
resp = s3.list_objects_v2(Bucket='landing', Prefix='weather/', MaxKeys=20)
print(f'Files trong landing/weather/: {resp.get(\"KeyCount\", 0)}')
for c in resp.get('Contents', []):
    size_mb = c['Size'] / 1024 / 1024
    print(f'  {c[\"Key\"]} ({size_mb:.1f} MB)')
"
```

**Ket qua mong doi**: 7 files, tong ~72 MB

---

## 5. Buoc 3: Chay Batch Pipeline

Day la buoc chinh — xu ly du lieu Kaggle qua 3 tang Bronze -> Silver -> Gold.

### 5.1 Bronze: Nap du lieu tho (Kaggle CSV -> Bronze)

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/batch_bronze_weather.py
```

**Job nay lam gi:**
- Doc 6 file CSV tu `s3a://landing/weather/`
- Cac file CSV co dang "wide" (cot = thanh pho), chuyen sang dang "long" (moi dong = 1 do luong)
- Join voi `city_attributes.csv` de lay thong tin country, lat, lon
- Chuyen nhiet do tu Kelvin sang Celsius
- Ghi vao `s3a://bronze/weather_batch/` (Delta format)

**Ket qua mong doi**: `Written 1,629,108 records to Bronze`

**Thoi gian**: ~2-3 phut

### 5.2 Silver + Gold: Lam sach va tong hop (ETL)

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/weather_etl.py
```

**Job nay lam gi:**

**Silver (Lam sach):**
- Doc tu Bronze (ca batch va streaming)
- Loc chi giu du lieu **New York**
- Loc bo du lieu khong hop le (nhiet do ngoai [-80, 60]°C, do am ngoai [0, 100]%)
- Loai bo ban ghi trung lap (deduplicate)
- Ghi vao `s3a://silver/weather_clean/` bang MERGE (khong mat du lieu cu)

**Gold (Tong hop) — 3 bang:**
- `gold_weather_daily_stats`: Thong ke theo ngay (avg/min/max nhiet do, do am, gio, dieu kien thoi tiet chu dao)
- `gold_weather_monthly_stats`: Thong ke theo thang (xu huong mua, so ngay mua/nang)
- `gold_weather_city_summary`: Tong quan New York (nhiet do moi nhat, trung binh tong the, so ngay theo doi)

**Kiem tra ket qua**:
```bash
docker exec spark-master bash -c 'cat > /tmp/check_counts.py << "PYEOF"
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
PYEOF
spark-submit --master spark://spark-master:7077 /tmp/check_counts.py' 2>/dev/null
```

**Ket qua mong doi**:
```
Bronze Batch: ~1,629,108 records
Bronze Streaming: NOT FOUND (chua chay streaming)
Silver: ~43,000+ records (chi New York, da dedup)
Gold Daily: ~1,854 records (5 nam theo ngay)
Gold Monthly: ~61 records (5 nam theo thang)
Gold City Summary: 1 record (tong quan New York)
```

**Thoi gian**: ~3-5 phut

### 5.3 Dang ky tables trong Trino

```bash
docker exec -u root spark-master pip3 install requests -q
docker exec spark-master python3 /opt/spark/jobs/register_trino_tables.py
```

**Lam gi**: Goi Trino API de dang ky cac Delta tables, cho phep truy van SQL.

**Ket qua mong doi**: `5 registered/existing, 1 skipped` (streaming Bronze chua co data nen skip)

---

## 6. Buoc 4: Kiem tra ket qua qua Trino

### 6.1 Truy van Gold Daily Stats

```bash
docker exec trino trino --execute \
  "SELECT recorded_date, avg_temperature, min_temperature, max_temperature,
          dominant_weather_condition
   FROM delta.default.gold_weather_daily_stats
   ORDER BY recorded_date DESC LIMIT 10"
```

### 6.2 Truy van Gold Monthly Stats

```bash
docker exec trino trino --execute \
  "SELECT year_month, avg_temperature, rainy_day_count, clear_day_count,
          total_measurements
   FROM delta.default.gold_weather_monthly_stats
   ORDER BY year_month DESC LIMIT 12"
```

### 6.3 Truy van City Summary

```bash
docker exec trino trino --execute \
  "SELECT * FROM delta.default.gold_weather_city_summary"
```

### 6.4 Truy van Trino tuong tac (CLI)

```bash
docker exec -it trino trino
```

Trong Trino CLI, thu:
```sql
SHOW TABLES FROM delta.default;
SELECT COUNT(*) FROM delta.default.silver_weather_clean;
SELECT AVG(avg_temperature) FROM delta.default.gold_weather_monthly_stats WHERE year_month LIKE '2017%';
```

Go `quit` de thoat.

---

## 7. Buoc 5: Setup Superset

### 7.1 Tao tai khoan admin

```bash
docker exec superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@lakehouse.local --password admin
```

### 7.2 Tao ket noi Trino

```bash
bash superset/setup_trino_connection.sh
```

**Ket qua mong doi**: `Trino Lakehouse -> Created!`

### 7.3 Su dung Superset

1. Mo **http://localhost:8088**
2. Dang nhap: `admin` / `admin`
3. Vao **SQL Lab** (menu trai) -> Chon database **"Trino Lakehouse"**
4. Chay thu query:

```sql
SELECT recorded_date, avg_temperature, dominant_weather_condition
FROM gold_weather_daily_stats
WHERE recorded_date >= DATE '2017-01-01'
ORDER BY recorded_date
```

### 7.4 Tao Chart

1. **SQL Lab** -> Chay query -> Click **"Create Chart"**
2. Hoac: **Charts** -> **+ Chart** -> Chon dataset -> Chon loai bieu do

**Goi y bieu do:**
- **Line Chart**: `avg_temperature` theo `recorded_date` (xu huong nhiet do)
- **Bar Chart**: `avg_temperature` theo `year_month` (nhiet do theo thang)
- **Pie Chart**: `clear_day_count` vs `rainy_day_count` (ti le thoi tiet)

### 7.5 Tao Dashboard

1. **Dashboards** -> **+ Dashboard**
2. Keo tha cac Charts da tao vao dashboard
3. Luu lai

---

## 8. Buoc 6: Setup Streaming Pipeline

> **Luu y**: Buoc nay la tuy chon. Pipeline batch (Buoc 3-5) da du de phan tich du lieu lich su.
> Streaming chi can neu ban muon du lieu thoi gian thuc.

### 8.1 Yeu cau

- OpenWeather API key (mien phi tai https://openweathermap.org/api)
- Cap nhat key trong file `.env`: `OPENWEATHER_API_KEY=your_key_here`

### 8.2 Tao NiFi Flow

```bash
python3 nifi/scripts/create-weather-flow.py \
  --nifi-url https://localhost:8443 \
  --api-key b7c38c6d5f1a92667cb556dfb1d5ec03
```

**Lam gi**: Tao flow trong NiFi gom 4 processors:
- **Trigger New York** (GenerateFlowFile): Tao FlowFile moi 5 phut de trigger InvokeHTTP
- **Fetch New York** (InvokeHTTP): Goi OpenWeather API 2.5, auto-terminate relationship `Original`
- **Publish to Kafka** (PublishKafka_2_6): Gui JSON response vao Kafka topic `weather-raw`, auto-terminate `success`
- **Log Failures** (LogAttribute): Ghi log loi, auto-terminate `success`

> **Luu y**: InvokeHTTP trong NiFi 1.x can co FlowFile dau vao de trigger — khong tu goi API.
> Vi vay can co GenerateFlowFile o truoc de tao FlowFile dinh ky.

### 8.3 Start NiFi Processors

1. Mo **https://localhost:8443/nifi/**
2. Dang nhap: `admin` / `admin123456789`
3. Double-click vao process group **"OpenWeather One Call to Kafka"**
4. Nhan **Ctrl+A** (chon tat ca processors)
5. Click nut **Start** (Play) tren toolbar

**Sau khi start**: NiFi se tu dong goi API moi 5 phut va gui du lieu vao Kafka.

### 8.X Kiem tra du lieu da vao Kafka

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic weather-raw \
  --from-beginning \
  --timeout-ms 5000
```

**Ket qua mong doi**: JSON weather data cua New York (temp, humidity, pressure...)

### 8.4 Start Spark Streaming

```bash
docker exec airflow-webserver airflow dags trigger weather_streaming
```

**Lam gi**: Chay Spark Structured Streaming, doc tu Kafka topic `weather-raw`, ghi vao `s3a://bronze/weather_streaming/` (Delta format). Job chay lien tuc 24h.

### 8.5 Xac nhan streaming hoat dong

Doi 5-10 phut (cho NiFi goi API it nhat 1 lan), sau do:

```bash
docker exec spark-master python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='admin', aws_secret_access_key='admin123456',
    region_name='us-east-1')
resp = s3.list_objects_v2(Bucket='bronze', Prefix='weather_streaming/', MaxKeys=10)
print(f'Streaming Bronze files: {resp.get(\"KeyCount\", 0)}')
"
```

**Ket qua mong doi**: `Streaming Bronze files: > 0`

---

## 9. Buoc 7: Bat tu dong hoa

### 9.1 Hieu ve Airflow DAGs

| DAG | Chay khi nao | Lam gi |
|-----|-------------|--------|
| `weather_pipeline` | Tu dong moi 1 gio | Check CSV moi -> Ingest -> ETL -> Validate -> Register Trino |
| `weather_streaming` | Trigger thu cong 1 lan | Chay Spark Streaming lien tuc (doc Kafka -> Bronze) |

### 9.2 Bat tu dong

```bash
# Bat pipeline tu dong chay moi gio
docker exec airflow-webserver airflow dags unpause weather_pipeline

# Bat streaming (neu da setup o Buoc 6)
docker exec airflow-webserver airflow dags unpause weather_streaming
```

### 9.3 Kiem tra tren Airflow UI

1. Mo **http://localhost:8082**
2. Dang nhap: `airflow` / `airflow`
3. Trang chinh hien danh sach DAGs va trang thai

**Y nghia cac cot:**
- **DAG**: Ten pipeline
- **Schedule**: Lich chay (vd: `@hourly`)
- **Last Run**: Lan chay gan nhat
- **Runs**: So lan da chay (xanh = thanh cong, do = that bai)

4. Click vao ten DAG -> Tab **"Graph"** de xem luong tasks

### 9.4 Sau khi bat tu dong

**Ban khong can lam gi them.** Moi gio, Airflow se:

1. Kiem tra co CSV moi trong `landing/weather/` khong
2. Neu co -> chay `batch_bronze_weather.py`
3. Chay `weather_etl.py` (Silver + Gold)
4. Validate du lieu Bronze/Silver/Gold
5. Dang ky tables trong Trino

Neu ban upload CSV moi vao MinIO -> pipeline se tu dong xu ly trong vong 1 gio.

---

---

## 10. Buoc 8: Setup Machine Learning Pipeline

> **Luu y**: He thong tich hop san ML Pipeline de du bao nhiet do New York City 24 gio toi dua tren du lieu lich su va hien tai.
> De biet chi tiet ve kien truc, hay xem [ML_DOCS.md](ML_DOCS.md).

### 10.1 Tao Feature Store

Chuyen doi du lieu tu Silver/Gold thanh cac dac trung (features) de huan luyen mo hinh:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/jobs/feature_store.py
```

### 10.2 Huan luyen Mo hinh (Train Model)

Chay pipeline huan luyen va danh gia (RandomForest, GradientBoosting, Ridge), ghi log ket qua len MLflow:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/jobs/ml_training.py
```

Ban co the xem ket qua huan luyen tai **MLflow UI**: http://localhost:5000

### 10.3 Chuyen Mo hinh sang Production

Truoc khi thu nghiem API, ban can chuyen mo hinh (version 1) sang trang thai Production va khoi dong lai container server:

```bash
curl -X POST "http://localhost:8000/models/1/transition?stage=Production"
docker restart model-serving
```

### 10.4 Thu nghiem API Du bao

Gui request den Model Serving API de lay du bao nhiet do:

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "temperature": 25.0,
    "humidity": 60.0,
    "pressure": 1013.0,
    "wind_speed": 3.5,
    "hour": 14,
    "day_of_week": 3,
    "month": 4
  }'
```

Xem tai lieu API (Swagger UI): http://localhost:8000/docs

### 10.5 Bat tu dong hoa ML Pipeline

ML pipeline cung co 1 DAG trong Airflow de tu dong cap nhat du lieu feature, huan luyen lai mo hinh tren Airflow moi ngay. Tuy nhien buoc chuyen stage model sang Production moi se can lam thu cong hoac them automation trong MLflow:

```bash
# Bat pipeline ML tu dong
docker exec airflow-webserver airflow dags unpause ml_pipeline
```

---

## 11. Quan ly va van hanh

### Xem logs

```bash
# Logs cua Spark
docker compose logs -f spark-master

# Logs cua Airflow
docker compose logs -f airflow-webserver

# Logs cua NiFi
docker compose logs -f nifi

# Logs cua Kafka
docker compose logs -f kafka
```

### Dung / Khoi dong lai

```bash
# Dung tat ca (giu du lieu)
docker compose down

# Khoi dong lai
docker compose up -d

# Dung va xoa toan bo du lieu (reset hoan toan)
docker compose down -v
```

### Rebuild sau khi sua code

```bash
# Sua Spark jobs (spark/jobs/*.py) -> Rebuild Spark
docker compose up -d --build spark-master spark-worker

# Sua Airflow DAGs -> Khong can rebuild (volume mount tu dong cap nhat)

# Sua Dockerfile -> Rebuild service tuong ung
docker compose up -d --build airflow-webserver airflow-scheduler
```

### Chay ETL thu cong

```bash
# Chay toan bo ETL
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/weather_etl.py

# Trigger Airflow DAG thu cong
docker exec airflow-webserver airflow dags trigger weather_pipeline
```

---

## 12. Xu ly su co

### 12.1 Kiem tra tong quan trang thai he thong

```bash
# Kiem tra tat ca containers
docker compose ps

# Kiem tra du lieu tung layer
docker exec spark-master python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='admin', aws_secret_access_key='admin123456',
    region_name='us-east-1')
for bucket in ['bronze', 'silver', 'gold']:
    resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=100)
    count = resp.get('KeyCount', 0)
    print(f'{bucket.upper()}: {count} files')
"
```

### 12.2 Chay validation de kiem tra du lieu

```bash
# Validate Bronze (row counts, null checks, freshness)
docker exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/spark/jobs/validate_weather_bronze.py

# Validate Silver (temperature range, humidity, distinct cities)
docker exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/spark/jobs/validate_weather_silver.py

# Validate Gold (cross-layer counts, temperature consistency min <= avg <= max)
docker exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/spark/jobs/validate_weather_gold.py
```

**Ket qua mong doi**: Tat ca phai hien `validation PASSED`

### 12.3 Spark Master/Worker khong start

```bash
docker compose logs spark-master | tail -10
```

- Neu `Permission denied`: Rebuild image: `docker compose up -d --build spark-master spark-worker`
- Neu `OOM` (Out of Memory): Tang RAM hoac giam `SPARK_WORKER_MEMORY` trong docker-compose.yml

### 12.4 Airflow DAG fail

**Buoc 1: Xac dinh DAG va task nao fail**

```bash
# Xem trang thai tasks cua weather_pipeline
docker exec airflow-scheduler airflow tasks states-for-dag-run weather_pipeline <run_id>

# Xem trang thai tasks cua weather_streaming
docker exec airflow-scheduler airflow tasks states-for-dag-run weather_streaming <run_id>
```

**Buoc 2: Doc logs cua task fail**

```bash
# Logs nam o duong dan nay trong container
docker exec airflow-scheduler cat \
  /opt/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=1.log \
  | tail -30
```

**Buoc 3: Xu ly theo loi cu the**

#### Loi: `Task received SIGTERM signal` (Zombie kill)

**Nguyen nhan**: Spark job chay lau hon `scheduler_zombie_task_threshold` (mac dinh 300 giay = 5 phut). Scheduler tuong task da chet va kill no.

**Cach fix**: Tang zombie threshold trong `docker-compose.yml` (da config san 3600s):
```yaml
environment:
  AIRFLOW__SCHEDULER__SCHEDULER_ZOMBIE_TASK_THRESHOLD: '3600'
```
Sau do restart scheduler: `docker compose restart airflow-scheduler`

#### Loi: `Failed to find data source: kafka`

**Nguyen nhan**: Airflow container thieu Kafka JARs trong PySpark.

**Cach fix**: Rebuild Airflow images (Dockerfile da bao gom cac JARs can thiet):
```bash
docker compose up -d --build airflow-webserver airflow-scheduler
```

#### Loi: `ClassNotFoundException: delta.DefaultSource`

**Nguyen nhan**: Airflow container thieu Delta Lake JARs.

**Cach fix**: Tuong tu — rebuild Airflow images:
```bash
docker compose up -d --build airflow-webserver airflow-scheduler
```

#### Loi: `S3AFileSystem not found`

**Nguyen nhan**: Thieu `hadoop-aws` va `aws-java-sdk-bundle` JARs.

**Cach fix**: Rebuild Airflow images (Dockerfile da bao gom):
```bash
docker compose up -d --build airflow-webserver airflow-scheduler
```

> **Luu y chung**: Neu gap loi JAR/ClassNotFound, rebuild Airflow la cach fix chac chan nhat.
> Dockerfile da cau hinh san viec copy tat ca JARs can thiet (Delta, Kafka, Hadoop-AWS) vao PySpark.

#### DAG bi paused (khong chay du da trigger)

```bash
# Kiem tra trang thai paused
docker exec airflow-scheduler airflow dags list

# Unpause
docker exec airflow-webserver airflow dags unpause weather_pipeline
docker exec airflow-webserver airflow dags unpause weather_streaming
```

#### Clear tasks fail de chay lai

```bash
# Clear tat ca tasks fail cua 1 DAG (se chay lai tu dau)
docker exec airflow-scheduler airflow tasks clear weather_pipeline \
  -s 2026-03-28 -e 2026-03-29 -y

# Hoac trigger DAG run moi
docker exec airflow-webserver airflow dags trigger weather_pipeline
```

### 12.5 NiFi khong gui du lieu vao Kafka

**Buoc 1: Kiem tra processors co dang Running**

Mo https://localhost:8443/nifi/, vao process group "OpenWeather One Call to Kafka".
Tat ca 4 processors phai co icon play xanh. Neu co warning (tam giac vang), hover vao de doc loi.

**Buoc 2: Kiem tra tung van de**

#### NiFi start nhung khong fetch API (In/Out = 0)

**Nguyen nhan**: Thieu GenerateFlowFile processor. InvokeHTTP can FlowFile dau vao de trigger.

**Cach fix**: Xoa flow cu, chay lai script tao flow:
```bash
python3 nifi/scripts/create-weather-flow.py \
  --nifi-url https://localhost:8443 \
  --api-key <YOUR_API_KEY>
```

#### Warning: `Remote URL is invalid because Not a valid URL`

**Nguyen nhan**: URL chua ky tu dac biet (khoang trang trong ten thanh pho).

**Cach fix**: Script da su dung `urllib.parse.quote()` de URL-encode. Xoa flow cu va tao lai.

#### Warning: `Delivery Guarantee is invalid`

**Nguyen nhan**: Kafka Transactional Producer khong tuong thich voi single-node Kafka.

**Cach fix**: Script da cau hinh `use-transactions: false` va `acks: 1`. Xoa flow cu va tao lai.

#### Publish to Kafka: `TimeoutException awaiting InitProducerId`

**Nguyen nhan**: NiFi dang su dung Kafka Transactions nhung single-node Kafka khong ho tro.

**Cach fix**: Xoa flow cu, chay lai script (da fix `use-transactions: false`).

#### Cach xoa flow cu de tao lai

1. Vao process group "OpenWeather One Call to Kafka"
2. Nhan **Ctrl+A** -> Click **Stop** (nut vuong) de dung tat ca processors
3. Right-click tung connection -> **Empty queue** (lam sach queue)
4. Quay lai root (click "NiFi Flow" o breadcrumb duoi cung)
5. Right-click process group -> **Delete**

**Buoc 3: Xac nhan du lieu da vao Kafka**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic weather-raw \
  --from-beginning \
  --timeout-ms 5000
```

### 12.6 Trino khong query duoc

```bash
# Kiem tra tables da dang ky
docker exec trino trino --execute "SHOW TABLES FROM delta.default"
```

- Neu khong co tables: Chay lai `docker exec spark-master python3 /opt/spark/jobs/register_trino_tables.py`
- Neu loi connection: Kiem tra Trino dang chay: `docker compose ps trino`

### 12.7 Superset khong ket noi Trino

- Kiem tra database connection: **Settings** -> **Database Connections** -> **Trino Lakehouse** -> **Test Connection**
- Neu loi: Chay lai `bash superset/setup_trino_connection.sh`

### 12.8 MinIO khong doc duoc files

```bash
docker exec spark-master python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
    aws_access_key_id='admin', aws_secret_access_key='admin123456',
    region_name='us-east-1')
for bucket in ['landing', 'bronze', 'silver', 'gold']:
    resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=50)
    count = resp.get('KeyCount', 0)
    print(f'{bucket}: {count} files')
"
```

- Su dung boto3 thay vi `mc` de upload (mc co the gay loi inconsistency)

### 12.9 Streaming Bronze co 0 files (Kafka co data nhung Bronze trong)

**Nguyen nhan**: Spark Streaming job chua chay hoac da bi kill.

**Kiem tra**:
```bash
# Xem streaming task co dang running
docker exec airflow-scheduler airflow tasks states-for-dag-run weather_streaming <run_id>
```

**Cach fix**:
```bash
# Trigger lai streaming DAG
docker exec airflow-webserver airflow dags trigger weather_streaming
```

### 12.10 Reset toan bo pipeline (chay lai tu dau)

```bash
# 1. Dung tat ca
docker compose down

# 2. Xoa toan bo data (volumes)
docker compose down -v

# 3. Khoi dong lai
docker compose up -d

# 4. Doi services san sang (1-2 phut)
# 5. Upload lai CSV (Buoc 4.2)
# 6. Chay lai pipeline (Buoc 5)
```

---

## Tham khao nhanh

### URLs

| Service | URL | Dang nhap |
|---------|-----|-----------|
| MinIO Console | http://localhost:9001 | admin / admin123456 |
| Airflow | http://localhost:8082 | airflow / airflow |
| Superset | http://localhost:8088 | admin / admin |
| Spark Master | http://localhost:8080 | — |
| Trino | http://localhost:8085 | — |
| NiFi | https://localhost:8443 | admin / admin123456789 |
| MLflow UI | http://localhost:5000 | — |
| Model API Docs | http://localhost:8000/docs | — |

### Lenh hay dung

```bash
docker compose up -d                 # Khoi dong
docker compose down                  # Dung
docker compose ps                    # Trang thai
docker compose logs -f <service>     # Xem logs

# Spark jobs
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/<job>.py

# Trino CLI
docker exec -it trino trino

# Airflow
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver airflow dags trigger <dag_id>
docker exec airflow-webserver airflow dags unpause <dag_id>
```

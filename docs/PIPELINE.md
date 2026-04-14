# Quy trinh xay dung Data Warehouse voi Airflow (Batch Pipeline)

## Tong quan

He thong su dung **Apache Airflow** de dieu phoi toan bo quy trinh xay dung Data Warehouse tu du lieu thoi tiet lich su (Kaggle CSV). Quy trinh duoc thiet ke theo kien truc **Medallion** (Landing -> Bronze -> Silver -> Gold) ket hop mo hinh **Star Schema** (Dim/Fact), dam bao du lieu duoc xu ly co he thong, kiem tra chat luong o moi tang, va san sang phuc vu truy van phan tich.

DAG `weather_pipeline` chay tu dong **moi 1 gio**, gom 6 giai doan:

```text
  Stage 1            Stage 2           Stage 3          Stage 4          Stage 5
+-----------+    +-------------+    +-----------+    +------------+    +----------+
| Check CSV | -> | Ingest      | -> | Validate  | -> | ETL        | -> | Validate |
| Landing   |    | Landing ->  |    | Bronze    |    | Bronze ->  |    | Silver   |
| Zone      |    | Bronze      |    | Quality   |    | Silver ->  |    | Gold     |
|           |    |             |    |           |    | Dim/Fact   |    | Quality  |
+-----------+    +-------------+    +-----------+    +------------+    +----------+
                                                           |
                                                           v          Stage 6
                                                     +------------+
                                                     | Register   |
                                                     | Trino      |
                                                     | Tables     |
                                                     +------------+
```

---

## Stage 1: Kiem tra Landing Zone (`check_csv_in_landing`)

**Muc dich**: Xac dinh co du lieu CSV moi can xu ly hay khong, tranh chay pipeline khi khong co gi moi.

**Co che hoat dong**:

Airflow su dung `BranchPythonOperator` — mot operator dac biet cho phep re nhanh (branching) dua tren ket qua tra ve cua ham Python. Ham `check_landing_zone()` ket noi truc tiep den MinIO (S3-compatible storage) qua thu vien `boto3`, liet ke cac object trong bucket `landing/weather/`:

```python
s3 = boto3.client("s3", endpoint_url="http://minio:9000", ...)
resp = s3.list_objects_v2(Bucket="landing", Prefix="weather/", MaxKeys=10)
files = [o["Key"] for o in resp.get("Contents", [])
         if o["Key"].endswith(".csv") and "/processed/" not in o["Key"]]
```

- Neu tim thay it nhat 1 file `.csv` chua nam trong thu muc `processed/` -> tra ve `'ingest_csv_to_bronze'` (chay nap du lieu)
- Neu khong co file nao -> tra ve `'skip_csv_ingest'` (bo qua, nhay thang den validate)

**Ly do thiet ke**: Pipeline chay moi gio nhung du lieu Kaggle chi upload 1 lan. Viec kiem tra truoc giup tranh chay lai buoc ingest khong can thiet, tiet kiem tai nguyen Spark cluster.

**Ket qua**: 1 trong 2 nhanh duoc chon:
- `ingest_csv_to_bronze` -> di vao Stage 2
- `skip_csv_ingest` (EmptyOperator) -> nhay thang den Stage 3

---

## Stage 2: Nap du lieu vao Bronze (`ingest_csv_to_bronze`)

**Muc dich**: Chuyen du lieu tu dinh dang goc (CSV wide-format) sang dinh dang chuan cua Lakehouse (Delta Lake long-format) trong tang Bronze.

**Spark Job**: `batch_bronze_weather.py`

**Quy trinh chi tiet**:

### Buoc 2.1: Doc 6 file CSV tu Landing Zone

Dataset Kaggle co cau truc **wide-format** — moi file CSV la mot thuoc tinh thoi tiet, trong do:
- Dong = timestamp (moi gio tu 2012-2017)
- Cot = ten thanh pho (36 thanh pho)

```text
datetime,           New York, Los Angeles, Chicago, ...
2012-10-01 12:00,   286.15,   295.37,      284.82, ...
2012-10-01 13:00,   286.73,   295.92,      285.11, ...
```

Spark doc tung file qua `spark.read.csv()` voi `header=true` va `inferSchema=true`.

### Buoc 2.2: Unpivot tu wide sang long format

Dung ham `stack()` cua Spark SQL de chuyen doi:

```text
TRUOC (wide):  1 dong = 1 timestamp x 36 cities
SAU (long):    1 dong = 1 do luong (timestamp, city, value)
```

Vi du voi `temperature.csv`:
```text
TRUOC: datetime=2012-10-01 12:00, New York=286.15, Chicago=284.82
SAU:   datetime=2012-10-01 12:00, city=New York,  temperature=286.15
       datetime=2012-10-01 12:00, city=Chicago,   temperature=284.82
```

Ket qua: Tu ~45,000 dong x 36 cot thanh ~1,600,000 dong x 3 cot cho moi file.

### Buoc 2.3: Join cac thuoc tinh

6 DataFrame (temperature, humidity, pressure, wind_speed, wind_direction, weather_description) duoc join tren khoa `(datetime, city)` bang `outer join` de giu tat ca du lieu du co gia tri NULL.

### Buoc 2.4: Bo sung metadata

- Join voi `city_attributes.csv` de lay `country`, `latitude`, `longitude`
- Them cot `source = "kaggle_csv"` (phan biet voi du lieu streaming)
- Them cot `ingested_at = current_timestamp()` (thoi diem nap)
- Ep kieu `datetime` sang Timestamp

### Buoc 2.5: Ghi vao Bronze Delta Table

```python
result.write.format("delta").mode("append").save("s3a://bronze/weather_batch")
```

Su dung `mode("append")` de khong ghi de du lieu cu neu chay lai. Du lieu duoc luu duoi dang Delta Lake tren MinIO (S3), cho phep:
- **ACID transactions**: Dam bao ghi nhat quan
- **Schema enforcement**: Tu dong kiem tra schema
- **Time travel**: Truy van du lieu tai bat ky thoi diem nao trong qua khu

**Nguyen tac Bronze**: Giu nguyen du lieu tho — khong loc, khong doi don vi, khong loai bo ban ghi. Nhiet do van la Kelvin, tat ca 36 thanh pho deu duoc giu lai.

**Ket qua**: ~1,629,108 records tai `s3a://bronze/weather_batch/`

---

## Stage 3: Kiem tra chat luong Bronze (`validate_bronze`)

**Muc dich**: Xac minh du lieu Bronze hop le TRUOC khi ETL doc no, phat hien som cac van de tu nguon du lieu.

**Spark Job**: `validate_weather_bronze.py`

**Cac kiem tra thuc hien**:

| Kiem tra | Mo ta | Muc do |
|----------|-------|--------|
| Row count > 0 | Dam bao bang khong rong | Critical |
| NULL check: `dt`, `name` (streaming) | Timestamp va ten thanh pho khong duoc null | Warning |
| NULL check: `city`, `datetime` (batch) | Khoa chinh khong duoc null | Warning |
| Data freshness | Kiem tra `ingested_at` gan nhat | Info |

**Trigger rule**: `none_failed_min_one_success` — task nay chay du la nhanh `ingest_csv` hay `skip_csv` duoc chon o Stage 1. Dieu nay dam bao:
- Neu vua ingest xong -> validate du lieu moi
- Neu skip ingest -> van validate du lieu cu da co

**Luu y**: Validate Bronze chi canh bao (warning), khong lam fail pipeline, vi du lieu Bronze co the chua day du (streaming chua chay) nhung van du de ETL xu ly.

---

## Stage 4: ETL — Xay dung Silver va Gold (`run_weather_etl`)

**Muc dich**: Chuyen doi du lieu tho (Bronze) thanh du lieu sach (Silver), sau do tong hop thanh cac bang phan tich (Dim/Fact) theo mo hinh Star Schema.

**Spark Job**: `weather_etl.py`

Day la buoc phuc tap nhat, gom 5 giai doan con:

### 4.1 Silver Transformation (Lam sach du lieu)

**Doc tu Bronze**: Doc ca 2 nguon `bronze/weather_batch` (Kaggle) va `bronze/weather_streaming` (API) neu co, sau do `UNION` thanh 1 DataFrame duy nhat.

**Chuan hoa schema**: Du lieu tu 2 nguon co cau truc khac nhau:
- Kaggle: flat CSV, nhiet do Kelvin, datetime dang string
- API: nested JSON, nhiet do Celsius, timestamp dang unix epoch

Ca hai duoc map ve cung 1 schema thong nhat voi cac cot: `city, country, latitude, longitude, temperature, humidity, pressure, wind_speed, weather_condition, recorded_at, source`.

**Loc du lieu (Filter)**:
```python
df.filter(col("city") == "New York")       # Chi phan tich New York
  .filter(col("temperature").between(-80, 60))  # Nhiet do hop le (°C)
  .filter(col("humidity").between(0, 100))       # Do am hop le (%)
  .filter(col("city").isNotNull())               # Khong chap nhan city NULL
```

Tu ~1,629,108 records (36 thanh pho) loc con ~43,000+ records (chi New York).

**Kiem tra chat luong (Great Expectations)**:
```python
validate_with_ge(df_clean)  # Kiem tra NULL, range, consistency
```

Neu du lieu khong dat chat luong -> raise Exception, pipeline dung lai.

**Loai bo trung lap (Deduplicate)**:
```python
Window.partitionBy("city", "recorded_dt").orderBy(desc("transformed_at"))
# Giu ban ghi moi nhat cho moi (city, recorded_dt)
```

Tranh ghi trung khi pipeline chay nhieu lan tren cung du lieu.

**Ghi vao Silver bang MERGE (Upsert)**:
```python
silver_table.merge(df_clean, "target.city = source.city AND target.recorded_dt = source.recorded_dt")
    .whenMatchedUpdateAll()      # Neu da co -> cap nhat
    .whenNotMatchedInsertAll()   # Neu chua co -> them moi
```

Su dung Delta Lake MERGE thay vi overwrite de:
- Khong mat du lieu lich su khi chay lai
- Chi cap nhat nhung ban ghi thay doi
- Ho tro incremental loading (nap them du lieu moi ma khong anh huong du lieu cu)

**Ket qua**: ~43,000+ records tai `s3a://silver/weather_clean/`

### 4.2 Dimension Tables (Mo hinh Star Schema)

Xay dung 2 bang dimension de phuc vu truy van phan tich:

**`dim_city`** — Dimension thanh pho:
```
city_id | city     | country | latitude  | longitude
1       | New York | US      | 40.7128   | -74.0060
```
- Tao `city_id` bang `row_number()` de lam surrogate key
- `dropDuplicates(["city"])` dam bao moi thanh pho chi xuat hien 1 lan

**`dim_date`** — Dimension ngay:
```
date_id | recorded_date | year | month | day
1       | 2012-10-01    | 2012 | 10    | 1
2       | 2012-10-02    | 2012 | 10    | 2
...
```
- Tach ngay thanh year, month, day de ho tro truy van theo thoi gian
- `dropDuplicates()` tren `recorded_date`

**Ly do dung Star Schema**: Cho phep truy van nhanh bang cach join fact voi dim qua surrogate key (so nguyen), thay vi join tren string (ten thanh pho, ngay). Dong thoi tach biet du lieu mo ta (dimension) va du lieu do luong (fact).

### 4.3 Fact Tables (Bang tong hop phan tich)

**`fact_weather_daily_stats`** — Thong ke thoi tiet theo ngay:

Aggregate tu Silver theo `(city, country, recorded_date)`:

| Cot | Cong thuc | Y nghia |
|-----|-----------|---------|
| `avg_temperature` | `AVG(temperature)` | Nhiet do trung binh ngay |
| `min_temperature` | `MIN(temperature)` | Nhiet do thap nhat |
| `max_temperature` | `MAX(temperature)` | Nhiet do cao nhat |
| `avg_humidity` | `AVG(humidity)` | Do am trung binh |
| `avg_pressure` | `AVG(pressure)` | Ap suat trung binh |
| `avg_wind_speed` | `AVG(wind_speed)` | Toc do gio trung binh |
| `measurement_count` | `COUNT(*)` | So lan do trong ngay |
| `rolling_avg_temp` | `AVG(avg_temp) OVER (7 ngay)` | Trung binh truot 7 ngay |
| `temp_volatility` | `STDDEV(avg_temp) OVER (7 ngay)` | Bien dong nhiet do 7 ngay |
| `dominant_weather_condition` | `row_number() + COUNT` | Dieu kien thoi tiet xuat hien nhieu nhat trong ngay |
| `city_id` | Join `dim_city` | Khoa ngoai lien ket dimension thanh pho |
| `date_id` | Join `dim_date` | Khoa ngoai lien ket dimension ngay |

Partition theo `year/month` va OPTIMIZE + ZORDER tren `(city_id, date_id)` de tang hieu suat truy van.

**Ket qua**: ~1,854 records (5 nam x 365 ngay)

---

**`fact_weather_monthly_stats`** — Thong ke thoi tiet theo thang:

Aggregate tu Silver theo `(city, country, year_month)`:

| Cot | Y nghia |
|-----|---------|
| `avg/min/max_temperature` | Nhiet do trung binh/thap nhat/cao nhat trong thang |
| `avg_humidity`, `avg_wind_speed` | Do am, toc do gio trung binh thang |
| `total_measurements` | Tong so lan do trong thang |
| `rainy_day_count` | So ngay mua (weather_condition chua "rain") |
| `clear_day_count` | So ngay nang (weather_condition chua "clear") |

**Ket qua**: ~61 records (5 nam x 12 thang)

---

**`fact_weather_city_summary`** — Tong quan thanh pho:

Tong hop toan bo lich su cua New York thanh **1 ban ghi duy nhat**:

| Cot | Y nghia |
|-----|---------|
| `latest_temperature` | Nhiet do do gan nhat |
| `latest_humidity` | Do am do gan nhat |
| `latest_recorded_at` | Thoi diem do gan nhat |
| `avg_temperature_overall` | Nhiet do trung binh toan bo lich su |
| `total_days_tracked` | Tong so ngay co du lieu |
| `dominant_weather_overall` | Dieu kien thoi tiet xuat hien nhieu nhat qua toan bo lich su |

**Ket qua**: 1 record

### 4.4 Optimization

Sau khi ghi fact tables, thuc hien toi uu hoa Delta Lake:

```python
OPTIMIZE delta.`path` ZORDER BY (city_id, date_id)
```
- **OPTIMIZE**: Gop cac file nho thanh file lon, giam so luong file can doc
- **ZORDER**: Sap xep du lieu theo `city_id` va `date_id`, giup truy van loc theo thanh pho hoac khoang thoi gian nhanh hon

```python
VACUUM delta.`path` RETAIN 168 HOURS
```
- **VACUUM**: Xoa cac file cu khong con duoc tham chieu (giu lai 7 ngay de ho tro time travel)

---

## Stage 5: Kiem tra chat luong Silver va Gold (`validate_outputs`)

**Muc dich**: Dam bao du lieu dau ra cua ETL dung va nhat quan truoc khi phuc vu truy van.

Hai task chay **song song** trong TaskGroup `validate_outputs`:

### validate_silver (`validate_weather_silver.py`)

| Kiem tra | Dieu kien | Hanh dong khi fail |
|----------|-----------|-------------------|
| Row count | > 0 | Raise ValueError |
| Required columns | city, country, temperature, humidity, ... | Raise ValueError |
| Temperature range | [-80, 60] °C | Raise ValueError |
| Humidity range | [0, 100] % | Raise ValueError |
| NULL city | count = 0 | Raise ValueError |

### validate_gold (`validate_weather_gold.py`)

| Kiem tra | Dieu kien | Hanh dong khi fail |
|----------|-----------|-------------------|
| Row count | > 0 | Raise ValueError |
| Required columns | city, recorded_date, avg_temperature, ... | Raise ValueError |
| Cross-validation | sum(measurement_count) == Silver row count | Raise ValueError |
| Temperature consistency | min <= avg <= max cho moi dong | Raise ValueError |

**Cross-validation** la kiem tra quan trong nhat: tong `measurement_count` cua tat ca ngay trong Gold phai bang tong so ban ghi trong Silver. Neu khong khop -> co loi mat du lieu hoac trung lap trong qua trinh aggregate.

---

## Stage 6: Dang ky tables trong Trino (`register_trino_tables`)

**Muc dich**: Lam cho tat ca Delta tables co the truy van bang SQL thong qua Trino, phuc vu cho Superset dashboard va phan tich ad-hoc.

**Script**: `register_trino_tables.py`

**Co che**: Goi Trino REST API de dang ky tung Delta table:

```python
CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'fact_weather_daily_stats',
    table_location => 's3://gold/fact_weather_daily_stats'
)
```

**8 tables duoc dang ky**:

| Tang | Table | Mo ta |
|------|-------|-------|
| Bronze | `bronze_weather_batch` | Du lieu tho Kaggle |
| Bronze | `bronze_weather_streaming` | Du lieu tho API (neu co) |
| Silver | `silver_weather_clean` | Du lieu da lam sach |
| Gold | `dim_city` | Dimension: thanh pho |
| Gold | `dim_date` | Dimension: ngay |
| Gold | `fact_weather_daily_stats` | Fact: thong ke ngay |
| Gold | `fact_weather_monthly_stats` | Fact: thong ke thang |
| Gold | `fact_weather_city_summary` | Fact: tong quan thanh pho |

**Xu ly loi thong minh**:
- Table da ton tai -> skip (idempotent)
- Path chua co du lieu -> skip voi warning (streaming Bronze chua chay)
- Loi khac -> log error nhung khong fail pipeline

**Trigger rule**: `none_failed_min_one_success` — chay ngay sau ETL, khong doi validate xong. Ly do: viec dang ky table khong phu thuoc vao ket qua validate, va nguoi dung co the bat dau truy van som hon.

---

## Tong ket luong du lieu

```text
Kaggle CSV (7 files, 72 MB)
    |
    | upload len MinIO
    v
s3://landing/weather/*.csv          <-- Landing Zone (CSV goc)
    |
    | batch_bronze_weather.py
    | (unpivot, join metadata, Delta write)
    v
s3://bronze/weather_batch/          <-- Bronze (1,629,108 records, 36 cities)
    |
    | weather_etl.py
    | (filter New York, Kelvin->Celsius, dedup, MERGE)
    v
s3://silver/weather_clean/          <-- Silver (~43,000 records, New York only)
    |
    | weather_etl.py (tiep tuc)
    | (aggregate, join dim, partition, optimize)
    v
s3://gold/dim_city/                 <-- Dim City (1 record)
s3://gold/dim_date/                 <-- Dim Date (~1,854 records)
s3://gold/fact_weather_daily_stats/ <-- Fact Daily (~1,854 records, partition year/month)
s3://gold/fact_weather_monthly_stats/ <-- Fact Monthly (~61 records)
s3://gold/fact_weather_city_summary/  <-- Fact Summary (1 record)
    |
    | register_trino_tables.py
    v
Trino (SQL query engine) -> Superset (Dashboard)
```

## Cau hinh Airflow

| Tham so | Gia tri | Y nghia |
|---------|---------|---------|
| `schedule_interval` | `@hourly` | Chay tu dong moi 1 gio |
| `catchup` | `False` | Khong chay bu cac lan da miss |
| `retries` | `1` | Thu lai 1 lan neu task fail |
| `retry_delay` | `5 phut` | Doi 5 phut truoc khi thu lai |
| `execution_timeout` | `2 gio` (ingest, ETL) | Gioi han thoi gian chay toi da |
| `trigger_rule` | `none_failed_min_one_success` | Chay neu khong co task nao fail va it nhat 1 task thanh cong |
| `max_active_runs` | mac dinh (1) | Chi 1 DAG run tai mot thoi diem |

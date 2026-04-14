# Quy trinh xu ly du lieu Streaming voi Airflow

## Tong quan

He thong su dung **Apache Airflow** de giam sat va duy tri quy trinh nap du lieu thoi gian thuc (real-time) tu OpenWeather API vao tang Bronze cua Lakehouse. Khac voi batch pipeline xu ly du lieu lich su theo dot, streaming pipeline chay **lien tuc** de thu thap du lieu thoi tiet moi nhat cua New York moi 5 phut.

Luong du lieu streaming di qua 4 thanh phan:

```text
  OpenWeather API           NiFi              Kafka              Spark Streaming
+----------------+    +------------+    +---------------+    +------------------+
| GET /data/2.5/ | -> | Fetch +    | -> | Topic:        | -> | Parse JSON +     |
| weather?q=     |    | Publish    |    | weather-raw   |    | Write Delta      |
| New York       |    | to Kafka   |    | 3 partitions  |    | to Bronze        |
| moi 5 phut     |    |            |    | 7-day retain  |    |                  |
+----------------+    +------------+    +---------------+    +------------------+
                                                                      |
                                                                      v
                                                             s3://bronze/
                                                             weather_streaming/
```

Airflow khong truc tiep xu ly du lieu streaming, ma dong vai tro **Supervisor** — dam bao Spark Streaming job luon chay, tu dong khoi dong lai khi bi crash hoac timeout.

---

## Thanh phan 1: Nguon du lieu — OpenWeather API 2.5

**API**: Current Weather Data 2.5 (free tier)
**Endpoint**: `https://api.openweathermap.org/data/2.5/weather?q=New+York,US&units=metric&appid={key}`
**Tan suat**: Moi 5 phut (288 calls/ngay, nam trong gioi han free tier 1,000 calls/ngay)

**Du lieu tra ve** (JSON):

```json
{
  "coord": {"lon": -74.006, "lat": 40.7143},
  "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
  "main": {
    "temp": 22.5,        // Nhiet do (°C, da chuyen vi units=metric)
    "feels_like": 21.8,
    "temp_min": 20.1,
    "temp_max": 24.3,
    "humidity": 65,       // Do am (%)
    "pressure": 1015      // Ap suat (hPa)
  },
  "wind": {"speed": 3.6, "deg": 220, "gust": 5.1},
  "clouds": {"all": 0},
  "visibility": 10000,
  "dt": 1713100800,       // Unix timestamp cua lan do
  "sys": {"country": "US", "sunrise": 1713087234, "sunset": 1713134567},
  "name": "New York",
  "cod": 200
}
```

Du lieu co cau truc **nested JSON** voi nhieu cap (main.temp, wind.speed, weather[0].main). Viec flatten se duoc thuc hien o buoc Spark Streaming.

---

## Thanh phan 2: Thu thap du lieu — Apache NiFi

**Vai tro**: Goi API dinh ky va day du lieu vao Kafka. NiFi dong vai tro nhu mot **data collector** hoat dong doc lap voi Spark.

**Ly do dung NiFi thay vi goi API truc tiep tu Spark**:
- NiFi co giao dien truc quan de giam sat luong du lieu (so message, loi, throughput)
- Tach biet viec thu thap (NiFi) va xu ly (Spark) — moi thanh phan lam 1 viec
- NiFi co san cac processor xu ly HTTP, Kafka, retry, error routing ma khong can viet code
- Neu API thay doi hoac can them thanh pho, chi can chinh NiFi flow ma khong sua Spark code

### NiFi Flow Architecture

Flow duoc tao tu dong boi script `nifi/scripts/create-weather-flow.py`, gom 4 processors trong process group **"OpenWeather One Call to Kafka"**:

```text
+------------------------+       +---------------------+       +-------------------+
| GenerateFlowFile       |       | InvokeHTTP          |       | PublishKafka_2_6   |
| "Trigger New York"     | ----> | "Fetch New York"    | ----> | "Publish to Kafka" |
|                        |       |                     |       |                    |
| Tao FlowFile rong      |       | GET OpenWeather API |       | Topic: weather-raw |
| moi 5 phut de trigger  |       | Parse JSON response |       | acks: 1            |
| InvokeHTTP             |       |                     |       | use-transactions:  |
|                        |       | Auto-terminate:     |       | false              |
| schedulingPeriod:      |       | "Original"          |       |                    |
| 5 min                  |       |                     |       | Auto-terminate:    |
+------------------------+       +-----+-------+-------+       | "success"          |
                                       |       |               +--------+-----------+
                                       |       |                        |
                              Response |       | Failure/               | failure
                                       |       | No Retry/              |
                                       v       | Retry                  v
                                   (to Kafka)  |            +-------------------+
                                               +----------> | LogAttribute      |
                                                            | "Log Failures"    |
                                                            |                   |
                                                            | Ghi log loi       |
                                                            | Auto-terminate:   |
                                                            | "success"         |
                                                            +-------------------+
```

### Chi tiet tung Processor

**1. GenerateFlowFile — "Trigger New York"**

```
Type: org.apache.nifi.processors.standard.GenerateFlowFile
schedulingPeriod: 5 min
File Size: 0B
```

Tao mot FlowFile rong (0 byte) moi 5 phut. FlowFile nay khong chua du lieu — no chi dong vai tro **trigger** de kich hoat InvokeHTTP. Day la dac diem cua NiFi 1.x: InvokeHTTP khong tu goi API, no can FlowFile dau vao de bat dau xu ly.

**2. InvokeHTTP — "Fetch New York"**

```
Type: org.apache.nifi.processors.standard.InvokeHTTP
HTTP Method: GET
Remote URL: https://api.openweathermap.org/data/2.5/weather?q=New+York%2CUS&units=metric&appid={key}
Auto-terminate: Original
```

Nhan FlowFile tu GenerateFlowFile, goi GET request den OpenWeather API. Ket qua:
- **Response** (thanh cong): FlowFile moi chua JSON response -> chuyen sang Kafka
- **Original**: FlowFile goc (rong) -> auto-terminate (xoa bo, khong can nua)
- **Failure / No Retry / Retry**: Loi -> chuyen sang LogAttribute

URL duoc encode bang `urllib.parse.quote()` de xu ly ky tu dac biet (khoang trang trong "New York" -> "New+York%2CUS").

**3. PublishKafka_2_6 — "Publish to Kafka"**

```
Type: org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6
bootstrap.servers: kafka:9092
topic: weather-raw
acks: 1
use-transactions: false
Auto-terminate: success
```

Nhan JSON response tu InvokeHTTP va gui vao Kafka topic `weather-raw`. Cau hinh quan trong:
- `acks: 1` — Kafka broker xac nhan nhan message (can bang giua do tin cay va hieu suat)
- `use-transactions: false` — Tat Kafka transactions vi single-node Kafka khong ho tro transactions (se gap loi `TimeoutException awaiting InitProducerId` neu bat)

**4. LogAttribute — "Log Failures"**

```
Type: org.apache.nifi.processors.standard.LogAttribute
Auto-terminate: success
```

Nhan cac FlowFile loi tu InvokeHTTP va PublishKafka, ghi log de debug. La diem cuoi (sink) cua flow — khong chuyen tiep di dau.

---

## Thanh phan 3: Message Broker — Apache Kafka

**Vai tro**: Lam bo dem (buffer) giua NiFi (producer) va Spark Streaming (consumer), dam bao du lieu khong bi mat neu mot trong hai ben tam ngung.

### Topic: `weather-raw`

| Tham so | Gia tri | Y nghia |
|---------|---------|---------|
| `partitions` | 3 | Cho phep 3 consumer doc song song (du cho scale) |
| `replication-factor` | 1 | Single-node, khong replicate (do an, khong can HA) |
| `retention.ms` | 604800000 (7 ngay) | Giu message 7 ngay — neu Spark down, khong mat du lieu |

Duoc tao tu dong boi `kafka-init` container khi he thong khoi dong:

```bash
kafka-topics --create --if-not-exists \
  --topic weather-raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000
```

### Tai sao dung Kafka thay vi ghi thang vao MinIO?

- **Decoupling**: NiFi va Spark hoat dong doc lap. NiFi ghi vao Kafka bat ke Spark co dang chay hay khong
- **Buffering**: Neu Spark Streaming crash, du lieu van nam trong Kafka (giu 7 ngay). Khi Spark restart, no doc lai tu checkpoint — khong mat message nao
- **Replay**: Co the doc lai du lieu tu dau (from-beginning) de xu ly lai
- **Backpressure**: Kafka tu dong xu ly khi producer nhanh hon consumer

---

## Thanh phan 4: Xu ly Streaming — Spark Structured Streaming

**Spark Job**: `streaming_bronze_weather.py`

### Buoc 4.1: Doc tu Kafka

```python
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather-raw")
    .option("startingOffsets", "earliest")     # Doc tu dau neu chua co checkpoint
    .option("failOnDataLoss", "false")         # Khong fail neu partition bi xoa
    .load())
```

Spark Structured Streaming doc **lien tuc** tu Kafka topic `weather-raw`. Moi message Kafka la 1 cap (key, value, timestamp) — du lieu JSON nam trong truong `value`.

- `startingOffsets: earliest` — Lan chay dau tien, doc tat ca message tu dau topic (khong bo sot)
- `failOnDataLoss: false` — Neu Kafka xoa message cu (qua retention 7 ngay), khong lam Spark crash

### Buoc 4.2: Parse JSON va Flatten

```python
parsed_df = (kafka_df
    .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
    .withColumn("data", from_json(col("json_value"), weather_api25_schema))
    .select(col("data.*"), col("kafka_timestamp"))
    .withColumn("ingested_at", current_timestamp()))
```

Quy trinh:
1. Cast `value` (binary) sang string
2. Parse JSON string thanh struct bang schema da dinh nghia truoc
3. Flatten: `select("data.*")` bien struct thanh cac cot rieng biet
4. Giu `kafka_timestamp` (thoi diem Kafka nhan message) va them `ingested_at` (thoi diem Spark xu ly)

**Schema duoc dinh nghia tuong minh** (khong dung inferSchema) vi Structured Streaming yeu cau schema co dinh. Schema map chinh xac cau truc JSON cua OpenWeather API 2.5:

```python
weather_api25_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType()),
    ])),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
        ...
    ])),
    StructField("wind", StructType([...])),
    StructField("weather", ArrayType(StructType([...]))),
    StructField("dt", LongType()),
    StructField("name", StringType()),
    ...
])
```

Du lieu SAU khi parse van giu nguyen cau truc nested (main.temp, wind.speed, ...). Viec flatten thanh cac cot don (temperature, humidity, ...) se do `weather_etl.py` thuc hien o tang Silver — dung nguyen tac Bronze giu nguyen du lieu tho.

### Buoc 4.3: Ghi vao Bronze Delta Table

```python
query = (parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://bronze/_checkpoints/weather")
    .option("mergeSchema", "true")
    .start("s3a://bronze/weather_streaming/"))
```

| Tham so | Gia tri | Y nghia |
|---------|---------|---------|
| `format` | `delta` | Ghi duoi dang Delta Lake (ACID, schema enforcement) |
| `outputMode` | `append` | Chi them moi, khong cap nhat hoac xoa du lieu cu |
| `checkpointLocation` | `s3a://bronze/_checkpoints/weather` | Luu vi tri doc cuoi cung trong Kafka (offset) |
| `mergeSchema` | `true` | Tu dong them cot moi neu API thay doi response schema |

**Checkpoint** la co che quan trong nhat cua Spark Structured Streaming:
- Luu Kafka offset da doc (vi du: partition 0 offset 42, partition 1 offset 38, ...)
- Khi Spark restart, doc lai tu checkpoint thay vi tu dau -> **exactly-once processing**
- Checkpoint luu tren MinIO (S3), khong mat khi container restart

### Buoc 4.4: Chay lien tuc

```python
query.awaitTermination()
```

Dong nay block vinh vien — Spark Streaming chay khong ngung, lien tuc doc message moi tu Kafka va ghi vao Delta. Job chi dung khi:
- Bi kill boi Airflow (execution_timeout: 12 gio)
- Container Spark crash
- Loi khong phuc hoi duoc (vi du: MinIO khong truy cap duoc)

---

## Airflow Supervisor — Dam bao Streaming chay lien tuc

### Van de: Airflow khong phu hop cho long-running jobs

Airflow duoc thiet ke cho **batch jobs** — cac task co diem bat dau va ket thuc ro rang. Spark Streaming la **long-running job** chay vinh vien, tao ra xung dot:

| Van de | Hau qua |
|--------|---------|
| `execution_timeout` | Sau 12 gio, Airflow kill task |
| Task crash | Khong ai biet, khong tu restart |
| Nhieu DAG run chong nhau | Co the start 2 streaming job cung luc |

### Giai phap: Supervisor Pattern

DAG `weather_streaming` khong chi don gian "start streaming". No hoat dong nhu mot **supervisor** — kiem tra dinh ky va dam bao streaming luon chay:

```text
Moi 30 phut (*/30 * * * *):

  check_streaming_active
  (BranchPythonOperator)
        |
        |--- Query Spark REST API: http://spark-master:8080/json/
        |    Tim app co ten chua "streaming" trong activeapps
        |
        +---> streaming_already_running     (neu TIM THAY -> skip, khong lam gi)
        |     (EmptyOperator)
        |
        +---> start_weather_streaming       (neu KHONG TIM THAY -> khoi dong moi)
              (BashOperator)
              spark-submit streaming_bronze_weather.py
              execution_timeout: 12 gio
```

### Chi tiet ham kiem tra

```python
def check_streaming_active(**kwargs):
    req = urllib.request.urlopen("http://spark-master:8080/json/", timeout=5)
    data = json.loads(req.read().decode())
    active_apps = [
        app for app in data.get("activeapps", [])
        if "streaming" in app.get("name", "").lower()
    ]
    if active_apps:
        return 'streaming_already_running'
    return 'start_weather_streaming'
```

Ham goi **Spark Master REST API** tai `http://spark-master:8080/json/` — endpoint nay tra ve JSON chua danh sach tat ca ung dung dang chay tren cluster. Ham loc nhung app co ten chua "streaming" (vi Spark job ten la "Streaming Bronze Weather - API 2.5").

### Vong doi cua Streaming job

```text
Thoi gian    Su kien                          Airflow phan ung
─────────────────────────────────────────────────────────────────────
00:00       DAG run #1: khong co streaming    -> start_weather_streaming
00:00       Spark Streaming bat dau chay      -> job dang chay...
00:30       DAG run #2: tim thay streaming    -> streaming_already_running (skip)
01:00       DAG run #3: tim thay streaming    -> streaming_already_running (skip)
...
11:30       DAG run #24: tim thay streaming   -> streaming_already_running (skip)
12:00       execution_timeout 12h -> Airflow kill task
12:00       DAG run #25: khong tim thay       -> start_weather_streaming (restart!)
12:00       Spark Streaming bat dau chay lai  -> job dang chay...
...
(lap lai vinh vien)
```

### Cau hinh bao ve

| Tham so | Gia tri | Ly do |
|---------|---------|-------|
| `schedule_interval` | `*/30 * * * *` | Kiem tra moi 30 phut — can bang giua phat hien nhanh va khong tao qua nhieu DAG run |
| `max_active_runs` | `1` | Chi 1 DAG run tai mot thoi diem — tranh 2 supervisor chay chong nhau |
| `catchup` | `False` | Khong chay bu cac lan da miss khi Airflow vua khoi dong lai |
| `retries` | `2` | Neu start_weather_streaming fail (vi du Spark Master chua san sang), thu lai 2 lan |
| `retry_delay` | `2 phut` | Doi 2 phut giua cac lan thu — du thoi gian cho Spark Master phuc hoi |
| `execution_timeout` | `12 gio` | Streaming chay toi da 12 gio roi bi kill — DAG run tiep theo se restart |

---

## Tong ket luong du lieu Streaming end-to-end

```text
OpenWeather API 2.5
    |
    | GET moi 5 phut (NiFi GenerateFlowFile -> InvokeHTTP)
    v
NiFi "Fetch New York"
    |
    | PublishKafka_2_6 (acks=1, no transactions)
    v
Kafka topic: weather-raw
    | 3 partitions, 7-day retention
    | Message = JSON response nguyen ban tu API
    v
Spark Structured Streaming
    | readStream (format=kafka, startingOffsets=earliest)
    | Parse JSON voi schema co dinh
    | Them ingested_at timestamp
    v
s3://bronze/weather_streaming/      <-- Bronze Delta Table
    | Checkpoint: s3://bronze/_checkpoints/weather
    | outputMode: append, mergeSchema: true
    v
(Doc boi weather_etl.py trong batch pipeline)
    | Flatten nested JSON -> cac cot don
    | UNION voi batch Bronze
    | Filter, dedup, MERGE vao Silver
    v
s3://silver/weather_clean/          <-- Silver (ket hop batch + streaming)
```

### Truy van kiem tra du lieu streaming

Sau khi streaming chay duoc 5-10 phut, dung cac lenh sau de xac nhan du lieu da do vao Bronze va Silver:

**Kiem tra Bronze Streaming** (du lieu tho tu API):

> **Luu y**: Cac truy van ben duoi chi hoat dong SAU KHI:
> 1. NiFi da goi API va gui data vao Kafka
> 2. Spark Streaming da chay va ghi it nhat 1 batch vao `s3://bronze/weather_streaming/`
> 3. `register_trino_tables.py` da chay lai de dang ky bang moi
>
> Neu gap loi `Table does not exist` -> streaming chua co du lieu, chua can truy van.

```bash
# Xem 10 records moi nhat do vao Bronze
docker exec trino trino --execute \
  "SELECT name as city,
          main.temp as temperature,
          main.humidity as humidity,
          wind.speed as wind_speed,
          from_unixtime(dt) as recorded_at,
          ingested_at
   FROM delta.default.bronze_weather_streaming
   ORDER BY ingested_at DESC LIMIT 10"

# Dem tong records va kiem tra freshness
docker exec trino trino --execute \
  "SELECT COUNT(*) as total_records,
          MIN(from_unixtime(dt)) as earliest_record,
          MAX(from_unixtime(dt)) as latest_record,
          MAX(ingested_at) as last_ingested
   FROM delta.default.bronze_weather_streaming"

# Dem so records theo ngay (xac nhan du lieu do lien tuc)
docker exec trino trino --execute \
  "SELECT CAST(ingested_at AS DATE) as ngay,
          COUNT(*) as so_records
   FROM delta.default.bronze_weather_streaming
   GROUP BY CAST(ingested_at AS DATE)
   ORDER BY ngay DESC LIMIT 7"
```

**Kiem tra Silver** (du lieu streaming da merge voi batch):

```bash
# Xem 10 records moi nhat trong Silver (ca batch + streaming)
docker exec trino trino --execute \
  "SELECT city, temperature, humidity, wind_speed,
          weather_condition, recorded_at, source
   FROM delta.default.silver_weather_clean
   ORDER BY recorded_at DESC LIMIT 10"

# Kiem tra du lieu tu ca 2 nguon da merge vao Silver
docker exec trino trino --execute \
  "SELECT source,
          COUNT(*) as records,
          MIN(recorded_at) as earliest,
          MAX(recorded_at) as latest
   FROM delta.default.silver_weather_clean
   GROUP BY source"
```

**Ket qua mong doi**:
- Bronze Streaming: Co records voi `ingested_at` gan thoi diem hien tai (chenh lech < 30 phut)
- Silver: Co 2 dong trong ket qua `GROUP BY source`: `kaggle_csv` (du lieu lich su) va `openweather_api` (du lieu streaming)

### Diem quan trong

- **NiFi** chi lam 1 viec: goi API va day vao Kafka. Khong xu ly, khong transform
- **Kafka** la bo dem giua NiFi va Spark. Du lieu an toan 7 ngay du Spark co crash
- **Spark Streaming** chi lam 1 viec: parse JSON va ghi vao Bronze. Khong loc, khong doi don vi
- **weather_etl.py** (batch pipeline) la noi du lieu streaming duoc ket hop voi batch va xu ly thanh Silver/Gold
- **Airflow** khong tham gia vao luong du lieu — chi giam sat va dam bao Spark Streaming luon song

### So sanh voi Batch Pipeline

| Khia canh | Batch | Streaming |
|-----------|-------|-----------|
| Nguon du lieu | Kaggle CSV (lich su 2012-2017) | OpenWeather API (thoi gian thuc) |
| Tan suat | 1 lan upload, xu ly moi gio | Lien tuc moi 5 phut |
| Khoi luong | ~1.6 trieu records | ~288 records/ngay |
| Thu thap | Upload thu cong len MinIO | NiFi tu dong goi API |
| Van chuyen | Truc tiep tu MinIO | Kafka lam trung gian |
| Xu ly | Spark batch (doc -> transform -> ghi) | Spark Structured Streaming (doc lien tuc -> ghi) |
| Bronze format | Delta, append mode | Delta, append mode + checkpoint |
| Airflow vai tro | Dieu phoi toan bo pipeline | Supervisor (giam sat + restart) |

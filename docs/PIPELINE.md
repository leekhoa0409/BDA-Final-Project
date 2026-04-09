  Kiến trúc tổng thể                                                                        
   
  ┌─────────────────────────────────────────────────────────────────────────┐               
  │                           DATA SOURCES                                  │
  │                                                                         │               
  │  📊 Kaggle Historical (2012-2017)           🌐 OpenWeather API 2.5      │
  │  6 CSV files × 36 cities                    New York, mỗi 5 phút       │                
  │  Wide format: rows=hours, cols=cities       GET /data/2.5/weather       │               
  │  temperature, humidity, pressure,           ?q=New York,US              │               
  │  wind_speed, wind_direction,                &units=metric               │               
  │  weather_description                        &appid={key}               │                
  │  + city_attributes.csv (metadata)           Free: 288 calls/ngày       │                
  └─────────┬───────────────────────────────────────────┬───────────────────┘               
            │                                           │                                   
            ▼                                           ▼                                   
  ┌──────────────────────┐                 ┌─────────────────────────┐
  │  MinIO Landing Zone  │                 │  Apache NiFi (:8443)    │                      
  │  s3://landing/       │                 │                         │                      
  │  weather/*.csv       │                 │  [Fetch New York]       │                      
  │                      │                 │   InvokeHTTP (5 min)    │                      
  │  Upload qua:         │                 │         │               │                      
  │  • MinIO Console     │                 │         ▼               │                      
  │    (:9001)           │                 │  [Publish to Kafka]     │                      
  │  • mc CLI            │                 │   topic: weather-raw    │                      
  │  • boto3             │                 │         │               │
  └──────────┬───────────┘                 │  [Log Failures]        │                       
             │                             └─────────┬───────────────┘                      
             │                                       │                                      
             ▼                                       ▼                                      
  ┌──────────────────────┐                 ┌─────────────────────────┐                      
  │  Spark Batch Job     │                 │  Apache Kafka (:9092)   │
  │                      │                 │                         │                      
  │  batch_bronze_       │                 │  Topic: weather-raw     │
  │  weather.py          │                 │  3 partitions           │                      
  │                      │                 │  Retention: 7 ngày      │
  │  1. Đọc 6 CSV files  │                 └─────────┬───────────────┘                      
  │  2. Unpivot wide→long│                           │                                      
  │  3. Join metadata    │                           ▼                                      
  │     (city_attributes)│                 ┌─────────────────────────┐                      
  │  4. Kelvin → Celsius │                 │  Spark Streaming        │                      
  │  5. Append Delta     │                 │                         │                      
  │                      │                 │  streaming_bronze_      │                      
  │  Output: 1,629,108   │                 │  weather.py             │                      
  │  records (36 cities) │                 │                         │                      
  └──────────┬───────────┘                 │  • Parse API 2.5 JSON   │                      
             │                             │  • Append Delta         │                      
             │                             │  • Checkpoint recovery  │                      
             │                             └─────────┬───────────────┘                      
             ▼                                       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐               
  │                        🥉 BRONZE LAYER (Raw Data)                       │               
  │                                                                         │
  │  s3://bronze/weather_batch/           s3://bronze/weather_streaming/    │               
  │  1,629,108 records                    Real-time New York data           │               
  │  36 cities, hourly 2012-2017          API 2.5 JSON (nested)             │               
  │  Delta format, append mode            Delta format, append mode         │               
  │                                                                         │               
  │  Nguyên tắc: Giữ nguyên raw data, không filter, không transform        │                
  └────────────────────────────┬────────────────────────────────────────────┘               
                               │                                                          
                               ▼                                                            
  ┌─────────────────────────────────────────────────────────────────────────┐               
  │                    weather_etl.py — SILVER TRANSFORMATION               │
  │                                                                         │               
  │  1. read_streaming_bronze()          2. read_batch_bronze()             │             
  │     Flatten nested JSON:                Đã flat từ CSV, chỉ rename:    │                
  │     name → city                         Kelvin → Celsius               │                
  │     main.temp → temperature             datetime → recorded_at         │                
  │     main.humidity → humidity            Thêm source="kaggle_csv"       │                
  │     wind.speed → wind_speed                                            │                
  │     weather[0].main → condition                                        │                
  │     Thêm source="openweather_api"                                      │                
  │                         │                          │                    │             
  │                         └──────────┬───────────────┘                    │               
  │                                    ▼                                    │               
  │                          UNION 2 DataFrames                             │               
  │                                    │                                    │               
  │                                    ▼                                    │             
  │                    Filter: city == "New York"                           │               
  │                    Filter: temperature ∈ [-80, 60]°C                   │              
  │                    Filter: humidity ∈ [0, 100]%                        │                
  │                    Filter: city IS NOT NULL                             │               
  │                                    │                                    │               
  │                                    ▼                                    │               
  │                    Deduplicate trên (city, recorded_dt)                 │             
  │                    row_number() → giữ bản mới nhất                     │                
  │                                    │                                    │               
  │                                    ▼                                    │               
  │                    MERGE vào Silver (Delta Lake upsert)                 │               
  │                    • Match: city + recorded_dt                          │               
  │                    • Matched → UPDATE                                   │               
  │                    • Not matched → INSERT                               │               
  │                    • Không mất dữ liệu lịch sử                         │              
  └────────────────────────────┬────────────────────────────────────────────┘               
                               │                                                            
                               ▼                                                            
  ┌─────────────────────────────────────────────────────────────────────────┐               
  │                     🥈 SILVER LAYER (Clean Data)                        │             
  │                                                                         │               
  │  s3://silver/weather_clean/                                             │
  │  130,887 records — New York only, deduplicated                         │                
  │  Delta format, MERGE mode                                               │               
  │                                                                         │               
  │  Schema: city, country, latitude, longitude, temperature, feels_like,  │                
  │  humidity, pressure, wind_speed, weather_condition,                     │               
  │  weather_description, recorded_at, recorded_dt, recorded_date,         │              
  │  source, transformed_at                                                 │               
  └────────────────────────────┬────────────────────────────────────────────┘               
                               │                                                            
                ┌──────────────┼──────────────┐                                             
                ▼              ▼              ▼                                             
  ┌──────────────────┐ ┌────────────────┐ ┌──────────────────────┐
  │  gold_daily_     │ │ gold_monthly_  │ │ gold_city_summary()  │                          
  │  stats()         │ │ stats()        │ │                      │                          
  │                  │ │                │ │ Grain: 1 row =       │                          
  │ Grain:           │ │ Grain:         │ │ New York tổng hợp    │                          
  │ city + date      │ │ city + yyyy-MM │ │                      │                          
  │                  │ │                │ │ • latest_temperature  │                         
  │ • avg_temperature│ │ • avg/min/max  │ │ • latest_humidity     │                         
  │ • min_temperature│ │   temperature  │ │ • latest_recorded_at  │                         
  │ • max_temperature│ │ • avg_humidity │ │ • avg_temp_overall    │                         
  │ • avg_humidity   │ │ • avg_wind     │ │ • total_days_tracked  │                         
  │ • avg_pressure   │ │ • rainy_day_   │ │ • dominant_weather    │                         
  │ • avg_wind_speed │ │   count        │ │ • latitude, longitude │                         
  │ • dominant_      │ │ • clear_day_   │ │                      │                          
  │   weather        │ │   count        │ │ 1 record             │                          
  │   (row_number)   │ │ • total_       │ │ Mode: overwrite       │                         
  │ • measurement_   │ │   measurements │ │                      │                          
  │   count          │ │                │ └──────────┬───────────┘                          
  │                  │ │ 61 records     │            │                                      
  │ 1,854 records    │ │ Mode: overwrite│            │                                      
  │ Mode: overwrite  │ │                │            │                                      
  └────────┬─────────┘ └───────┬────────┘            │                                      
           └───────────────────┼─────────────────────┘                                      
                               │                                                            
                               ▼                                                            
  ┌─────────────────────────────────────────────────────────────────────────┐               
  │                      🥇 GOLD LAYER (Analytics)                          │             
  │                                                                         │               
  │  s3://gold/weather_daily_stats/      1,854 records (5 năm daily)       │
  │  s3://gold/weather_monthly_stats/    61 records (5 năm monthly)        │                
  │  s3://gold/weather_city_summary/     1 record (New York overview)      │                
  │                                                                         │               
  │  Delta format, overwrite mode (full refresh mỗi lần chạy ETL)         │                 
  └────────────────────────────┬────────────────────────────────────────────┘               
                               │
                               ▼                                                            
  ┌─────────────────────────────────────────────────────────────────────────┐             
  │                   register_trino_tables.py                              │               
  │                                                                         │
  │  Gọi Trino REST API: CALL delta.system.register_table(...)             │                
  │  Register 6 tables → delta.default.*                                    │               
  │  Skip nếu đã register hoặc path chưa tồn tại                          │                 
  └────────────────────────────┬────────────────────────────────────────────┘               
                               │                                                            
                               ▼                                                            
  ┌─────────────────────────────────────────────────────────────────────────┐             
  │                      Trino (:8085)                                      │               
  │                      SQL Query Engine                                   │
  │                                                                         │               
  │  Catalog: delta    Schema: default    Metastore: s3://warehouse/       │              
  │                                                                         │               
  │  Tables:                                                                │             
  │  • delta.default.gold_weather_daily_stats                              │                
  │  • delta.default.gold_weather_monthly_stats                            │              
  │  • delta.default.gold_weather_city_summary                             │                
  │  • delta.default.silver_weather_clean                                  │              
  │  • delta.default.bronze_weather_batch                                  │                
  └────────────────────────────┬────────────────────────────────────────────┘               
                               │
                               ▼                                                            
  ┌─────────────────────────────────────────────────────────────────────────┐             
  │                    Apache Superset (:8088)                               │              
  │                                                                         │
  │  Database: "Trino Lakehouse"                                            │               
  │  Connection: trino://trino@trino:8080/delta/default                    │                
  │                                                                         │
  │  SQL Lab → Viết query trực tiếp                                        │                
  │  Charts → Tạo biểu đồ từ Gold tables                                  │                 
  │  Dashboards → Tổ hợp charts thành dashboard                           │                 
  │                                                                         │               
  │  Ví dụ phân tích:                                                      │                
  │  • Biến động nhiệt độ New York theo ngày/tháng                        │               
  │  • So sánh nhiệt độ các mùa (monthly_stats)                           │                 
  │  • Tỷ lệ ngày mưa vs nắng theo tháng                                  │                 
  │  • Xu hướng độ ẩm, áp suất theo thời gian                             │                 
  └─────────────────────────────────────────────────────────────────────────┘               
                                                                                            
  Airflow Orchestration                                                                     
                                                                                          
  ┌─────────────────────────────────────────────────────────────────────┐                   
  │  weather_pipeline (tự động @hourly)                                 │                 
  │                                                                     │                   
  │  [check_csv_in_landing]                                             │
  │         │                                                           │                   
  │   ┌─────┴─────┐                                                    │                    
  │   │           │                                                     │                   
  │  [ingest]  [skip]     ← Có CSV mới trong landing? Nếu có → ingest │                     
  │   │           │                                                     │                   
  │   └─────┬─────┘                                                    │
  │         │                                                           │                   
  │  [run_weather_etl]    ← Silver (MERGE) + Gold (3 tables)           │                  
  │         │                                                           │                   
  │   ┌─────┴──────────────┐                                           │
  │   │                    │                                            │                   
  │  [validate_bronze]  [register_trino]  ← Song song                  │                  
  │   │                                                                 │                   
  │  ┌┴──┐                                                              │                   
  │  │   │                                                              │                   
  │ [validate_silver] [validate_gold]     ← Song song                  │                    
  └─────────────────────────────────────────────────────────────────────┘                 
                                                                                            
  ┌─────────────────────────────────────────────────────────────────────┐                 
  │  weather_streaming (trigger thủ công 1 lần, chạy liên tục 24h)     │                    
  │                                                                     │                   
  │  [start_weather_streaming]                                          │
  │   Spark Structured Streaming: Kafka → Bronze                       │                    
  │   Chạy liên tục, tự đọc message mới từ Kafka                      │                     
  └─────────────────────────────────────────────────────────────────────┘                   
                                                                                            
  Validation (Kiểm tra chất lượng)                                                          
                                                                                            
  ┌────────┬────────────────────────────┬────────────────────────────────────────────────┐  
  │ Layer  │            Job             │                    Kiểm tra                    │
  ├────────┼────────────────────────────┼────────────────────────────────────────────────┤  
  │ Bronze │ validate_weather_bronze.py │ Row count > 0, NULL check (dt, name, city,     │
  │        │                            │ datetime), data freshness                      │
  ├────────┼────────────────────────────┼────────────────────────────────────────────────┤  
  │ Silver │ validate_weather_silver.py │ Required columns, temp ∈ [-80,60], humidity ∈  │
  │        │                            │ [0,100], NULL city                             │  
  ├────────┼────────────────────────────┼────────────────────────────────────────────────┤
  │ Gold   │ validate_weather_gold.py   │ Cross-validate: sum(measurement_count) ==      │  
  │        │                            │ Silver count, min ≤ avg ≤ max temp             │  
  └────────┴────────────────────────────┴────────────────────────────────────────────────┘
                                                                                            
  Services (15 containers)                                                                

  ┌──────────────┬────────────┬──────────────────────┬─────────────────────────────────┐    
  │   Service    │    Port    │        Login         │             Vai trò             │ 
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ MinIO        │ 9001       │ admin / admin123456  │ Storage UI — upload CSV, xem    │  
  │ Console      │            │                      │ buckets                         │ 
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ MinIO API    │ 9000       │ —                    │ S3-compatible API cho           │    
  │              │            │                      │ Spark/Trino                     │    
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ NiFi         │ 8443       │ admin /              │ Fetch OpenWeather API → Kafka   │  
  │              │ (HTTPS)    │ admin123456789       │                                 │    
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤  
  │ Kafka        │ 9092 /     │ —                    │ Message broker (weather-raw     │ 
  │              │ 29092      │                      │ topic)                          │    
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤ 
  │ Zookeeper    │ 2181       │ —                    │ Kafka coordination              │    
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ Spark Master │ 8080       │ —                    │ ETL processing cluster          │ 
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ Spark Worker │ 8081       │ —                    │ Worker node (2 cores, 2GB)      │  
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ Airflow      │ 8082       │ airflow / airflow    │ Pipeline orchestration          │ 
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ Trino        │ 8085       │ —                    │ SQL query engine                │  
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ Superset     │ 8088       │ admin / admin        │ BI dashboards                   │  
  ├──────────────┼────────────┼──────────────────────┼─────────────────────────────────┤    
  │ PostgreSQL   │ 5432, 5433 │ —                    │ Metadata (Airflow + Superset)   │
  └──────────────┴────────────┴──────────────────────┴─────────────────────────────────┘    
                                                                                          
  MinIO Buckets                                                                             
                                                                                          
  ┌───────────┬──────────────────────────────────────────────────────────────────────────┐  
  │  Bucket   │                                 Mục đích                                 │
  ├───────────┼──────────────────────────────────────────────────────────────────────────┤  
  │ landing   │ CSV upload staging — Kaggle files                                        │
  ├───────────┼──────────────────────────────────────────────────────────────────────────┤
  │ bronze    │ Raw: weather_batch/ + weather_streaming/                                 │  
  ├───────────┼──────────────────────────────────────────────────────────────────────────┤  
  │ silver    │ Clean: weather_clean/ (New York only)                                    │  
  ├───────────┼──────────────────────────────────────────────────────────────────────────┤  
  │ gold      │ Analytics: weather_daily_stats/, weather_monthly_stats/,                 │
  │           │ weather_city_summary/                                                    │  
  ├───────────┼──────────────────────────────────────────────────────────────────────────┤
  │ warehouse │ Trino file metastore                                                     │  
  └───────────┴──────────────────────────────────────────────────────────────────────────┘  
  
  Spark Jobs                                                                                
                                                                                          
  ┌─────────────────────────────┬───────────────┬───────────────────────────────────────┐
  │             Job             │     Loại      │            Input → Output             │
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤
  │ config.py                   │ Config        │ Tất cả S3 paths, Kafka, Trino config  │
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤
  │ batch_bronze_weather.py     │ Batch Ingest  │ landing/*.csv → bronze/weather_batch  │   
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤   
  │ streaming_bronze_weather.py │ Streaming     │ Kafka weather-raw →                   │   
  │                             │ Ingest        │ bronze/weather_streaming              │   
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤ 
  │ weather_etl.py              │ Transform     │ Bronze (cả 2) → Silver → Gold (3      │
  │                             │               │ tables)                               │   
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤
  │ validate_weather_bronze.py  │ Validation    │ Check Bronze quality                  │   
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤   
  │ validate_weather_silver.py  │ Validation    │ Check Silver quality                  │
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤   
  │ validate_weather_gold.py    │ Validation    │ Check Gold quality + cross-validate   │ 
  ├─────────────────────────────┼───────────────┼───────────────────────────────────────┤
  │ register_trino_tables.py    │ Metadata      │ Register Delta tables trong Trino     │
  └─────────────────────────────┴───────────────┴───────────────────────────────────────┘ 
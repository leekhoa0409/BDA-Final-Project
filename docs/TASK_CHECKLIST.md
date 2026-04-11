# Danh sách công việc chi tiết (Detailed Task List)

Dưới đây là bảng phân công chi tiết công việc cho 4 thành viên. Mỗi nhiệm vụ đi kèm với kết quả mong đợi (Output).

---

## Member 1: Infrastructure & Data Ingestion (DE)
*Bảo trì nền tảng và luồng dữ liệu thô từ Nguồn -> Bronze.*

- [ ] **1. Quản trị Infrastructure**
    - [ ] Tối ưu hóa file `docker-compose.yml`: Gom nhóm các service, cấu hình healthcheck.
    - [ ] Quản lý cấu hình mạng (network) và lưu trữ (MinIO buckets).
- [ ] **2. Ingestion Pipeline (NiFi + Kafka)**
    - [ ] Cấu hình NiFi Fetch OpenWeather API theo chu kỳ (5-10p).
    - [ ] Đảm bảo dữ liệu đẩy vào Kafka topic `weather-raw` đúng định dạng JSON.
    - [ ] Xử lý backpressure và lỗi kết nối NiFi.
- [ ] **3. Orchestration (Airflow)**
    - [ ] Hoàn thiện các DAGs: Thêm cơ chế retry, alert khi job fail.
    - [ ] Tách biệt các biến môi trường (Environment Variables) vào Airflow Variables/Connections.
- [ ] **4. CI/CD & Security**
    - [ ] Viết GitHub Actions cơ bản để tự động chạy Unit Test khi push code.
    - [ ] Phân quyền truy cập MinIO (Access key/Secret key) cho các ứng dụng.

---

## Member 2: Lakehouse & Data Quality (DE)
*Xây dựng các tầng dữ liệu Medallion và kiểm soát chất lượng.*

- [ ] **1. Spark Transformation (Silver Layer)**
    - [ ] Viết job Spark xử lý dữ liệu từ 36 thành phố (không chỉ NY).
    - [ ] Thực hiện Deduplication và chuẩn hóa kiểu dữ liệu.
    - [ ] Sử dụng Delta Lake Merge để Upsert dữ liệu vào Silver.
- [ ] **2. Data Modeling (Gold Layer)**
    - [ ] Thiết kế mô hình Star Schema: Bảng Fact (Weather stats) và các bảng Dim (City, Date).
    - [ ] Tính toán các chỉ số nâng cao: Rolling average, Moving standard deviation.
- [ ] **3. Data Quality (Great Expectations)**
    - [ ] Cài đặt GE và tạo bộ Expectation Suite (Null check, Range check cho temp/humidity).
    - [ ] Tích hợp GE vào Airflow pipeline để chặn dữ liệu "bẩn" lên Gold layer.
- [ ] **4. SQL Query Optimization**
    - [ ] Cấu hình Trino để truy vấn Delta tables nhanh nhất (partitioning, Z-order).

---

## Member 3: Machine Learning & MLOps (MLE)
*Dự báo thời tiết và triển khai Model.*

- [ ] **1. Feature Engineering**
    - [ ] Trích xuất các đặc trưng thời gian (hour, day of week, season).
    - [ ] Tạo các lagged features (nhiệt độ 1h trước, 3h trước).
- [ ] **2. Model Development**
    - [ ] Thử nghiệm các mô hình: Linear Regression, XGBoost, Random Forest.
    - [ ] Đánh giá model bằng MAE, RMSE.
- [ ] **3. MLflow Tracking & Registry**
    - [ ] Cấu hình MLflow Tracking server để lưu lại các lần training.
    - [ ] Đăng ký model tốt nhất vào Model Registry.
- [ ] **4. Model Serving**
    - [ ] Xây dựng FastAPI service để load model từ MLflow và trả về kết quả dự báo.
    - [ ] Viết client dự báo real-time lấy dữ liệu trực tiếp từ Kafka.

---

## Member 4: Monitoring, BI & Documentation (Analyst)
*Giám sát, phân tích insight và viết báo cáo.*

- [ ] **1. Monitoring System (Prometheus & Grafana)**
    - [ ] Triển khai Prometheus để thu thập metrics từ các container.
    - [ ] Thiết kế dashboard Grafana: Theo dõi CPU/RAM, tình trạng sống sót của các service.
    - [ ] Theo dõi metrics của Spark jobs và Airflow DAGs.
- [ ] **2. BI Dashboards (Superset)**
    - [ ] Xây dựng bộ Dashboard trực quan về thời tiết: Bản đồ nhiệt độ, biểu đồ xu hướng 5 năm.
    - [ ] Dashboard so sánh các thành phố và phát hiện bất thường (Anomaly detection).
- [ ] **3. Business Analysis**
    - [ ] Thực hiện các phân tích nâng cao: Sự tương quan giữa các yếu tố thời tiết.
    - [ ] Viết tóm tắt các insight tìm thấy trong dữ liệu.
- [ ] **4. Final Report & Documentation**
    - [ ] Viết báo cáo đồ án (Word/Latex) bao gồm kiến trúc, công nghệ và kết quả.
    - [ ] Hoàn thiện README, sơ đồ Architect và hướng dẫn cài đặt (GUIDE.md).

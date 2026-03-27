# Lakehouse Docker Architecture

Hệ thống Lakehouse hoàn chỉnh triển khai trên Docker, sử dụng kiến trúc Medallion (Bronze - Silver - Gold) với Delta Lake và các công cụ dữ liệu phổ biến.

## 🏗 Kiến trúc hệ thống

Dữ liệu luân chuyển qua 3 lớp lưu trữ trên **MinIO**:
1.  **Bronze:** Dữ liệu thô (Raw data) từ Ingestion.
2.  **Silver:** Dữ liệu đã được làm sạch và chuẩn hóa.
3.  **Gold:** Dữ liệu đã được tổng hợp (Aggregated) sẵn sàng cho báo cáo.

### Các thành phần chính:
- **Lưu trữ:** MinIO (Object Storage tương thích S3).
- **Thu thập dữ liệu:** Apache NiFi & Kafka.
- **Xử lý dữ liệu:** Apache Spark 3.5.3 (hỗ trợ Delta Lake 3.1.0).
- **Điều phối (Orchestration):** Apache Airflow (Python 3.8).
- **Công cụ truy vấn:** Trino (Query Engine).
- **Trực quan hóa:** Apache Superset.

---

## 🚀 Hướng dẫn cài đặt

### 1. Yêu cầu hệ thống
- Docker & Docker Compose.
- RAM tối thiểu: 12GB - 16GB (để chạy mượt 16 containers).

### 2. Triển khai
```bash
# Khởi động toàn bộ hệ thống
docker compose up -d

# Kiểm tra trạng thái các container
docker compose ps
```

---

## 🔗 Danh sách dịch vụ & Cổng truy cập

| Dịch vụ | Cổng (Host) | Port (Internal) | Tài khoản (Mặc định) |
| :--- | :--- | :--- | :--- |
| **MinIO Console** | `9001` | 9001 | `admin` / `admin123456` |
| **Airflow UI** | `8082` | 8080 | `airflow` / `airflow` |
| **Superset UI** | `8088` | 8088 | `admin` / `admin` |
| **Spark Master** | `8080` | 8080 | - |
| **Trino** | `8085` | 8080 | `trino` (không pass) |
| **NiFi** | `8443` | 8443 | (HTTPS - check logs for pass) |

# Data Lakehouse Architecture on Docker

A complete Data Lakehouse system deployed on Docker, implementing the **Medallion Architecture** (Bronze - Silver - Gold) with Delta Lake and industry-standard big data tools.

## System Architecture

```
                          ┌──────────────────────┐
                          │   Apache Airflow      │
                          │   (Orchestration)     │
                          │   Port: 8082          │
                          └──────────┬───────────┘
                                     │ triggers spark-submit
                                     ▼
┌─────────────┐          ┌──────────────────────┐          ┌──────────────────────┐
│ Apache NiFi │          │   Apache Spark        │          │       MinIO          │
│ (Ingestion) │          │   Master + Worker     │◄────────►│  (S3-compatible      │
│ Port: 8443  │          │   Port: 8080, 7077    │  read/   │   Object Storage)    │
└─────────────┘          └──────────────────────┘  write    │  Port: 9000, 9001    │
                                                   Delta    │                      │
┌─────────────┐                                    Lake     │  ┌────────────────┐  │
│   Kafka +   │                                     ▲       │  │ Bronze (Raw)   │  │
│  Zookeeper  │                                     │       │  │ Silver (Clean) │  │
│ (Streaming) │                                     │       │  │ Gold (Agg)     │  │
│ Port: 29092 │                                     │       │  └────────────────┘  │
└─────────────┘                                     │       └──────────┬───────────┘
                                                    │                  │
                          ┌──────────────────────┐  │                  │
                          │       Trino           │──┘    reads Delta  │
                          │   (SQL Query Engine)  │◄───────────────────┘
                          │   Port: 8085          │
                          └──────────┬───────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  Apache Superset      │
                          │  (BI & Dashboards)    │
                          │  Port: 8088           │
                          └──────────────────────┘

                    ┌────────────┐  ┌────────────────┐
                    │ PostgreSQL │  │  PostgreSQL     │
                    │ (Airflow)  │  │  (Superset)     │
                    │ Port: 5432 │  │  Port: 5433     │
                    └────────────┘  └────────────────┘
```

## Medallion Architecture

Data flows through 3 storage layers on **MinIO**:

| Layer | Path | Purpose | Transformations |
|:------|:-----|:--------|:----------------|
| **Bronze** | `s3a://bronze/employees` | Raw data ingestion | Add `ingested_at` timestamp |
| **Silver** | `s3a://silver/employees_clean` | Cleaned & normalized data | Trim whitespace, uppercase cities, filter invalid records (salary > 0, age > 0), add `transformed_at` |
| **Gold** | `s3a://gold/employee_stats` | Aggregated analytics | Group by city: employee count, avg salary, avg age |

All layers use **Delta Lake** format, providing ACID transactions, schema enforcement, and time travel capabilities.

## Tech Stack

| Component | Technology | Version | Purpose |
|:----------|:-----------|:--------|:--------|
| Object Storage | MinIO | 2024-01-01 | S3-compatible data lake storage |
| Data Processing | Apache Spark | 3.5.3 | Distributed ETL engine |
| Table Format | Delta Lake | 3.1.0 | ACID transactions on data lake |
| Orchestration | Apache Airflow | 2.7.3 | Workflow scheduling & monitoring |
| SQL Query Engine | Trino | 435 | Federated SQL queries on Delta tables |
| Visualization | Apache Superset | 3.1.0 | BI dashboards & data exploration |
| Data Ingestion | Apache NiFi | 1.24.0 | Data routing & transformation |
| Event Streaming | Apache Kafka | 7.5.3 | Real-time event streaming |
| Coordination | Zookeeper | 7.5.3 | Kafka cluster coordination |
| Metadata DB | PostgreSQL | 15 | Airflow & Superset metadata stores |

## Project Structure

```
.
├── docker-compose.yml              # Orchestrates all services
├── README.md
├── .env                            # Environment variables (not tracked in git)
├── airflow/
│   ├── Dockerfile                  # Airflow image with Java & Spark support
│   ├── requirements.txt            # Python dependencies
│   └── dags/
│       └── example_dag.py          # ETL pipeline DAG definition
├── spark/
│   ├── Dockerfile                  # Spark image with Delta Lake JARs
│   ├── spark-defaults.conf         # Spark & S3A configuration
│   └── jobs/
│       └── example_etl.py          # Bronze → Silver → Gold ETL job
├── superset/
│   └── superset_config.py          # Superset configuration
├── trino/
│   └── catalog/
│       └── delta.properties        # Delta Lake connector config
└── minio/
    └── init-buckets.sh             # Creates bronze/silver/gold buckets
```

## Services & Ports

| Service | Port (Host) | Port (Internal) | Default Credentials |
|:--------|:------------|:----------------|:--------------------|
| **MinIO Console** | 9001 | 9001 | `admin` / `admin123456` |
| **MinIO API** | 9000 | 9000 | Same as above |
| **Airflow UI** | 8082 | 8080 | `airflow` / `airflow` |
| **Superset UI** | 8088 | 8088 | `admin` / `admin` |
| **Spark Master UI** | 8080 | 8080 | - |
| **Spark Worker UI** | 8081 | 8081 | - |
| **Trino** | 8085 | 8080 | `trino` (no password) |
| **NiFi** | 8443 | 8443 | `admin` / `admin123456789` (HTTPS) |
| **Kafka** | 29092 | 9092 | - |
| **Zookeeper** | 2181 | 2181 | - |
| **PostgreSQL (Airflow)** | 5432 | 5432 | From `.env` |
| **PostgreSQL (Superset)** | 5433 | 5432 | From `.env` |

## ETL Pipeline

The pipeline is orchestrated by **Airflow** (DAG: `lakehouse_etl_pipeline`, scheduled `@daily`):

```
┌─────────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  submit_spark_etl   │────►│ check_bronze     │     │ check_silver     │     │ check_gold       │
│  (spark-submit to   │────►│ (validate Bronze │     │ (validate Silver │     │ (validate Gold   │
│   Spark cluster)    │────►│  layer)          │     │  layer)          │     │  layer)          │
└─────────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
```

**ETL Job** (`spark/jobs/example_etl.py`):

1. **Bronze Ingestion** - Creates sample employee records and writes to `s3a://bronze/employees` in Delta format
2. **Silver Transformation** - Reads Bronze, applies data quality rules (trim, uppercase, filter invalid), writes to `s3a://silver/employees_clean`
3. **Gold Aggregation** - Reads Silver, groups by city with aggregations (count, avg salary, avg age), writes to `s3a://gold/employee_stats`

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Minimum RAM: **12-16 GB** (to run all 14+ containers)

### Setup

1. **Create `.env` file** in the project root:

```env
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123456

# PostgreSQL - Airflow
POSTGRES_AIRFLOW_USER=airflow
POSTGRES_AIRFLOW_PASSWORD=airflow
POSTGRES_AIRFLOW_DB=airflow

# PostgreSQL - Superset
POSTGRES_SUPERSET_USER=superset
POSTGRES_SUPERSET_PASSWORD=superset123
POSTGRES_SUPERSET_DB=superset

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key-here

# Superset
SUPERSET_SECRET_KEY=your-superset-secret-key
```

2. **Start the system:**

```bash
docker compose up -d
```

3. **Check container status:**

```bash
docker compose ps
```

4. **Access the services** via the ports listed in the [Services & Ports](#services--ports) table.

## Architecture Assessment

### Strengths

| Area | Assessment |
|:-----|:-----------|
| **Overall Architecture** | Medallion pattern (Bronze/Silver/Gold) is industry-standard and well-implemented |
| **Tech Stack** | Proven combination: Spark + Delta Lake + MinIO + Trino + Superset |
| **Containerization** | Health checks, dependency ordering (`depends_on` + `condition`), persistent volumes |
| **Data Format** | Delta Lake provides ACID transactions, schema enforcement, and time travel |
| **Network Isolation** | Single bridge network for inter-service communication |

### Recommendations for Improvement

| # | Issue | Severity | Recommendation |
|:--|:------|:---------|:---------------|
| 1 | Credentials hardcoded in `spark-defaults.conf`, `delta.properties`, DAG file, and `superset_config.py` | HIGH | Use environment variables or secret management (Docker Secrets, Vault) for all credentials |
| 2 | Kafka & NiFi are deployed but unused by any pipeline code | MEDIUM | Either integrate them (NiFi → Kafka → Spark Streaming → Bronze) or remove to save ~2-3GB RAM |
| 3 | Validation tasks (`check_bronze/silver/gold`) are placeholders (`echo` + `sleep`) | MEDIUM | Implement real validation: record counts, schema checks, data freshness verification |
| 4 | File-based Hive Metastore in Trino | MEDIUM | Use Hive Metastore Service (HMS) with PostgreSQL for shared metadata across Spark and Trino |
| 5 | No monitoring or alerting stack | MEDIUM | Add Prometheus + Grafana for Spark jobs, Airflow health, and MinIO storage metrics |
| 6 | Superset security disabled (CSRF, Talisman, secure cookies all off) | HIGH (prod) | Enable security features for any non-development deployment |
| 7 | BashOperator used for spark-submit instead of SparkSubmitOperator | LOW | Migrate to `SparkSubmitOperator` for better error handling and connection management |
| 8 | Single Spark worker (2 cores, 2GB) | LOW (dev) | Add workers and resource management (YARN/K8s) for production workloads |

### Summary

The architecture is well-designed and follows established Data Lakehouse patterns. It is suitable for **learning and demonstration purposes**. For production readiness, the key priorities are: secrets management, real data source integration, monitoring, and security hardening.

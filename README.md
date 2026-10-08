# Real-time Financial Lakehouse Data Platform

[![CI Pipeline](https://img.shields.io/badge/CI-pytest%20passing-brightgreen)](https://github.com)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-blue)](https://iceberg.apache.org/)
[![Storage](https://img.shields.io/badge/Storage-MinIO%20S3-orange)](https://min.io/)
[![Engine](https://img.shields.io/badge/Engine-Spark%203.5%20%7C%20Trino%20480-red)](https://trino.io/)

A production-grade, real-time financial streaming data platform built on **Apache Iceberg**, **MinIO (S3 Object Storage)**, **Apache Kafka**, **PySpark Structured Streaming**, **Trino**, and **Apache Airflow**.

The platform ingests live market trades via **Finnhub WebSocket**, protects ingestion integrity via **Dead Letter Queues (DLQ)**, processes micro-batches with PySpark, calculates **OHLCV Candlesticks** and **VWAP (Volume-Weighted Average Price)**, enforces **Medallion Lakehouse Tiers**, and orchestrates lakehouse maintenance and dimension data synchronization via **Airflow**.

---

## 🏛️ Platform Architecture

```
[ Finnhub WebSocket ] (Real-time Trades)
         │
         ▼
[ Finnhub Ingestion Service ] ──────(Validation / Parse Failures)─────► [ Kafka DLQ: stock_trades_dlq ]
         │ (Idempotent Producer)
         ▼
[ Kafka Topic: stock_trades ]
         │
         ▼
[ PySpark Structured Streaming ] (Watermarking, Deduplication, Windowing)
   ├── Tier 1: Silver Cleaned Trades  ──► iceberg.stock.raw_stream_data
   ├── Tier 2: Gold OHLCV & VWAP Bars ──► iceberg.stock.aggregated_data
   └── Tier 3: Gold Market Anomalies  ──► iceberg.stock.market_anomalies
         │
         ▼
┌────────────────────────────────────────────────────────────────────────┐
│             Apache Iceberg Catalog (Hive Metastore backed)             │
│                 Storage Layer: MinIO S3 (s3a://warehouse)              │
│                                                                        │
│   • stock.raw_stream_data    (Silver: Partitioned by Day, Symbol)      │
│   • stock.aggregated_data    (Gold: 1m OHLCV & VWAP Candlesticks)      │
│   • stock.market_anomalies   (Gold: Block trades & Price shocks)       │
│   • stock.dim_company        (Dimension: Company profile & Sector)     │
└────────────────────────────────────────────────────────────────────────┘
         ▲                                                ▲
         │ (Ad-hoc Analytics & Queries)                   │ (Lakehouse Maintenance)
   [ Trino SQL Engine ]                             [ Apache Airflow ]
         │                                          • iceberg_lakehouse_maintenance
         ▼                                          • finnhub_dimension_sync
   [ Apache Superset ]                              • data_quality_reconciliation
 (Interactive Dashboards)                           • pipeline_health_monitor
```

---

## 📂 Project Structure (Clean Architecture)

```text
.
├── application/             # Application services & streaming runners
│   └── services/            # StreamingService (with DLQ), PipelineHealthService
├── domain/                  # Core domain entities & financial use cases
│   ├── entities/            # TradeEvent, StockCandle, DeadLetterEvent, AnomalyEvent
│   └── use_cases/           # IngestTrade, ValidateTrade, DetectAnomaly
├── infrastructure/          # Adapters for third-party infrastructure
│   ├── finnhub/             # WebSocketClient (with error routing), MessageParser
│   ├── kafka/               # KafkaTradeProducer (DLQ enabled), TopicAdmin
│   ├── spark/               # SessionFactory (S3A), Transformer, TradeAggregator (OHLCV/VWAP)
│   └── iceberg/             # TableManager, IcebergStreamWriter
├── interfaces/              # Orchestration & boundary layers
│   └── airflow/dags/        # Production DAGs: Maintenance, Dim Sync, DQ, Monitor
├── docker/                  # Service Dockerfiles & initialization scripts
│   ├── airflow/             # Airflow image with Trino & Requests providers
│   ├── hive/                # Hive Metastore with PostgreSQL driver & S3A config
│   ├── minio/               # S3 storage scripts
│   ├── spark/               # PySpark base image
│   ├── trino/               # Trino with Iceberg S3 catalog configuration
│   └── superset/            # Superset image with Trino driver
├── scripts/                 # Operational CLI bootstrap and health scripts
├── tests/                   # Automated unit & integration test suites
│   ├── unit/                # Domain & parser unit tests
│   └── integration/         # Spark transformation & aggregation tests
├── docker-compose.yaml      # Multi-container local orchestration
├── pytest.ini               # Pytest root configuration
└── .env.example             # Configuration templates
```

---

## 🚀 Key Engineering Highlights

### 1. Financial Analytics: OHLCV & VWAP
Unlike naive streaming tutorials that only calculate average prices, this platform computes real-time quantitative trading metrics using PySpark Structured Streaming window functions:
- **Open**: Earliest price in window (`min_by(price, trade_timestamp)`)
- **High**: Peak price in window (`max(price)`)
- **Low**: Lowest price in window (`min(price)`)
- **Close**: Latest price in window (`max_by(price, trade_timestamp)`)
- **VWAP**: Volume-Weighted Average Price: $\text{VWAP} = \frac{\sum (\text{Price} \times \text{Volume})}{\sum \text{Volume}}$
- **Anomaly Stream**: Real-time detection of large block trades ($\ge 50,000$ shares) and rapid price dislocations.

### 2. Resilient Ingestion & Dead Letter Queue (DLQ)
- **Zero Data Loss**: Any trade failing JSON parsing or domain invariants (`price <= 0`, `volume < 0`) is automatically published to `stock_trades_dlq` along with error reason and audit timestamps.
- **Idempotent Kafka Producer**: Enabled with `enable.idempotence=true`, `acks=all`, and `snappy` compression.

### 3. S3 Cloud-Native Lakehouse (MinIO + Apache Iceberg v2)
- Replaces POSIX file directories with **MinIO S3-compatible Object Storage** (`s3a://warehouse/stock`).
- Decouples compute (Spark/Trino) from storage (S3).
- Uses Iceberg **Format Version 2** with hash write distribution and daily partition specs.

### 4. Production Airflow Orchestration (No Anti-Patterns)
Instead of misusing Airflow as a Docker container starter, Airflow performs real data platform operations:
1. **`iceberg_lakehouse_maintenance`**: Executes Trino compaction (`EXECUTE optimize`), snapshot expiration (`EXECUTE expire_snapshots`), and orphan file removal (`EXECUTE remove_orphan_files`) to eliminate the **Small Files Problem**.
2. **`finnhub_dimension_sync`**: Ingests company profile metadata (Industry, Market Cap, IPO date) from Finnhub REST API into `stock.dim_company`.
3. **`data_quality_reconciliation`**: Asserts financial contracts (no negative prices, $High \ge Low$, $High \ge Open$, $High \ge Close$).
4. **`pipeline_health_monitor`**: Monitors service health across Kafka, Trino, and MinIO.

---

## ⚡ Quickstart Guide

### 1. Prerequisites
- Docker Engine & Docker Compose (v2.20+)
- Python 3.10+ (for local development)
- Finnhub API Key (get free key at [finnhub.io](https://finnhub.io))

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env and insert your FINNHUB_API_KEY
```

### 3. Launch Platform Infrastructure
```bash
docker compose up --build -d
```

### 4. Bootstrap Kafka Topics & Iceberg Lakehouse
```bash
# Bootstrap Kafka data topic and DLQ topic
docker compose run --rm stock-producer python3 scripts/bootstrap_kafka_topic.py

# Bootstrap Iceberg namespaces and Medallion tables
docker compose exec spark-master python3 scripts/bootstrap_iceberg.py
```

### 5. Verify Running Services
```bash
docker compose ps
```

---

## 🌐 Service Web UIs & Endpoints

| Service | Port | Endpoint | Credentials |
| :--- | :--- | :--- | :--- |
| **MinIO S3 Console** | `9001` | [http://localhost:9001](http://localhost:9001) | `admin` / `admin12345` |
| **MinIO S3 API** | `9000` | `http://localhost:9000` | `admin` / `admin12345` |
| **Trino Query UI** | `8080` | [http://localhost:8080](http://localhost:8080) | User: `trino` |
| **Spark Master UI** | `8081` | [http://localhost:8081](http://localhost:8081) | None |
| **Airflow Web UI** | `8088` | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| **Superset BI** | `8098` | [http://localhost:8098](http://localhost:8098) | `admin` / `admin` |
| **Kafka Broker** | `9092` / `29092` | `localhost:29092` | None |

---

## 📊 Analytical SQL Queries (Trino)

Open the Trino CLI or execute queries in Superset:

### 1. Real-time OHLCV & VWAP Candlestick Bars
```sql
SELECT
    symbol,
    window_start,
    open_price,
    high_price,
    low_price,
    close_price,
    vwap,
    total_volume,
    trade_count
FROM iceberg.stock.aggregated_data
ORDER BY window_start DESC
LIMIT 20;
```

### 2. Star Schema Analysis: Fact Trades Joined with Dim Company
```sql
SELECT
    c.company_name,
    c.finnhub_industry,
    c.market_capitalization,
    a.symbol,
    a.window_start,
    a.close_price,
    a.vwap,
    a.total_volume
FROM iceberg.stock.aggregated_data a
JOIN iceberg.stock.dim_company c ON a.symbol = c.symbol
WHERE a.trade_date = CURRENT_DATE
ORDER BY a.total_volume DESC
LIMIT 50;
```

### 3. Market Anomalies (Block Trades Monitor)
```sql
SELECT
    symbol,
    trade_timestamp,
    price,
    volume,
    anomaly_type,
    description,
    severity
FROM iceberg.stock.market_anomalies
ORDER BY trade_timestamp DESC
LIMIT 25;
```

---

## 📈 Real-Time Business Intelligence (Apache Superset)

The platform provides out-of-the-box analytical dashboards connected directly to the Trino distributed query engine (`trino://trino@trino:8080/iceberg/stock`):

1. **Stock Price & VWAP Trend** (*Time-Series Line Chart*): Plots close price and Volume-Weighted Average Price across 1-minute event-time windows.
2. **Trading Volume by Symbol** (*Time-Series Bar Chart*): Visualizes liquidity and market depth across active tickers (`BINANCE:BTCUSDT`, `AAPL`, `MSFT`, `AMZN`, `GOOGL`).
3. **Total Processed Trades** (*Big Number with Trendline*): Live KPI counter measuring streaming throughput.
4. **Real-time Market Anomalies** (*Interactive Table*): Audit log of detected institutional block trades ($\ge 50,000$ shares) and rapid price deviations.

> **Auto-Refresh**: Enable the Superset dashboard 10-second auto-refresh interval for zero-reload live financial monitoring.

---

## ⚡ Demo Data Generation

To quickly populate the Lakehouse with realistic historical candlestick trends and anomaly events for portfolio presentation and dashboard visualization:

```bash
# Seed 120 historical 1-minute OHLCV/VWAP candles per symbol + anomaly events
python scripts/seed_demo_data.py
```

---

## 🧪 Automated Testing

Run the test suite locally with `pytest`:

```bash
# Run full unit test suite (OHLCV formulas, DLQ serialization, Finnhub parser, settings)
pytest -v
```

---

## ☁️ 24/7 Production Deployment (Cloud VM)

When running locally, stopping the host machine terminates the Docker daemon. To run this streaming platform 24/7 continuously:

1. **Provision a Cloud VM**:
   - **Oracle Cloud (OCI) Always Free**: 4 OCPUs, 24 GB RAM Ampere A1 Compute instance (100% free forever).
   - **AWS / GCP / DigitalOcean**: 4 vCPU, 8–16 GB RAM Ubuntu instance.
2. **Deploy via Docker Compose**:
   ```bash
   git clone https://github.com/Bui-Tien-Dat-2808/Fintech-Finance.git
   cd Fintech-Finance
   cp .env.example .env  # set your FINNHUB_API_KEY
   docker compose up --build -d
   ```
3. The platform will run autonomously around the clock, continuously streaming market data and executing Airflow maintenance workflows.

## 💼 CV Descriptions & Portfolio Highlights

### Format A: Concise 1-Page Resume Format (Matches PDF Layout)
```markdown
Real-Time Stock Data Lakehouse                               Mar 2026 - Apr 2026

• Description: Designed and built an end-to-end Medallion Lakehouse streaming platform ingesting real-time stock trades from Finnhub WebSocket into Apache Iceberg on MinIO S3 object storage using PySpark Structured Streaming.

• Tech stack: Python, Apache Iceberg, MinIO (S3), PySpark, Apache Kafka, Trino, Apache Airflow, Apache Superset, Docker.

• Outcome: Delivered a sub-second financial streaming platform computing real-time OHLCV and VWAP metrics, featuring a Kafka Dead Letter Queue (DLQ) for zero data loss, automated Airflow lakehouse compaction, and an interactive Superset trading dashboard.

• Project link: https://github.com/Bui-Tien-Dat-2808/Fintech-Finance
```

### Format B: Detailed Bullet Points (For 2-Page CVs, Portfolio & LinkedIn)
> **Real-Time Financial Streaming Lakehouse Platform** *(Mar 2026 – Apr 2026)*  
> *Tech Stack: Apache Iceberg, MinIO (S3), PySpark, Apache Kafka, Trino, Apache Airflow, Apache Superset, Docker, Pytest, Python.*  
> - **Architecture & Storage**: Engineered an end-to-end Medallion Lakehouse platform ingesting live trade events from Finnhub WebSocket; implemented Apache Iceberg v2 open table format on MinIO S3 object storage with Hive Metastore catalog, enabling ACID transactions, hidden partitioning, and schema evolution.
> - **Streaming & Financial Analytics**: Built PySpark Structured Streaming jobs with event-time watermarking and 1-minute tumbling windows; computed quantitative trading indicators including OHLCV candlesticks and Volume-Weighted Average Price (VWAP) with sub-second latency.
> - **Resilience & Fault Tolerance**: Designed an idempotent Kafka producer with a Dead Letter Queue (DLQ) pattern, automatically isolating malformed ticks and preventing data corruption with zero silent drops.
> - **Anomaly Detection**: Developed real-time rule-based anomaly detection streaming filters to flag institutional large block trades ($\ge 50,000$ shares) and price dislocation events into a dedicated Iceberg Gold table.
> - **Orchestration & Data Quality**: Orchestrated 4 production Airflow DAGs automating Iceberg table maintenance (small-file compaction, snapshot expiration, orphan file cleanup), Finnhub REST dimension synchronization (Star-Schema), and continuous data quality invariant assertions.
> - **Serving & Visualization**: Integrated Trino distributed SQL query engine with Apache Superset to deliver real-time interactive dashboards with 10-second auto-refreshing financial KPIs.

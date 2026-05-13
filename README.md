# Real-time Stock Data Streaming Pipeline

## Overview
Project này xây dựng một pipeline streaming dữ liệu trade cổ phiếu theo hướng gần production. Dữ liệu real-time được ingest từ Finnhub WebSocket, đẩy vào Kafka, xử lý bằng PySpark Structured Streaming, lưu vào Apache Iceberg, query qua Trino và trực quan hóa bằng Superset. Airflow đảm nhiệm orchestration, còn toàn bộ Python code theo Clean Architecture.

## Architecture
```text
Finnhub WebSocket
        |
        v
Python Ingestion Service
        |
        v
Kafka topic: stock_trades
        |
        v
PySpark Structured Streaming
        |
        +--> Iceberg raw_stream_data
        |
        +--> 1-minute aggregation
                |
                v
          Iceberg aggregated_data
                |
                v
              Trino
                |
                v
            Superset
```

## Project structure
```text
.
|-- application/
|-- domain/
|-- infrastructure/
|   |-- finnhub/
|   |-- kafka/
|   |-- spark/
|   `-- iceberg/
|-- interfaces/
|   `-- airflow/
|-- shared/
|   |-- config/
|   `-- logging/
|-- scripts/
|-- tests/
|-- docker/
|-- docker-compose.yaml
|-- .env.example
`-- README.md
```

## Clean Architecture mapping
- `domain/`: entity và business rule cốt lõi cho trade event.
- `application/`: orchestration service sử dụng các domain use case.
- `infrastructure/`: adapter cho Finnhub, Kafka, Spark, Iceberg và Trino-related bootstrap.
- `interfaces/airflow/`: DAG orchestration.
- `shared/`: cấu hình môi trường và logging dùng chung.

## Main components
### 1. Ingestion service
- Kết nối `wss://ws.finnhub.io`
- Subscribe nhiều symbols từ `STOCK_SYMBOLS`
- Parse payload trade về `TradeEvent`
- Validate domain rule trước khi publish Kafka
- Kafka producer dùng `acks=all`, retry và delivery callback
- Logging cho connect, subscribe, delivery và reconnect

### 2. Spark streaming
- Source: Kafka Structured Streaming
- Parsing: JSON schema cố định
- Cleaning:
  - Drop null ở các field bắt buộc
  - Filter `price > 0`
  - Filter `volume >= 0`
  - Convert timestamp sang Spark `timestamp`
- Deduplication theo `symbol + trade_timestamp`
- Watermark mặc định: `2 minutes`
- Aggregation window: `1 minute`
- Checkpointing tách riêng cho raw và aggregate writer

### 3. Iceberg + Trino + Superset
- Catalog Spark: `stock_catalog`
- Trino catalog: `iceberg`
- Tables:
  - `stock_catalog.stock.raw_stream_data`
  - `stock_catalog.stock.aggregated_data`
- Partition:
  - Raw: `days(trade_timestamp), symbol`
  - Aggregate: `days(window_start), symbol`
- Shared warehouse path giữa Spark, Hive và Trino: `/data/warehouse`

### 4. Airflow orchestration
- `stock_pipeline_start`
  - check dependencies
  - ensure Kafka topic
  - ensure Iceberg namespace/tables
  - start Spark job container
  - start producer container
- `stock_pipeline_monitor`
  - chạy health check định kỳ
- `stock_pipeline_stop`
  - dừng producer và Spark job

## Environment variables
Copy file mẫu:

```bash
cp .env.example .env
```

Điền tối thiểu:
- `FINNHUB_API_KEY`
- `STOCK_SYMBOLS`
- `SUPERSET_SECRET_KEY`

Các biến quan trọng khác:
- `KAFKA_BROKER`
- `KAFKA_TOPIC`
- `SPARK_CHECKPOINT_ROOT`
- `ICEBERG_CATALOG_NAME`
- `ICEBERG_WAREHOUSE`
- `HIVE_METASTORE_URI`
- `TRINO_HOST`
- `TRINO_PORT`

## How to run
### 1. Start platform services
```bash
docker compose up --build -d
```

Nếu muốn chỉ khởi động hạ tầng trước:

```bash
docker compose up -d zookeeper kafka postgres hive-metastore spark-master spark-worker trino airflow-webserver airflow-scheduler superset
```

### 2. Bootstrap manually
```bash
docker compose run --rm stock-producer python3 scripts/bootstrap_kafka_topic.py
docker compose run --rm spark-streaming-job /opt/spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/project/scripts/bootstrap_iceberg.py
```

### 3. Start streaming jobs
```bash
docker compose up -d stock-producer spark-streaming-job
```

### 4. Orchestrate with Airflow
Sau khi Airflow UI sẵn sàng, dùng DAG theo thứ tự:
- chạy `stock_pipeline_start` để bootstrap và start pipeline
- dùng `stock_pipeline_monitor` để theo dõi health
- chỉ chạy `stock_pipeline_stop` khi muốn dừng `stock-producer` và `spark-streaming-job`

Không chạy `stock_pipeline_start` và `stock_pipeline_stop` cùng lúc.

### 5. Open UIs
- Airflow: `http://localhost:8088`
- Superset: `http://localhost:8098`
- Spark Master UI: `http://localhost:8081`
- Trino UI: `http://localhost:8080`

Default local credentials:
- Airflow: `admin / admin`
- Superset: `admin / admin`

## Superset setup
Tạo database connection trong Superset bằng SQLAlchemy URI:

```text
trino://trino@trino:8080/iceberg/stock
```

Sau đó tạo các chart gợi ý:
- Real-time price chart theo `trade_timestamp`
- Average price theo `window_start`
- Top active stocks theo `trade_count` hoặc `total_volume`

## Example queries
Raw stream:

```sql
SELECT *
FROM iceberg.stock.raw_stream_data
ORDER BY trade_timestamp DESC
LIMIT 20;
```

Aggregated metrics:

```sql
SELECT symbol, window_start, avg_price, total_volume, trade_count
FROM iceberg.stock.aggregated_data
ORDER BY window_start DESC
LIMIT 50;
```

Quick validation:

```sql
SELECT count(*) AS raw_count
FROM iceberg.stock.raw_stream_data;
```

```sql
SELECT count(*) AS aggregated_count
FROM iceberg.stock.aggregated_data;
```

## Testing
Chạy unit và integration tests:

```bash
pytest tests/unit
pytest tests/integration
```

## Reliability notes
- Kafka offset management được Spark quản lý qua checkpoint.
- WebSocket client có reconnect loop và resubscribe.
- `maxOffsetsPerTrigger` giúp hạn chế burst load.
- Watermark xử lý late data trong phạm vi cấu hình.
- Iceberg table dùng format version 2 để sẵn sàng cho schema evolution cơ bản.
- `IcebergTableManager` có logic recover khi metastore còn stale metadata nhưng file metadata Iceberg đã mất.

## Troubleshooting
### Query không có dữ liệu
Kiểm tra theo thứ tự:
- `stock-producer` có log delivery thành công vào Kafka
- `spark-streaming-job` không bị crash và có ghi raw/aggregate stream
- Trino query `count(*)` trên cả hai bảng không còn bằng `0`

Một số lệnh hữu ích:

```bash
docker logs fintechfinance-stock-producer-1 --tail 100
docker logs fintechfinance-spark-streaming-job-1 --tail 100
docker exec -it fintechfinance-trino-1 trino --execute "SELECT count(*) FROM iceberg.stock.raw_stream_data"
docker exec -it fintechfinance-trino-1 trino --execute "SELECT count(*) FROM iceberg.stock.aggregated_data"
```

### Kafka bị lệch cluster ID
Nếu Kafka báo `InconsistentClusterIdException`, reset riêng volume Kafka:

```bash
docker compose down
docker volume rm fintechfinance_kafka_data
docker compose up -d zookeeper kafka
```

### Docker Desktop hoặc WSL thiếu tài nguyên
Nếu build hoặc Spark job hay chết bất thường, tăng memory cho WSL/Docker Desktop trước khi chạy lại stack.

## Manual validation checklist
- Producer log cho thấy WebSocket connected và subscribed đúng symbols.
- Kafka topic `stock_trades` có offset tăng.
- Spark log cho thấy raw writer và aggregated writer đã start.
- Truy vấn Trino trả về dữ liệu ở cả `raw_stream_data` và `aggregated_data`.
- Dashboard Superset render được các biểu đồ chính.

## Future improvements
- Thêm metrics exporter cho Prometheus/Grafana.
- Thêm dead-letter topic cho malformed events.
- Thêm CI pipeline và data quality checks.
- Thêm automated end-to-end integration test bằng Testcontainers.

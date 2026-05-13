from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _read_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Environment variable {name} is required.")
    if value is None:
        raise ValueError(f"Environment variable {name} is not set.")
    return value


@dataclass(slots=True)
class Settings:
    finnhub_api_key: str
    finnhub_websocket_url: str
    kafka_broker: str
    kafka_topic: str
    kafka_client_id: str
    kafka_topic_partitions: int
    kafka_topic_replication_factor: int
    kafka_retries: int
    kafka_linger_ms: int
    kafka_flush_timeout_seconds: int
    stock_symbols: list[str]
    log_level: str
    spark_app_name: str
    spark_master_url: str
    spark_checkpoint_root: str
    spark_watermark_delay: str
    spark_shuffle_partitions: str
    spark_max_offsets_per_trigger: int
    iceberg_catalog_name: str
    iceberg_namespace: str
    iceberg_warehouse: str
    hive_metastore_uri: str
    trino_host: str
    trino_port: int
    trino_catalog: str
    trino_schema: str
    websocket_ping_interval_seconds: int
    websocket_ping_timeout_seconds: int
    websocket_reconnect_delay_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            finnhub_api_key=_read_env("FINNHUB_API_KEY", required=True),
            finnhub_websocket_url=_read_env(
                "FINNHUB_WEBSOCKET_URL",
                "wss://ws.finnhub.io",
            ),
            kafka_broker=_read_env("KAFKA_BROKER", "kafka:9092"),
            kafka_topic=_read_env("KAFKA_TOPIC", "stock_trades"),
            kafka_client_id=_read_env("KAFKA_CLIENT_ID", "stock-trade-producer"),
            kafka_topic_partitions=int(_read_env("KAFKA_TOPIC_PARTITIONS", "3")),
            kafka_topic_replication_factor=int(
                _read_env("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
            ),
            kafka_retries=int(_read_env("KAFKA_RETRIES", "10")),
            kafka_linger_ms=int(_read_env("KAFKA_LINGER_MS", "100")),
            kafka_flush_timeout_seconds=int(
                _read_env("KAFKA_FLUSH_TIMEOUT_SECONDS", "15")
            ),
            stock_symbols=[
                symbol.strip()
                for symbol in _read_env("STOCK_SYMBOLS", "AAPL,MSFT,AMZN").split(",")
                if symbol.strip()
            ],
            log_level=_read_env("LOG_LEVEL", "INFO"),
            spark_app_name=_read_env("SPARK_APP_NAME", "stock-streaming-job"),
            spark_master_url=_read_env("SPARK_MASTER_URL", "spark://spark-master:7077"),
            spark_checkpoint_root=_read_env(
                "SPARK_CHECKPOINT_ROOT",
                "/opt/project/.checkpoints",
            ),
            spark_watermark_delay=_read_env("SPARK_WATERMARK_DELAY", "2 minutes"),
            spark_shuffle_partitions=_read_env("SPARK_SHUFFLE_PARTITIONS", "4"),
            spark_max_offsets_per_trigger=int(
                _read_env("SPARK_MAX_OFFSETS_PER_TRIGGER", "5000")
            ),
            iceberg_catalog_name=_read_env("ICEBERG_CATALOG_NAME", "stock_catalog"),
            iceberg_namespace=_read_env("ICEBERG_NAMESPACE", "stock"),
            iceberg_warehouse=_read_env(
                "ICEBERG_WAREHOUSE",
                "file:///data/warehouse",
            ),
            hive_metastore_uri=_read_env(
                "HIVE_METASTORE_URI",
                "thrift://hive-metastore:9083",
            ),
            trino_host=_read_env("TRINO_HOST", "trino"),
            trino_port=int(_read_env("TRINO_PORT", "8080")),
            trino_catalog=_read_env("TRINO_CATALOG", "iceberg"),
            trino_schema=_read_env("TRINO_SCHEMA", "stock"),
            websocket_ping_interval_seconds=int(
                _read_env("WEBSOCKET_PING_INTERVAL_SECONDS", "20")
            ),
            websocket_ping_timeout_seconds=int(
                _read_env("WEBSOCKET_PING_TIMEOUT_SECONDS", "10")
            ),
            websocket_reconnect_delay_seconds=int(
                _read_env("WEBSOCKET_RECONNECT_DELAY_SECONDS", "5")
            ),
        )

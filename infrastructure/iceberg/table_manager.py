from __future__ import annotations

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from shared.config.settings import Settings
from shared.logging.logger import get_logger


class IcebergTableManager:
    """Creates schema and tables required by the streaming pipeline."""

    def __init__(self, settings: Settings, spark: SparkSession) -> None:
        self._settings = settings
        self._spark = spark
        self._logger = get_logger(self.__class__.__name__)

    def ensure_namespace(self) -> None:
        namespace = self._settings.iceberg_namespace
        catalog = self._settings.iceberg_catalog_name
        self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
        self._logger.info("Iceberg namespace ensured: %s.%s", catalog, namespace)

    def ensure_tables(self) -> None:
        catalog = self._settings.iceberg_catalog_name
        namespace = self._settings.iceberg_namespace

        raw_table_name = f"{catalog}.{namespace}.raw_stream_data"
        raw_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {raw_table_name} (
            symbol STRING,
            price DOUBLE,
            volume BIGINT,
            trade_timestamp TIMESTAMP,
            ingestion_timestamp TIMESTAMP,
            source STRING,
            trade_date DATE
        )
        USING iceberg
        PARTITIONED BY (days(trade_timestamp), symbol)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.distribution-mode' = 'hash'
        )
        """

        aggregated_table_name = f"{catalog}.{namespace}.aggregated_data"
        aggregated_table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {aggregated_table_name} (
            symbol STRING,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            avg_price DOUBLE,
            min_price DOUBLE,
            max_price DOUBLE,
            total_volume BIGINT,
            trade_count BIGINT,
            trade_date DATE
        )
        USING iceberg
        PARTITIONED BY (days(window_start), symbol)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.distribution-mode' = 'hash'
        )
        """

        self._ensure_table(raw_table_name, raw_table_ddl)
        self._ensure_table(aggregated_table_name, aggregated_table_ddl)
        self._logger.info("Iceberg tables ensured in namespace=%s", namespace)

    def _ensure_table(self, table_name: str, ddl: str) -> None:
        try:
            self._spark.sql(ddl)
        except Py4JJavaError as exc:
            if not self._is_stale_metadata_error(exc):
                raise

            self._logger.warning(
                "Detected stale Iceberg metadata for table=%s. "
                "Dropping table metadata and recreating it.",
                table_name,
            )
            self._spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            self._spark.sql(ddl)
            self._logger.info("Recovered stale Iceberg metadata for table=%s", table_name)

    @staticmethod
    def _is_stale_metadata_error(error: Py4JJavaError) -> bool:
        message = str(error)
        stale_markers = [
            "org.apache.iceberg.exceptions.NotFoundException",
            "metadata.json does not exist",
            "Failed to open input stream for file",
        ]
        return all(marker in message for marker in stale_markers)

from __future__ import annotations

from infrastructure.iceberg.table_manager import IcebergTableManager
from infrastructure.iceberg.writer import IcebergStreamWriter
from infrastructure.spark.aggregator import TradeAggregator
from infrastructure.spark.kafka_reader import KafkaTradeStreamReader
from infrastructure.spark.session_factory import SparkSessionFactory
from infrastructure.spark.transformer import TradeStreamTransformer
from shared.config.settings import Settings
from shared.logging.logger import get_logger


class StockStreamingJob:
    """Coordinates Spark structured streaming from Kafka to Iceberg."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)

    def run(self) -> None:
        spark = SparkSessionFactory.create(self._settings)
        table_manager = IcebergTableManager(self._settings, spark)
        reader = KafkaTradeStreamReader(self._settings)
        writer = IcebergStreamWriter(self._settings)

        self._logger.info("Ensuring Iceberg namespace and tables before streaming.")
        table_manager.ensure_namespace()
        table_manager.ensure_tables()

        source_df = reader.read(spark)
        clean_df = TradeStreamTransformer.clean_and_deduplicate(
            source_df,
            self._settings.spark_watermark_delay,
        )
        aggregated_df = TradeAggregator.aggregate_1m(clean_df)

        queries = [
            writer.write_raw(clean_df),
            writer.write_aggregated(aggregated_df),
        ]

        self._logger.info("Spark structured streaming job started.")
        try:
            spark.streams.awaitAnyTermination()
        finally:
            for query in queries:
                if query.isActive:
                    query.stop()
            spark.stop()

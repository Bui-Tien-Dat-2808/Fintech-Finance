from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from shared.config.settings import Settings
from shared.logging.logger import get_logger


class IcebergStreamWriter:
    """Writes streaming DataFrames to Iceberg via foreachBatch."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)

    def write_raw(self, df: DataFrame) -> StreamingQuery:
        target_table = self._target_table("raw_stream_data")
        checkpoint = f"{self._settings.spark_checkpoint_root}/raw_stream_data"
        self._logger.info("Starting raw stream write. table=%s", target_table)
        return (
            df.writeStream.outputMode("append")
            .foreachBatch(
                lambda batch_df, batch_id: self._write_batch(
                    batch_df=batch_df,
                    batch_id=batch_id,
                    target_table=target_table,
                )
            )
            .option("checkpointLocation", checkpoint)
            .queryName("raw_stream_data_writer")
            .start()
        )

    def write_aggregated(self, df: DataFrame) -> StreamingQuery:
        target_table = self._target_table("aggregated_data")
        checkpoint = f"{self._settings.spark_checkpoint_root}/aggregated_data"
        self._logger.info("Starting aggregate stream write. table=%s", target_table)
        return (
            df.writeStream.outputMode("append")
            .foreachBatch(
                lambda batch_df, batch_id: self._write_batch(
                    batch_df=batch_df,
                    batch_id=batch_id,
                    target_table=target_table,
                )
            )
            .option("checkpointLocation", checkpoint)
            .queryName("aggregated_data_writer")
            .start()
        )

    def _write_batch(self, batch_df: DataFrame, batch_id: int, target_table: str) -> None:
        if batch_df.isEmpty():
            self._logger.info("Skipping empty batch. table=%s batch_id=%s", target_table, batch_id)
            return

        self._logger.info("Writing batch. table=%s batch_id=%s", target_table, batch_id)
        batch_df.writeTo(target_table).append()

    def _target_table(self, table_name: str) -> str:
        return (
            f"{self._settings.iceberg_catalog_name}."
            f"{self._settings.iceberg_namespace}."
            f"{table_name}"
        )

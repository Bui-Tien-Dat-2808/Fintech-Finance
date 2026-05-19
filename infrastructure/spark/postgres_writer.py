from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from shared.config.settings import Settings
from shared.logging.logger import get_logger


class PostgresStreamWriter:
    """Writes streaming DataFrames into PostgreSQL serving tables."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)
        self._jdbc_url = (
            f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/"
            f"{settings.postgres_database}"
        )
        self._connection_properties = {
            "driver": "org.postgresql.Driver",
            "user": settings.postgres_user,
            "password": settings.postgres_password,
        }

    def write_raw(self, df: DataFrame) -> StreamingQuery:
        checkpoint = f"{self._settings.spark_checkpoint_root}/raw_trades"
        return (
            df.writeStream.outputMode("append")
            .foreachBatch(
                lambda batch_df, batch_id: self._write_batch(
                    batch_df=batch_df,
                    batch_id=batch_id,
                    table_name=self._settings.postgres_raw_table,
                )
            )
            .option("checkpointLocation", checkpoint)
            .queryName("postgres_raw_trades_writer")
            .start()
        )

    def write_aggregated(self, df: DataFrame) -> StreamingQuery:
        checkpoint = f"{self._settings.spark_checkpoint_root}/aggregated_trades_1m"
        return (
            df.writeStream.outputMode("append")
            .foreachBatch(
                lambda batch_df, batch_id: self._write_batch(
                    batch_df=batch_df,
                    batch_id=batch_id,
                    table_name=self._settings.postgres_aggregated_table,
                )
            )
            .option("checkpointLocation", checkpoint)
            .queryName("postgres_aggregated_trades_writer")
            .start()
        )

    def _write_batch(self, batch_df: DataFrame, batch_id: int, table_name: str) -> None:
        if batch_df.isEmpty():
            self._logger.info("Skipping empty batch. table=%s batch_id=%s", table_name, batch_id)
            return

        self._logger.info("Writing batch to PostgreSQL. table=%s batch_id=%s", table_name, batch_id)
        (
            batch_df.write.mode("append")
            .jdbc(
                url=self._jdbc_url,
                table=table_name,
                properties=self._connection_properties,
            )
        )

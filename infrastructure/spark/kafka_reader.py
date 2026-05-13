from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from infrastructure.spark.schema import TRADE_EVENT_SCHEMA
from shared.config.settings import Settings


class KafkaTradeStreamReader:
    """Reads JSON payloads from Kafka and projects them into columns."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def read(self, spark: SparkSession) -> DataFrame:
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self._settings.kafka_broker)
            .option("subscribe", self._settings.kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", self._settings.spark_max_offsets_per_trigger)
            .load()
        )

        parsed_df = kafka_df.select(
            F.from_json(
                F.col("value").cast("string"),
                TRADE_EVENT_SCHEMA,
            ).alias("payload")
        ).select("payload.*")

        return parsed_df

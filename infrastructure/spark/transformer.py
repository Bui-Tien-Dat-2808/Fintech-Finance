from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TradeStreamTransformer:
    """Cleans and normalizes the raw Kafka stream."""

    @staticmethod
    def clean_and_deduplicate(df: DataFrame, watermark_delay: str) -> DataFrame:
        cleaned_df = (
            df.dropna(
                subset=[
                    "symbol",
                    "price",
                    "volume",
                    "trade_timestamp",
                    "ingestion_timestamp",
                ]
            )
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("volume", F.col("volume").cast("bigint"))
            .withColumn("trade_timestamp", F.to_timestamp("trade_timestamp"))
            .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
            .filter(F.col("price") > 0)
            .filter(F.col("volume") >= 0)
            .withColumn("trade_date", F.to_date("trade_timestamp"))
        )

        return cleaned_df.withWatermark(
            "trade_timestamp",
            watermark_delay,
        ).dropDuplicates(["symbol", "trade_timestamp"])

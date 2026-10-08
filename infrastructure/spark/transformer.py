from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TradeStreamTransformer:
    """Transforms raw Kafka streams into Silver cleaned events and anomaly streams."""

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

    @staticmethod
    def extract_anomalies(silver_df: DataFrame) -> DataFrame:
        """Flags high-impact trades (e.g. block trades with volume > 50,000) for real-time monitoring."""
        return (
            silver_df.filter(F.col("volume") >= 50000)
            .withColumn("anomaly_type", F.lit("LARGE_BLOCK_TRADE"))
            .withColumn(
                "description",
                F.concat(
                    F.lit("Large trade of "),
                    F.col("volume"),
                    F.lit(" shares detected for "),
                    F.col("symbol"),
                    F.lit(" at price $"),
                    F.col("price"),
                ),
            )
            .withColumn("severity", F.lit("INFO"))
            .select(
                "symbol",
                "trade_timestamp",
                "price",
                "volume",
                "anomaly_type",
                "description",
                "severity",
                "trade_date",
            )
        )

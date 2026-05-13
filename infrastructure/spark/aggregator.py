from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TradeAggregator:
    """Computes rolling window metrics for downstream analytics."""

    @staticmethod
    def aggregate_1m(df: DataFrame) -> DataFrame:
        aggregated_df = (
            df.groupBy(
                "symbol",
                F.window("trade_timestamp", "1 minute"),
            )
            .agg(
                F.avg("price").alias("avg_price"),
                F.min("price").alias("min_price"),
                F.max("price").alias("max_price"),
                F.sum("volume").alias("total_volume"),
                F.count("*").alias("trade_count"),
            )
            .select(
                F.col("symbol"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("avg_price"),
                F.col("min_price"),
                F.col("max_price"),
                F.col("total_volume"),
                F.col("trade_count"),
            )
            .withColumn("trade_date", F.to_date("window_start"))
        )
        return aggregated_df

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TradeAggregator:
    """Computes OHLCV candlestick bars and VWAP metrics for downstream analytics."""

    @staticmethod
    def aggregate_candles(df: DataFrame, window_duration: str = "1 minute") -> DataFrame:
        aggregated_df = (
            df.groupBy(
                "symbol",
                F.window("trade_timestamp", window_duration),
            )
            .agg(
                F.expr("min_by(price, trade_timestamp)").alias("open_price"),
                F.max("price").alias("high_price"),
                F.min("price").alias("low_price"),
                F.expr("max_by(price, trade_timestamp)").alias("close_price"),
                F.round(F.avg("price"), 4).alias("avg_price"),
                F.sum("volume").alias("total_volume"),
                F.count("*").alias("trade_count"),
                F.round(
                    F.when(
                        F.sum("volume") > 0,
                        F.sum(F.col("price") * F.col("volume")) / F.sum("volume"),
                    ).otherwise(F.avg("price")),
                    4,
                ).alias("vwap"),
            )
            .select(
                F.col("symbol"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("open_price"),
                F.col("high_price"),
                F.col("low_price"),
                F.col("close_price"),
                F.col("avg_price"),
                F.col("vwap"),
                F.col("total_volume"),
                F.col("trade_count"),
            )
            .withColumn("trade_date", F.to_date("window_start"))
        )
        return aggregated_df

    @classmethod
    def aggregate_1m(cls, df: DataFrame) -> DataFrame:
        return cls.aggregate_candles(df, "1 minute")

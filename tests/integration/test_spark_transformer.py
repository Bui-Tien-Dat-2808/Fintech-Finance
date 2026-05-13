from pyspark.sql import SparkSession

from infrastructure.spark.aggregator import TradeAggregator
from infrastructure.spark.transformer import TradeStreamTransformer


def test_clean_and_aggregate_stream() -> None:
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    rows = [
        (
            "AAPL",
            "189.42",
            "10",
            "2026-05-05T10:00:00+00:00",
            "2026-05-05T10:00:01+00:00",
            "finnhub",
        ),
        (
            "AAPL",
            "189.42",
            "10",
            "2026-05-05T10:00:00+00:00",
            "2026-05-05T10:00:01+00:00",
            "finnhub",
        ),
        (
            "AAPL",
            "-1",
            "10",
            "2026-05-05T10:00:02+00:00",
            "2026-05-05T10:00:03+00:00",
            "finnhub",
        ),
    ]
    columns = [
        "symbol",
        "price",
        "volume",
        "trade_timestamp",
        "ingestion_timestamp",
        "source",
    ]

    df = spark.createDataFrame(rows, schema=columns)
    clean_df = TradeStreamTransformer.clean_and_deduplicate(df, "2 minutes")
    result = clean_df.collect()

    assert len(result) == 1

    aggregated = TradeAggregator.aggregate_1m(clean_df, "2 minutes").collect()
    assert len(aggregated) == 1
    assert aggregated[0]["trade_count"] == 1

    spark.stop()

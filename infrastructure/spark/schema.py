from pyspark.sql.types import StringType, StructField, StructType


TRADE_EVENT_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("price", StringType(), nullable=False),
        StructField("volume", StringType(), nullable=False),
        StructField("trade_timestamp", StringType(), nullable=False),
        StructField("ingestion_timestamp", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
    ]
)

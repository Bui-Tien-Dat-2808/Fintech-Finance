from __future__ import annotations

from pyspark.sql import SparkSession

from shared.config.settings import Settings


class SparkSessionFactory:
    """Builds SparkSession configured for Kafka and Iceberg."""

    @staticmethod
    def create(settings: Settings) -> SparkSession:
        return (
            SparkSession.builder.appName(settings.spark_app_name)
            .master(settings.spark_master_url)
            .config(
                "spark.jars.packages",
                ",".join(
                    [
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                        "org.postgresql:postgresql:42.7.3",
                    ]
                ),
            )
            .config(
                f"spark.sql.catalog.{settings.iceberg_catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.{settings.iceberg_catalog_name}.type",
                "hive",
            )
            .config(
                f"spark.sql.catalog.{settings.iceberg_catalog_name}.uri",
                settings.hive_metastore_uri,
            )
            .config(
                f"spark.sql.catalog.{settings.iceberg_catalog_name}.warehouse",
                settings.iceberg_warehouse,
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
            .config("spark.streaming.backpressure.enabled", "true")
            .getOrCreate()
        )

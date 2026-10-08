from __future__ import annotations

from pyspark.sql import SparkSession

from shared.config.settings import Settings


class SparkSessionFactory:
    """Builds SparkSession configured for Kafka, MinIO S3 Object Storage, and Iceberg Lakehouse."""

    @staticmethod
    def create(settings: Settings) -> SparkSession:
        builder = (
            SparkSession.builder.appName(settings.spark_app_name)
            .master(settings.spark_master_url)
            .config(
                "spark.jars.packages",
                ",".join(
                    [
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                        "org.apache.hadoop:hadoop-aws:3.3.4",
                        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
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
            # S3A Configurations for MinIO / S3 Object Storage
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(f"spark.sql.catalog.{settings.iceberg_catalog_name}.s3.endpoint", settings.s3_endpoint)
            .config(f"spark.sql.catalog.{settings.iceberg_catalog_name}.s3.path-style-access", "true")
            # Runtime Tuning
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
            .config("spark.streaming.backpressure.enabled", "true")
        )

        return builder.getOrCreate()

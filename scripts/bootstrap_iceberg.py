from infrastructure.iceberg.table_manager import IcebergTableManager
from infrastructure.spark.session_factory import SparkSessionFactory
from shared.config.settings import Settings
from shared.logging.logger import configure_logging, get_logger


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings)
    logger = get_logger("bootstrap_iceberg")
    logger.info("Ensuring Iceberg namespace and tables exist.")
    spark = SparkSessionFactory.create(settings)
    manager = IcebergTableManager(settings=settings, spark=spark)
    manager.ensure_namespace()
    manager.ensure_tables()
    spark.stop()


if __name__ == "__main__":
    main()

from infrastructure.spark.streaming_job import StockStreamingJob
from shared.config.settings import Settings
from shared.logging.logger import configure_logging


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings)
    StockStreamingJob(settings).run()


if __name__ == "__main__":
    main()

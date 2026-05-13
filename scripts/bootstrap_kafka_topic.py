from shared.config.settings import Settings
from shared.logging.logger import configure_logging, get_logger
from infrastructure.kafka.topic_admin import KafkaTopicAdmin


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings)
    logger = get_logger("bootstrap_kafka_topic")
    logger.info("Ensuring Kafka topic exists.")
    KafkaTopicAdmin(settings).ensure_topic()


if __name__ == "__main__":
    main()

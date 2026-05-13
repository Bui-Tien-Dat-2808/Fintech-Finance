from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

from shared.config.settings import Settings
from shared.logging.logger import get_logger


class KafkaTopicAdmin:
    """Creates the Kafka topic needed by the pipeline if it is missing."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)
        self._client = AdminClient({"bootstrap.servers": settings.kafka_broker})

    def ensure_topic(self) -> None:
        metadata = self._client.list_topics(timeout=10)
        if self._settings.kafka_topic in metadata.topics:
            self._logger.info("Kafka topic already exists: %s", self._settings.kafka_topic)
            return

        new_topic = NewTopic(
            self._settings.kafka_topic,
            num_partitions=self._settings.kafka_topic_partitions,
            replication_factor=self._settings.kafka_topic_replication_factor,
        )
        futures = self._client.create_topics([new_topic])
        futures[self._settings.kafka_topic].result(timeout=30)
        self._logger.info("Kafka topic created: %s", self._settings.kafka_topic)

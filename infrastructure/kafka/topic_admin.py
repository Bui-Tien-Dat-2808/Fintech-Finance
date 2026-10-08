from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

from shared.config.settings import Settings
from shared.logging.logger import get_logger


class KafkaTopicAdmin:
    """Creates the Kafka topics (data topic and DLQ topic) needed by the pipeline."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)
        self._client = AdminClient({"bootstrap.servers": settings.kafka_broker})

    def ensure_topic(self) -> None:
        metadata = self._client.list_topics(timeout=10)
        existing_topics = set(metadata.topics.keys())

        topics_to_create: list[NewTopic] = []

        if self._settings.kafka_topic not in existing_topics:
            topics_to_create.append(
                NewTopic(
                    self._settings.kafka_topic,
                    num_partitions=self._settings.kafka_topic_partitions,
                    replication_factor=self._settings.kafka_topic_replication_factor,
                )
            )

        if self._settings.kafka_dlq_topic not in existing_topics:
            topics_to_create.append(
                NewTopic(
                    self._settings.kafka_dlq_topic,
                    num_partitions=max(1, self._settings.kafka_topic_partitions // 2),
                    replication_factor=self._settings.kafka_topic_replication_factor,
                )
            )

        if not topics_to_create:
            self._logger.info("All required Kafka topics already exist.")
            return

        futures = self._client.create_topics(topics_to_create)
        for topic, future in futures.items():
            future.result(timeout=30)
            self._logger.info("Kafka topic created: %s", topic)

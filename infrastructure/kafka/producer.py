from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer

from domain.entities.trade_event import TradeEvent
from shared.config.settings import Settings
from shared.logging.logger import get_logger


class KafkaTradeProducer:
    """Publishes normalized trade events to Kafka."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)
        config: dict[str, Any] = {
            "bootstrap.servers": settings.kafka_broker,
            "client.id": settings.kafka_client_id,
            "acks": "all",
            "retries": settings.kafka_retries,
            "enable.idempotence": True,
            "linger.ms": settings.kafka_linger_ms,
            "compression.type": "snappy",
        }
        self._producer = Producer(config)

    def publish(self, trade: TradeEvent) -> None:
        message_key = trade.symbol.encode("utf-8")
        message_value = json.dumps(trade.to_dict()).encode("utf-8")
        self._producer.produce(
            self._settings.kafka_topic,
            key=message_key,
            value=message_value,
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def close(self) -> None:
        self._logger.info("Flushing Kafka producer.")
        self._producer.flush(self._settings.kafka_flush_timeout_seconds)

    def _delivery_callback(self, error: Exception | None, message: Any) -> None:
        if error is not None:
            self._logger.error("Kafka delivery failed: %s", error)
            return

        self._logger.info(
            "Kafka message delivered. topic=%s partition=%s offset=%s key=%s",
            message.topic(),
            message.partition(),
            message.offset(),
            message.key().decode("utf-8"),
        )

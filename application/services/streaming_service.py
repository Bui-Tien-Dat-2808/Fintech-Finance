from __future__ import annotations

from datetime import datetime, timezone

from confluent_kafka.admin import AdminClient

from domain.entities.dead_letter import DeadLetterEvent
from domain.entities.trade_event import TradeEvent
from domain.entities.validation_error import TradeValidationError
from domain.use_cases.ingest_trade import IngestTradeUseCase
from infrastructure.finnhub.websocket_client import FinnhubWebSocketClient
from infrastructure.kafka.producer import KafkaTradeProducer
from shared.config.settings import Settings
from shared.logging.logger import get_logger


class StreamingService:
    """Runs the producer-side streaming pipeline with Dead Letter Queue protection."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)
        self._producer = KafkaTradeProducer(settings=settings)
        self._ingest_trade = IngestTradeUseCase()
        self._websocket_client = FinnhubWebSocketClient(
            settings=settings,
            trade_handler=self._handle_trade,
            error_handler=self._handle_error,
        )

    def _handle_trade(self, trade: TradeEvent) -> None:
        try:
            normalized_trade = self._ingest_trade.execute(trade)
            self._producer.publish(normalized_trade)
        except TradeValidationError as err:
            self._logger.warning("Trade rejected by domain validation: %s. Routing to DLQ.", err)
            dlq_event = DeadLetterEvent(
                raw_payload=str(trade.to_dict()),
                error_reason=f"VALIDATION_FAILED: {err}",
                timestamp=datetime.now(tz=timezone.utc),
            )
            self._producer.publish_dead_letter(dlq_event)

    def _handle_error(self, raw_payload: str, error_reason: str) -> None:
        self._logger.warning("Routing unparseable message to DLQ: %s", error_reason)
        dlq_event = DeadLetterEvent(
            raw_payload=raw_payload,
            error_reason=error_reason,
            timestamp=datetime.now(tz=timezone.utc),
        )
        self._producer.publish_dead_letter(dlq_event)

    def run(self) -> None:
        self._logger.info(
            "Starting streaming service for symbols=%s with DLQ topic=%s",
            self._settings.stock_symbols,
            self._settings.kafka_dlq_topic,
        )
        self._websocket_client.run_forever()

    def close(self) -> None:
        self._logger.info("Closing streaming service.")
        self._producer.close()
        self._websocket_client.close()

    def check_kafka_connectivity(self) -> bool:
        admin_client = AdminClient({"bootstrap.servers": self._settings.kafka_broker})
        metadata = admin_client.list_topics(timeout=10)
        return bool(metadata.brokers)

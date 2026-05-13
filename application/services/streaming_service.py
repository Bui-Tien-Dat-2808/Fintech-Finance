from __future__ import annotations

from confluent_kafka.admin import AdminClient

from domain.use_cases.ingest_trade import IngestTradeUseCase
from infrastructure.finnhub.websocket_client import FinnhubWebSocketClient
from infrastructure.kafka.producer import KafkaTradeProducer
from shared.config.settings import Settings
from shared.logging.logger import get_logger


class StreamingService:
    """Runs the producer-side streaming pipeline."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = get_logger(self.__class__.__name__)
        self._producer = KafkaTradeProducer(settings=settings)
        self._ingest_trade = IngestTradeUseCase()
        self._websocket_client = FinnhubWebSocketClient(
            settings=settings,
            trade_handler=self._handle_trade,
        )

    def _handle_trade(self, trade) -> None:
        normalized_trade = self._ingest_trade.execute(trade)
        self._producer.publish(normalized_trade)

    def run(self) -> None:
        self._logger.info(
            "Starting streaming service for symbols=%s",
            self._settings.stock_symbols,
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

from __future__ import annotations

import json
import time
from collections.abc import Callable

from websocket import WebSocketApp

from domain.entities.trade_event import TradeEvent
from infrastructure.finnhub.message_parser import FinnhubMessageParser
from shared.config.settings import Settings
from shared.logging.logger import get_logger


class FinnhubWebSocketClient:
    """Handles WebSocket lifecycle and automatic resubscription."""

    def __init__(
        self,
        settings: Settings,
        trade_handler: Callable[[TradeEvent], None],
    ) -> None:
        self._settings = settings
        self._trade_handler = trade_handler
        self._logger = get_logger(self.__class__.__name__)
        self._should_run = True
        self._websocket_app: WebSocketApp | None = None
        self._url = (
            f"{settings.finnhub_websocket_url}?token={settings.finnhub_api_key}"
        )

    def connect(self) -> None:
        self._logger.info("Creating Finnhub WebSocket client.")
        self._websocket_app = WebSocketApp(
            self._url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_ping=self._on_ping,
            on_pong=self._on_pong,
        )

    def subscribe(self, symbols: list[str]) -> None:
        if self._websocket_app is None or self._websocket_app.sock is None:
            self._logger.warning("Subscription skipped because socket is not ready.")
            return

        for symbol in symbols:
            payload = {"type": "subscribe", "symbol": symbol}
            self._websocket_app.send(json.dumps(payload))
            self._logger.info("Subscribed to symbol=%s", symbol)

    def listen(self) -> None:
        if self._websocket_app is None:
            self.connect()

        assert self._websocket_app is not None
        self._logger.info("Listening to Finnhub stream.")
        self._websocket_app.run_forever(
            ping_interval=self._settings.websocket_ping_interval_seconds,
            ping_timeout=self._settings.websocket_ping_timeout_seconds,
            reconnect=self._settings.websocket_reconnect_delay_seconds,
        )

    def run_forever(self) -> None:
        while self._should_run:
            try:
                self.connect()
                self.listen()
            except KeyboardInterrupt:
                self._logger.info("Streaming service interrupted by user.")
                self._should_run = False
            except Exception as exc:  # pragma: no cover - defensive reconnect path
                self._logger.exception("Unexpected WebSocket failure: %s", exc)

            if self._should_run:
                time.sleep(self._settings.websocket_reconnect_delay_seconds)

    def close(self) -> None:
        self._should_run = False
        if self._websocket_app is not None:
            self._websocket_app.close()

    def _on_open(self, ws: WebSocketApp) -> None:
        self._logger.info("Finnhub WebSocket connection established.")
        self.subscribe(self._settings.stock_symbols)

    def _on_message(self, ws: WebSocketApp, message: str) -> None:
        parsed = json.loads(message)
        trades = FinnhubMessageParser.parse(parsed)
        if not trades and parsed.get("type") == "trade":
            self._logger.warning("Dropped malformed trade payload: %s", parsed)

        for trade in trades:
            self._trade_handler(trade)

    def _on_error(self, ws: WebSocketApp, error: Exception) -> None:
        self._logger.exception("Finnhub WebSocket error: %s", error)

    def _on_close(
        self,
        ws: WebSocketApp,
        close_status_code: int | None,
        close_message: str | None,
    ) -> None:
        self._logger.warning(
            "Finnhub WebSocket closed. code=%s message=%s",
            close_status_code,
            close_message,
        )

    def _on_ping(self, ws: WebSocketApp, message: str) -> None:
        self._logger.debug("Received WebSocket ping: %s", message)

    def _on_pong(self, ws: WebSocketApp, message: str) -> None:
        self._logger.debug("Received WebSocket pong: %s", message)

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from domain.entities.trade_event import TradeEvent


class FinnhubMessageParser:
    """Parses Finnhub WebSocket payloads into normalized trade events."""

    @staticmethod
    def parse(message: dict[str, Any]) -> list[TradeEvent]:
        if message.get("type") != "trade":
            return []

        events: list[TradeEvent] = []
        for item in message.get("data", []):
            symbol = item.get("s")
            price = item.get("p")
            volume = item.get("v")
            timestamp = item.get("t")

            if symbol is None or price is None or volume is None or timestamp is None:
                continue

            trade_timestamp = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            events.append(
                TradeEvent(
                    symbol=str(symbol),
                    price=Decimal(str(price)),
                    volume=int(volume),
                    trade_timestamp=trade_timestamp,
                    ingestion_timestamp=datetime.now(tz=timezone.utc),
                    source="finnhub",
                )
            )
        return events

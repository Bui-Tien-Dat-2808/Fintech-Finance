from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class TradeEvent:
    """Represents one normalized stock trade event."""

    symbol: str
    price: Decimal
    volume: int
    trade_timestamp: datetime
    ingestion_timestamp: datetime
    source: str = "finnhub"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["price"] = str(self.price)
        payload["trade_timestamp"] = self.trade_timestamp.astimezone(
            timezone.utc
        ).isoformat()
        payload["ingestion_timestamp"] = self.ingestion_timestamp.astimezone(
            timezone.utc
        ).isoformat()
        return payload

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class AnomalyEvent:
    """Represents a flagged financial anomaly (e.g. price jump, volume spike)."""

    symbol: str
    trade_timestamp: datetime
    price: Decimal
    volume: int
    anomaly_type: str
    description: str
    severity: str = "WARNING"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["price"] = str(self.price)
        payload["trade_timestamp"] = self.trade_timestamp.astimezone(timezone.utc).isoformat()
        return payload


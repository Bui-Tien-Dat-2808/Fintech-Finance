from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class StockCandle:
    """Represents an OHLCV candlestick bar with VWAP metric."""

    symbol: str
    window_start: datetime
    window_end: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    total_volume: int
    vwap: Decimal
    trade_count: int

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["open_price"] = str(self.open_price)
        payload["high_price"] = str(self.high_price)
        payload["low_price"] = str(self.low_price)
        payload["close_price"] = str(self.close_price)
        payload["vwap"] = str(self.vwap)
        payload["window_start"] = self.window_start.astimezone(timezone.utc).isoformat()
        payload["window_end"] = self.window_end.astimezone(timezone.utc).isoformat()
        return payload


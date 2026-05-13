from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(slots=True)
class AggregatedMetric:
    """Represents one aggregated stock metric row."""

    symbol: str
    window_start: datetime
    window_end: datetime
    avg_price: Decimal
    min_price: Decimal
    max_price: Decimal
    total_volume: int
    trade_count: int

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from domain.entities.anomaly import AnomalyEvent
from domain.entities.trade_event import TradeEvent


class DetectAnomalyUseCase:
    """Detects simple real-time anomalies for trade events."""

    @staticmethod
    def inspect(trade: TradeEvent, prev_price: Decimal | None = None) -> AnomalyEvent | None:
        if trade.volume > 100_000:
            return AnomalyEvent(
                symbol=trade.symbol,
                trade_timestamp=trade.trade_timestamp,
                price=trade.price,
                volume=trade.volume,
                anomaly_type="LARGE_BLOCK_TRADE",
                description=f"Unusual high trade volume detected: {trade.volume}",
                severity="INFO",
            )

        if prev_price and prev_price > 0:
            pct_change = abs((trade.price - prev_price) / prev_price)
            if pct_change >= Decimal("0.05"):
                return AnomalyEvent(
                    symbol=trade.symbol,
                    trade_timestamp=trade.trade_timestamp,
                    price=trade.price,
                    volume=trade.volume,
                    anomaly_type="EXTREME_PRICE_MOVEMENT",
                    description=f"Price moved by {float(pct_change)*100:.2f}% relative to last quote",
                    severity="HIGH",
                )

        return None


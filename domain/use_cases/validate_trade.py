from __future__ import annotations

from domain.entities.trade_event import TradeEvent
from domain.entities.validation_error import TradeValidationError


class ValidateTradeUseCase:
    """Validates normalized trade events before publishing."""

    @staticmethod
    def execute(trade: TradeEvent) -> TradeEvent:
        if not trade.symbol:
            raise TradeValidationError("Trade symbol must not be empty.")
        if trade.price <= 0:
            raise TradeValidationError("Trade price must be greater than zero.")
        if trade.volume < 0:
            raise TradeValidationError("Trade volume must not be negative.")
        return trade

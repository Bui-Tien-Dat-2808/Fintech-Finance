from __future__ import annotations

from domain.entities.trade_event import TradeEvent
from domain.use_cases.validate_trade import ValidateTradeUseCase


class IngestTradeUseCase:
    """Coordinates validation before a trade is sent downstream."""

    def __init__(self, validator: ValidateTradeUseCase | None = None) -> None:
        self._validator = validator or ValidateTradeUseCase()

    def execute(self, trade: TradeEvent) -> TradeEvent:
        return self._validator.execute(trade)

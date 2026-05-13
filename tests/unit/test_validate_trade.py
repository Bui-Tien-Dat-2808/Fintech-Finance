from datetime import datetime, timezone
from decimal import Decimal

import pytest

from domain.entities.trade_event import TradeEvent
from domain.entities.validation_error import TradeValidationError
from domain.use_cases.validate_trade import ValidateTradeUseCase


def test_validate_trade_accepts_valid_trade() -> None:
    trade = TradeEvent(
        symbol="AAPL",
        price=Decimal("189.42"),
        volume=100,
        trade_timestamp=datetime.now(tz=timezone.utc),
        ingestion_timestamp=datetime.now(tz=timezone.utc),
    )

    validated = ValidateTradeUseCase.execute(trade)

    assert validated == trade


def test_validate_trade_rejects_non_positive_price() -> None:
    trade = TradeEvent(
        symbol="AAPL",
        price=Decimal("0"),
        volume=100,
        trade_timestamp=datetime.now(tz=timezone.utc),
        ingestion_timestamp=datetime.now(tz=timezone.utc),
    )

    with pytest.raises(TradeValidationError):
        ValidateTradeUseCase.execute(trade)

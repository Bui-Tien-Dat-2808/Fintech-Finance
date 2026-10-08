from datetime import datetime, timezone
from decimal import Decimal

from domain.entities.trade_event import TradeEvent
from domain.use_cases.detect_anomaly import DetectAnomalyUseCase


def test_detect_large_block_trade() -> None:
    trade = TradeEvent(
        symbol="AAPL",
        price=Decimal("190.50"),
        volume=150_000,
        trade_timestamp=datetime.now(tz=timezone.utc),
        ingestion_timestamp=datetime.now(tz=timezone.utc),
    )

    anomaly = DetectAnomalyUseCase.inspect(trade)
    assert anomaly is not None
    assert anomaly.anomaly_type == "LARGE_BLOCK_TRADE"
    assert anomaly.severity == "INFO"


def test_detect_extreme_price_movement() -> None:
    trade = TradeEvent(
        symbol="AAPL",
        price=Decimal("210.00"),
        volume=100,
        trade_timestamp=datetime.now(tz=timezone.utc),
        ingestion_timestamp=datetime.now(tz=timezone.utc),
    )

    # Previous price was 190.00 -> change is (210-190)/190 = 10.5% > 5%
    anomaly = DetectAnomalyUseCase.inspect(trade, prev_price=Decimal("190.00"))
    assert anomaly is not None
    assert anomaly.anomaly_type == "EXTREME_PRICE_MOVEMENT"
    assert anomaly.severity == "HIGH"


def test_normal_trade_has_no_anomaly() -> None:
    trade = TradeEvent(
        symbol="AAPL",
        price=Decimal("190.25"),
        volume=100,
        trade_timestamp=datetime.now(tz=timezone.utc),
        ingestion_timestamp=datetime.now(tz=timezone.utc),
    )

    anomaly = DetectAnomalyUseCase.inspect(trade, prev_price=Decimal("190.00"))
    assert anomaly is None


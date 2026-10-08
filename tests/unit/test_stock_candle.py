from datetime import datetime, timezone
from decimal import Decimal

from domain.entities.stock_candle import StockCandle


def test_stock_candle_serialization() -> None:
    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    candle = StockCandle(
        symbol="AAPL",
        window_start=now,
        window_end=now,
        open_price=Decimal("150.10"),
        high_price=Decimal("152.00"),
        low_price=Decimal("149.80"),
        close_price=Decimal("151.50"),
        total_volume=50000,
        vwap=Decimal("151.25"),
        trade_count=120,
    )

    data = candle.to_dict()
    assert data["symbol"] == "AAPL"
    assert data["open_price"] == "150.10"
    assert data["high_price"] == "152.00"
    assert data["low_price"] == "149.80"
    assert data["close_price"] == "151.50"
    assert data["vwap"] == "151.25"
    assert data["total_volume"] == 50000
    assert data["trade_count"] == 120


from shared.config.settings import Settings


def test_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
    monkeypatch.setenv("STOCK_SYMBOLS", "AAPL,MSFT")

    settings = Settings.from_env()

    assert settings.finnhub_api_key == "test-key"
    assert settings.stock_symbols == ["AAPL", "MSFT"]
    assert settings.kafka_topic == "stock_trades"

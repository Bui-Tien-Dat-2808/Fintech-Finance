from infrastructure.finnhub.message_parser import FinnhubMessageParser


def test_finnhub_message_parser_parses_trade_events() -> None:
    payload = {
        "type": "trade",
        "data": [{"s": "AAPL", "p": 189.42, "v": 10, "t": 1715000000000}],
    }

    events = FinnhubMessageParser.parse(payload)

    assert len(events) == 1
    assert events[0].symbol == "AAPL"
    assert str(events[0].price) == "189.42"
    assert events[0].volume == 10

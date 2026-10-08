from datetime import datetime, timezone

from domain.entities.dead_letter import DeadLetterEvent


def test_dead_letter_serialization() -> None:
    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    dlq = DeadLetterEvent(
        raw_payload='{"bad": "data"}',
        error_reason="MALFORMED_SCHEMA",
        timestamp=now,
        source="finnhub",
    )

    data = dlq.to_dict()
    assert data["raw_payload"] == '{"bad": "data"}'
    assert data["error_reason"] == "MALFORMED_SCHEMA"
    assert data["source"] == "finnhub"
    assert "2026-10-08" in data["timestamp"]


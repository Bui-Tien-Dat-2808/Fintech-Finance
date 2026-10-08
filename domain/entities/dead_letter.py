from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class DeadLetterEvent:
    """Represents a malformed or rejected event routed to the Dead Letter Queue."""

    raw_payload: str
    error_reason: str
    timestamp: datetime
    source: str = "finnhub"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["timestamp"] = self.timestamp.astimezone(timezone.utc).isoformat()
        return payload


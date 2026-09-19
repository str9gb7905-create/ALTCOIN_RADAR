"""In-process request and Stage B.1 per-asset timing telemetry."""

from __future__ import annotations

from threading import Lock
from typing import Any


_lock = Lock()
_api_requests_total = 0
_requests_by_exchange: dict[str, int] = {}
_asset_enrichment_seconds: dict[str, float] = {}


def reset() -> None:
    global _api_requests_total
    with _lock:
        _api_requests_total = 0
        _requests_by_exchange.clear()
        _asset_enrichment_seconds.clear()


def record_api_request(source: str) -> None:
    global _api_requests_total
    with _lock:
        _api_requests_total += 1
        _requests_by_exchange[source] = _requests_by_exchange.get(source, 0) + 1


def record_asset_enrichment(symbol: str, elapsed_seconds: float) -> None:
    with _lock:
        _asset_enrichment_seconds[symbol] = round(elapsed_seconds, 6)


def snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "API_requests_total": _api_requests_total,
            "requests_by_exchange": dict(sorted(_requests_by_exchange.items())),
            "asset_enrichment_seconds": dict(sorted(_asset_enrichment_seconds.items())),
        }

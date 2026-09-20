"""Build one deterministic notification payload from the three event streams."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from radar_scan import OUTPUT_DIR
from state_store import FileSystemStateStore

PRICE_EVENTS = {"NEW_TRIGGER", "ESCALATION", "DIRECTION_CHANGE", "REENTRY"}
DIVERGENCE_EVENTS = {"DIVERGENCE_NEW", "DIVERGENCE_ESCALATION"}
TELEGRAM_MESSAGE_LIMIT = 4096
DEFAULT_PATHS = {
    "price_events": OUTPUT_DIR / "latest_notification_candidates.json",
    "technical_events": OUTPUT_DIR / "latest_technical_events.json",
    "divergence_events": OUTPUT_DIR / "latest_divergence_events.json",
    "snapshot": OUTPUT_DIR / "latest_snapshot.json",
    "technical": OUTPUT_DIR / "latest_stage_b.json",
    "divergence": OUTPUT_DIR / "latest_divergence.json",
    "merged": OUTPUT_DIR / "latest_merged_notification.json",
}


class NotificationMergeError(RuntimeError):
    """Raised when notification inputs cannot be merged safely."""


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise NotificationMergeError(f"invalid JSON object: {path.name}")
    return payload


def _run_id(payload: dict[str, Any], label: str) -> str:
    value = payload.get("run_id")
    if not isinstance(value, str) or not value:
        raise NotificationMergeError(f"missing run_id: {label}")
    return value


def _indexed_assets(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for asset in payload.get("assets", []):
        if isinstance(asset, dict) and isinstance(asset.get("symbol"), str):
            result[asset["symbol"].upper()] = asset
    return result


def _event_key(event: dict[str, Any]) -> tuple[str, str, str]:
    detail = {key: value for key, value in event.items() if key not in {"symbol", "notify"}}
    return (
        str(event.get("symbol", "")).upper(),
        str(event.get("event", "")),
        json.dumps(detail, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
    )


def _selected_events(price_payload, technical_payload, divergence_payload):
    price = [item for item in price_payload.get("assets", [])
             if isinstance(item, dict) and item.get("event") in PRICE_EVENTS]
    technical = [item for item in technical_payload.get("notification_events", [])
                 if isinstance(item, dict) and item.get("notify") is True]
    divergence = [item for item in divergence_payload.get("events", [])
                  if isinstance(item, dict) and item.get("event") in DIVERGENCE_EVENTS]
    return tuple(sorted(items, key=_event_key) for items in (price, technical, divergence))


def _divergence_summary(asset: dict[str, Any] | None) -> str:
    if not asset:
        return "NONE"
    found = []
    for timeframe in ("daily", "weekly"):
        data = asset.get(timeframe, {})
        if isinstance(data, dict):
            for direction in ("bullish", "bearish"):
                if data.get(direction):
                    found.append(f"{timeframe.upper()}_{direction.upper()}")
    return ", ".join(found) if found else "NONE"


def _volume_interpretation(asset: dict[str, Any] | None) -> str:
    if not asset:
        return "UNAVAILABLE"
    values = [asset.get("volume_status"), asset.get("volume_pattern"), asset.get("volume_note")]
    useful = [str(value) for value in values if value not in (None, "", "NONE")]
    return " / ".join(useful) if useful else "NORMAL"


def _format_value(value: Any, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}" if isinstance(value, (int, float)) else str(value)


def _format_price(value: Any) -> str:
    return f"{value:.8g}" if isinstance(value, (int, float)) else "N/A"


def _render_message(run_id: str, items: list[dict[str, Any]]) -> str:
    lines = [f"ALTCOIN RADAR | {run_id}"]
    for item in items:
        lines.extend([
            "", f"[{item['symbol']}]",
            f"Price events: {', '.join(item['price_events']) or 'NONE'}",
            f"Technical events: {', '.join(item['technical_events']) or 'NONE'}",
            f"Divergence events: {', '.join(item['divergence_events']) or 'NONE'}",
            f"Price: {_format_price(item['price'])}",
            f"1h: {_format_value(item['change_1h'])}%",
            f"24h: {_format_value(item['change_24h'])}%",
            f"4H RSI: {_format_value(item['rsi_4h'])}",
            f"Daily RSI: {_format_value(item['rsi_1d'])}",
            f"Weekly RSI: {_format_value(item['rsi_1w'])}",
            f"Divergence: {item['divergence']}",
            f"24h Volume Ratio: {_format_value(item['volume_ratio'])}",
            f"Volume: {item['volume_interpretation']}",
            f"Live Source: {item['live_source']}",
            f"History Source: {item['history_source']}",
        ])
    return "\n".join(lines)


def merge_payloads(price_payload, technical_events_payload, divergence_events_payload,
                   snapshot_payload, technical_payload, divergence_payload) -> dict[str, Any]:
    """Return a deterministic single-batch payload, or NO_NOTIFICATION."""
    payloads = {
        "price_events": price_payload, "technical_events": technical_events_payload,
        "divergence_events": divergence_events_payload, "snapshot": snapshot_payload,
        "technical": technical_payload, "divergence": divergence_payload,
    }
    run_ids = {_run_id(payload, label) for label, payload in payloads.items()}
    if len(run_ids) != 1:
        raise NotificationMergeError(f"run_id mismatch: {sorted(run_ids)}")
    run_id = next(iter(run_ids))
    price_events, technical_events, divergence_events = _selected_events(
        price_payload, technical_events_payload, divergence_events_payload
    )
    all_events = price_events + technical_events + divergence_events
    if not all_events:
        return {"schema_version": 1, "run_id": run_id, "status": "NO_NOTIFICATION",
                "notification_id": None, "event_count": 0, "items": [], "message": None}

    grouped = defaultdict(lambda: {"price": [], "technical": [], "divergence": []})
    for category, events in (("price", price_events), ("technical", technical_events),
                             ("divergence", divergence_events)):
        for event in events:
            symbol = str(event.get("symbol", "")).upper()
            if not symbol:
                raise NotificationMergeError("notification event is missing symbol")
            grouped[symbol][category].append(event)

    snapshot_assets = _indexed_assets(snapshot_payload)
    technical_assets = _indexed_assets(technical_payload)
    divergence_assets = _indexed_assets(divergence_payload)
    items = []
    for symbol in sorted(grouped):
        market = snapshot_assets.get(symbol, {})
        technical = technical_assets.get(symbol)
        events = grouped[symbol]
        source = technical or market
        items.append({
            "symbol": symbol,
            "price_events": [str(item["event"]) for item in events["price"]],
            "technical_events": [str(item["event"]) for item in events["technical"]],
            "divergence_events": [str(item["event"]) for item in events["divergence"]],
            "price": source.get("price", market.get("current_price")),
            "change_1h": source.get("change_1h", market.get("price_change_percentage_1h")),
            "change_24h": source.get("change_24h", market.get("price_change_percentage_24h")),
            "rsi_4h": technical.get("rsi_4h") if technical else None,
            "rsi_1d": technical.get("rsi_1d") if technical else None,
            "rsi_1w": technical.get("rsi_1w") if technical else None,
            "divergence": _divergence_summary(divergence_assets.get(symbol)),
            "volume_ratio": technical.get("volume_ratio") if technical else None,
            "volume_interpretation": _volume_interpretation(technical),
            "live_source": technical.get("live_source", "UNAVAILABLE") if technical else "UNAVAILABLE",
            "history_source": technical.get("history_source", "UNAVAILABLE") if technical else "UNAVAILABLE",
        })

    identity = {"run_id": run_id, "price": [_event_key(item) for item in price_events],
                "technical": [_event_key(item) for item in technical_events],
                "divergence": [_event_key(item) for item in divergence_events]}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"),
                                       ensure_ascii=False).encode("utf-8")).hexdigest()
    message = _render_message(run_id, items)
    if len(message) > TELEGRAM_MESSAGE_LIMIT:
        raise NotificationMergeError(
            f"MERGED_MESSAGE_TOO_LONG length={len(message)} limit={TELEGRAM_MESSAGE_LIMIT}"
        )
    return {"schema_version": 1, "run_id": run_id, "status": "READY",
            "notification_id": f"ntf_{digest[:32]}", "event_count": len(all_events),
            "items": items, "message": message}


def merge_from_files(paths: dict[str, Path] | None = None) -> dict[str, Any]:
    selected = DEFAULT_PATHS if paths is None else paths
    result = merge_payloads(
        _load_json(selected["price_events"]), _load_json(selected["technical_events"]),
        _load_json(selected["divergence_events"]), _load_json(selected["snapshot"]),
        _load_json(selected["technical"]), _load_json(selected["divergence"]),
    )
    FileSystemStateStore().save_atomic(selected["merged"], result)
    return result


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    result = merge_from_files()
    print(f"NOTIFICATION_MERGE_{result['status']} events={result['event_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

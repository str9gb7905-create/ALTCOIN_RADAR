"""Build one deterministic notification payload from the three event streams."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import quote

from radar_scan import OUTPUT_DIR, PROJECT_ROOT, THRESHOLD_PERCENT
from state_store import FileSystemStateStore

PRICE_EVENTS = {"NEW_TRIGGER", "ESCALATION", "DIRECTION_CHANGE", "REENTRY"}
DIVERGENCE_EVENTS = {"DIVERGENCE_NEW", "DIVERGENCE_ESCALATION"}
TELEGRAM_MESSAGE_LIMIT = 4096
POLICY_PATH = PROJECT_ROOT / "config" / "notification_policy.json"
DEFAULT_PATHS = {
    "price_events": OUTPUT_DIR / "latest_notification_candidates.json",
    "technical_events": OUTPUT_DIR / "latest_technical_events.json",
    "divergence_events": OUTPUT_DIR / "latest_divergence_events.json",
    "snapshot": OUTPUT_DIR / "latest_snapshot.json",
    "technical": OUTPUT_DIR / "latest_stage_b.json",
    "divergence": OUTPUT_DIR / "latest_divergence.json",
    "merged": OUTPUT_DIR / "latest_merged_notification.json",
}
EVENT_LABELS = {
    "NEW_TRIGGER": "首次觸發",
    "ESCALATION": "層級升高",
    "DIRECTION_CHANGE": "方向改變",
    "REENTRY": "再次觸發",
}
EXCHANGE_PREFIXES = {
    "Binance": "BINANCE",
    "Coinbase": "COINBASE",
    "OKX": "OKX",
    "Bybit": "BYBIT",
    "Bitget": "BITGET",
    "Gate": "GATEIO",
    "KuCoin": "KUCOIN",
    "MEXC": "MEXC",
    "Kraken": "KRAKEN",
}


class NotificationMergeError(RuntimeError):
    """Raised when notification inputs cannot be merged safely."""


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise NotificationMergeError(f"invalid JSON object: {path.name}")
    return payload


def _load_policy(path: Path = POLICY_PATH) -> dict[str, Any]:
    policy = _load_json(path)
    if policy.get("schema_version") != 1:
        raise NotificationMergeError("unsupported notification policy schema")
    if policy.get("price_alerts_enabled") is not True:
        raise NotificationMergeError("production price alerts are not enabled")
    if policy.get("price_threshold_percent") != THRESHOLD_PERCENT:
        raise NotificationMergeError("notification and scanner price thresholds do not match")
    if policy.get("standalone_technical_alerts_enabled") is not False:
        raise NotificationMergeError("standalone technical alerts must remain disabled")
    if policy.get("standalone_divergence_alerts_enabled") is not False:
        raise NotificationMergeError("standalone divergence alerts must remain disabled")
    exclusions = policy.get("immediate_price_excluded_symbols")
    if not isinstance(exclusions, list) or not all(
        isinstance(item, str) and item.strip() for item in exclusions
    ):
        raise NotificationMergeError("invalid immediate price exclusion list")
    if len(exclusions) != len({item.upper() for item in exclusions}):
        raise NotificationMergeError("duplicate immediate price exclusion symbol")
    return policy


def _run_id(payload: dict[str, Any], label: str) -> str:
    value = payload.get("run_id")
    if not isinstance(value, str) or not value:
        raise NotificationMergeError(f"missing run_id: {label}")
    return value


def _indexed_assets(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    assets = payload.get("assets")
    if not isinstance(assets, list):
        assets = payload.get("items", [])
    for asset in assets:
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


def _selected_events(price_payload, technical_payload, divergence_payload, policy):
    exclusions = {
        item.upper() for item in policy.get("immediate_price_excluded_symbols", [])
    }
    price = [item for item in price_payload.get("assets", [])
             if isinstance(item, dict) and item.get("event") in PRICE_EVENTS
             and isinstance(item.get("change_1h"), (int, float))
             and not isinstance(item.get("change_1h"), bool)
             and abs(item["change_1h"]) >= THRESHOLD_PERCENT
             and any(condition in {"1H_UP", "1H_DOWN"}
                     for condition in item.get("active_conditions", []))
             and str(item.get("symbol", "")).upper() not in exclusions]
    price_symbols = {str(item.get("symbol", "")).upper() for item in price}
    technical = [item for item in technical_payload.get("notification_events", [])
                 if isinstance(item, dict) and item.get("notify") is True
                 and str(item.get("symbol", "")).upper() in price_symbols]
    divergence = [item for item in divergence_payload.get("events", [])
                  if isinstance(item, dict) and item.get("event") in DIVERGENCE_EVENTS
                  and str(item.get("symbol", "")).upper() in price_symbols]
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


def _format_percent(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "N/A"
    return f"{value:+.2f}%"


def _tradingview_url(asset: dict[str, Any] | None, symbol: str) -> str:
    asset = asset or {}
    preferred = asset.get("history_source") or asset.get("live_source")
    markets = asset.get("cross_exchange_prices", [])
    selected = next(
        (
            market
            for market in markets
            if isinstance(market, dict) and market.get("exchange") == preferred
        ),
        None,
    )
    if selected is None:
        selected = next((market for market in markets if isinstance(market, dict)), None)
    exchange = (selected or {}).get("exchange") or preferred
    pair = (selected or {}).get("pair")
    prefix = EXCHANGE_PREFIXES.get(str(exchange))
    if not prefix or not isinstance(pair, str) or not pair:
        return f"https://www.tradingview.com/search/?query={quote(symbol)}"
    normalized_pair = pair.replace("-", "").replace("_", "").replace("/", "")
    return "https://www.tradingview.com/chart/?symbol=" + quote(
        f"{prefix}:{normalized_pair}", safe=""
    )


def _item_lines(item: dict[str, Any]) -> list[str]:
    event_labels = [EVENT_LABELS.get(event, event) for event in item["price_events"]]
    conditions = ", ".join(item["active_conditions"]) or "N/A"
    return [
        "",
        f"{item['symbol']}｜{'／'.join(event_labels)}",
        f"觸發：{conditions}",
        f"價格：US${_format_price(item['price'])}",
        f"1H：{_format_percent(item['change_1h'])}",
        f"24H：{_format_percent(item['change_24h'])}",
        f"層級：{item['severity_tier']}",
        f"4H RSI：{_format_value(item['rsi_4h'])}",
        f"日線 RSI：{_format_value(item['rsi_1d'])}",
        f"週線 RSI：{_format_value(item['rsi_1w'])}",
        f"成交量：{_format_value(item['volume_ratio'])}×｜{item['volume_interpretation']}",
        f"背離：{item['divergence']}",
        f"現貨來源：{item['live_source']}｜歷史來源：{item['history_source']}",
        "CoinGecko：",
        item["coingecko_url"],
        "TradingView：",
        item["tradingview_url"],
    ]


def _message_header(run_id: str) -> list[str]:
    return [
        "🚨 ALTCOIN RADAR｜即時價格異動",
        f"執行：{run_id}",
        "資料來源：CoinGecko＋公開現貨交易所",
    ]


def _render_message(run_id: str, items: list[dict[str, Any]]) -> str:
    lines = _message_header(run_id)
    for item in items:
        lines.extend(_item_lines(item))
    return "\n".join(lines)


def _render_overflow_message(run_id: str, items: list[dict[str, Any]]) -> str:
    """Keep every triggered symbol visible when a broad move exceeds Telegram's limit."""
    detailed_count = min(3, len(items))
    lines = _message_header(run_id)
    lines.append("同批異動過多；以下顯示幅度最高的詳細資料。")
    for item in items[:detailed_count]:
        lines.extend(_item_lines(item))
    remaining = items[detailed_count:]
    if remaining:
        lines.extend([
            "",
            f"其他同批觸發（{len(remaining)} 種）：",
            ", ".join(item["symbol"] for item in remaining),
        ])
    return "\n".join(lines)


def merge_payloads(price_payload, technical_events_payload, divergence_events_payload,
                   snapshot_payload, technical_payload, divergence_payload,
                   policy: dict[str, Any] | None = None) -> dict[str, Any]:
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
    selected_policy = policy or _load_policy()
    price_events, technical_events, divergence_events = _selected_events(
        price_payload, technical_events_payload, divergence_events_payload, selected_policy
    )
    if not price_events:
        return {"schema_version": 1, "run_id": run_id, "status": "NO_NOTIFICATION",
                "notification_id": None, "event_count": 0, "items": [], "message": None}
    all_events = price_events + technical_events + divergence_events

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
        price_event = events["price"][0]
        coingecko_id = market.get("coingecko_id")
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
            "abnormality_score": price_event.get("abnormality_score", 0),
            "severity_tier": price_event.get("severity_tier", "N/A"),
            "active_conditions": price_event.get("active_conditions", []),
            "coingecko_url": (
                f"https://www.coingecko.com/en/coins/{coingecko_id}"
                if isinstance(coingecko_id, str) and coingecko_id
                else "N/A"
            ),
            "tradingview_url": _tradingview_url(technical, symbol),
        })
    items.sort(key=lambda item: (-float(item["abnormality_score"] or 0), item["symbol"]))

    identity = {"run_id": run_id, "price": [_event_key(item) for item in price_events],
                "technical": [_event_key(item) for item in technical_events],
                "divergence": [_event_key(item) for item in divergence_events]}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"),
                                       ensure_ascii=False).encode("utf-8")).hexdigest()
    message = _render_message(run_id, items)
    if len(message) > TELEGRAM_MESSAGE_LIMIT:
        message = _render_overflow_message(run_id, items)
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
        _load_policy(),
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

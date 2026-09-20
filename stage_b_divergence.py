"""ALTCOIN RADAR Stage B.2: confirmed Daily/Weekly Wilder RSI divergence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from radar_scan import OUTPUT_DIR, PROJECT_ROOT, atomic_write_json, utc_now
from radar_state import SCAN_STATUS_PATH, SNAPSHOT_PATH, STATE_PATH as RADAR_STATE_PATH, TRIGGERS_PATH, read_json
from stage_b_enrich import CONFIG_PATH as EXCHANGE_CONFIG_PATH, TECHNICAL_STATE_PATH, sort_markets
from stage_b_sources import Candle, PublicSpotAdapter, SpotMarket, build_adapters
from state_store import FileSystemStateStore, StateStore


TECHNICAL_CONFIG_PATH = PROJECT_ROOT / "config" / "technical_config.json"
DIVERGENCE_STATE_PATH = PROJECT_ROOT / "state" / "divergence_state.json"
DIVERGENCE_OUTPUT_PATH = OUTPUT_DIR / "latest_divergence.json"
DIVERGENCE_EVENTS_PATH = OUTPUT_DIR / "latest_divergence_events.json"

FORMAL_TYPES = {
    "DAILY_BULLISH",
    "DAILY_BULLISH_CONSECUTIVE",
    "DAILY_BEARISH",
    "DAILY_BEARISH_CONSECUTIVE",
    "WEEKLY_BULLISH",
    "WEEKLY_BULLISH_CONSECUTIVE",
    "WEEKLY_BEARISH",
    "WEEKLY_BEARISH_CONSECUTIVE",
}


def wilder_rsi_series(closes: list[float], period: int = 14) -> list[float | None]:
    result: list[float | None] = [None] * len(closes)
    if len(closes) < period + 1:
        return result
    changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period

    def value() -> float:
        if average_gain == 0 and average_loss == 0:
            return 50.0
        if average_loss == 0:
            return 100.0
        if average_gain == 0:
            return 0.0
        return 100.0 - 100.0 / (1.0 + average_gain / average_loss)

    result[period] = value()
    for change_index in range(period, len(changes)):
        average_gain = (average_gain * (period - 1) + gains[change_index]) / period
        average_loss = (average_loss * (period - 1) + losses[change_index]) / period
        result[change_index + 1] = value()
    return result


def candle_date(open_ms: int) -> str:
    return datetime.fromtimestamp(open_ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def find_pivots(
    candles: list[Candle],
    rsi_values: list[float | None],
    side: str,
    left: int,
    right: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    field = "low" if side == "low" else "high"
    confirmed = []
    pending = []
    for index in range(left, len(candles)):
        price = getattr(candles[index], field)
        rsi = rsi_values[index]
        if price is None or rsi is None:
            continue
        left_values = [getattr(item, field) for item in candles[index - left : index]]
        available_right = candles[index + 1 : min(len(candles), index + right + 1)]
        right_values = [getattr(item, field) for item in available_right]
        if any(value is None for value in left_values + right_values):
            continue
        neighbors = left_values + right_values
        comparator = all(price < value for value in neighbors) if side == "low" else all(
            price > value for value in neighbors
        )
        if not comparator:
            continue
        pivot = {
            "timestamp": candle_date(candles[index].open_ms),
            "price": price,
            "rsi": rsi,
            "confirmed": len(available_right) == right,
            "bar_index": index,
        }
        (confirmed if pivot["confirmed"] else pending).append(pivot)
    return confirmed, pending


def bullish_pair(pivot_1: dict[str, Any], pivot_2: dict[str, Any], config: dict[str, Any]) -> bool:
    separation = pivot_2["bar_index"] - pivot_1["bar_index"]
    price_difference = (pivot_1["price"] - pivot_2["price"]) / abs(pivot_1["price"]) * 100
    rsi_difference = pivot_2["rsi"] - pivot_1["rsi"]
    return (
        separation >= config["min_pivot_separation_bars"]
        and price_difference >= config["min_price_difference_pct"]
        and rsi_difference >= config["min_rsi_difference"]
    )


def bearish_pair(pivot_1: dict[str, Any], pivot_2: dict[str, Any], config: dict[str, Any]) -> bool:
    separation = pivot_2["bar_index"] - pivot_1["bar_index"]
    price_difference = (pivot_2["price"] - pivot_1["price"]) / abs(pivot_1["price"]) * 100
    rsi_difference = pivot_1["rsi"] - pivot_2["rsi"]
    return (
        separation >= config["min_pivot_separation_bars"]
        and price_difference >= config["min_price_difference_pct"]
        and rsi_difference >= config["min_rsi_difference"]
    )


def divergence_signature(
    symbol: str, timeframe: str, direction: str, pivots: list[dict[str, Any]], consecutive: bool = False
) -> str:
    kind = f"{direction}_CONSECUTIVE" if consecutive else direction
    dates = "|".join(item["timestamp"] for item in pivots)
    return f"{symbol}|{timeframe}|{kind}|{dates}"


def signal_record(
    symbol: str,
    prefix: str,
    timeframe: str,
    direction: str,
    pivots: list[dict[str, Any]],
    consecutive: bool = False,
) -> dict[str, Any]:
    signal_type = f"{prefix}_{direction}" + ("_CONSECUTIVE" if consecutive else "")
    if signal_type not in FORMAL_TYPES:
        raise ValueError(f"unsupported divergence type: {signal_type}")
    clean_pivots = [{key: value for key, value in item.items() if key != "bar_index"} for item in pivots]
    return {
        "type": signal_type,
        "signature": divergence_signature(symbol, timeframe, direction, pivots, consecutive),
        "pivots": clean_pivots,
    }


def signals_from_pivots(
    symbol: str,
    prefix: str,
    timeframe: str,
    lows: list[dict[str, Any]],
    highs: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    signals = []
    if len(lows) >= 2 and bullish_pair(lows[-2], lows[-1], config):
        signals.append(signal_record(symbol, prefix, timeframe, "BULLISH", lows[-2:]))
    if len(lows) >= 3 and bullish_pair(lows[-3], lows[-2], config) and bullish_pair(
        lows[-2], lows[-1], config
    ):
        signals.append(
            signal_record(symbol, prefix, timeframe, "BULLISH", lows[-3:], consecutive=True)
        )
    if len(highs) >= 2 and bearish_pair(highs[-2], highs[-1], config):
        signals.append(signal_record(symbol, prefix, timeframe, "BEARISH", highs[-2:]))
    if len(highs) >= 3 and bearish_pair(highs[-3], highs[-2], config) and bearish_pair(
        highs[-2], highs[-1], config
    ):
        signals.append(
            signal_record(symbol, prefix, timeframe, "BEARISH", highs[-3:], consecutive=True)
        )
    return signals


def detect_timeframe(
    symbol: str,
    candles: list[Candle],
    timeframe_name: str,
    config: dict[str, Any],
    rsi_period: int,
) -> dict[str, Any]:
    completed = sorted(
        (
            item
            for item in candles
            if item.complete and item.high is not None and item.low is not None
        ),
        key=lambda item: item.open_ms,
    )
    rsi_values = wilder_rsi_series([item.close for item in completed], rsi_period)
    lows, pending_lows = find_pivots(
        completed, rsi_values, "low", config["pivot_left"], config["pivot_right"]
    )
    highs, pending_highs = find_pivots(
        completed, rsi_values, "high", config["pivot_left"], config["pivot_right"]
    )
    prefix = "DAILY" if timeframe_name == "daily" else "WEEKLY"
    timeframe = config["timeframe"]
    pair_config = {
        **config,
        "min_price_difference_pct": config["min_price_difference_pct"],
        "min_rsi_difference": config["min_rsi_difference"],
    }
    signals = signals_from_pivots(symbol, prefix, timeframe, lows, highs, pair_config)
    pending = []
    if lows and pending_lows and bullish_pair(lows[-1], pending_lows[-1], pair_config):
        pending.append(
            {
                "type": f"{prefix}_BULLISH_PENDING",
                "pivots": [
                    {key: value for key, value in item.items() if key != "bar_index"}
                    for item in (lows[-1], pending_lows[-1])
                ],
            }
        )
    if highs and pending_highs and bearish_pair(highs[-1], pending_highs[-1], pair_config):
        pending.append(
            {
                "type": f"{prefix}_BEARISH_PENDING",
                "pivots": [
                    {key: value for key, value in item.items() if key != "bar_index"}
                    for item in (highs[-1], pending_highs[-1])
                ],
            }
        )
    quality = "COMPLETE" if len(lows) >= 2 and len(highs) >= 2 else (
        "PARTIAL" if len(lows) >= 2 or len(highs) >= 2 else "DATA_INSUFFICIENT"
    )
    by_type = {item["type"]: item for item in signals}
    return {
        "bullish": by_type.get(f"{prefix}_BULLISH"),
        "bullish_consecutive": by_type.get(f"{prefix}_BULLISH_CONSECUTIVE"),
        "bearish": by_type.get(f"{prefix}_BEARISH"),
        "bearish_consecutive": by_type.get(f"{prefix}_BEARISH_CONSECUTIVE"),
        "pending": pending,
        "pivots_used": {
            "confirmed_low_count": len(lows),
            "confirmed_high_count": len(highs),
            "latest_lows": [
                {key: value for key, value in item.items() if key != "bar_index"} for item in lows[-3:]
            ],
            "latest_highs": [
                {key: value for key, value in item.items() if key != "bar_index"} for item in highs[-3:]
            ],
        },
        "signals": signals,
        "quality": quality,
        "completed_candles": len(completed),
    }


def timeframe_state(
    previous: dict[str, Any] | None, signals: list[dict[str, Any]]
) -> dict[str, Any]:
    seen = set(previous.get("seen_signatures", [])) if previous else set()
    seen.update(item["signature"] for item in signals)
    return {"active_signals": signals, "seen_signatures": sorted(seen)}


def divergence_events(
    symbol: str,
    timeframe: str,
    previous: dict[str, Any] | None,
    signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    known = set(previous.get("seen_signatures", [])) if previous else set()
    new_signals = [item for item in signals if item["signature"] not in known]
    events = []
    new_consecutive_directions = {
        item["type"].replace("_CONSECUTIVE", "") for item in new_signals if item["type"].endswith("_CONSECUTIVE")
    }
    for signal in new_signals:
        if not signal["type"].endswith("_CONSECUTIVE") and signal["type"] in new_consecutive_directions:
            continue
        event = "DIVERGENCE_NEW"
        if signal["type"].endswith("_CONSECUTIVE"):
            ordinary_type = signal["type"].replace("_CONSECUTIVE", "")
            direction = "BULLISH" if "BULLISH" in signal["type"] else "BEARISH"
            tf_code = "1D" if timeframe == "daily" else "1W"
            prefix_pivots = signal["pivots"][:2]
            ordinary_signature = (
                f"{symbol}|{tf_code}|{direction}|"
                + "|".join(item["timestamp"] for item in prefix_pivots)
            )
            if ordinary_signature in known or any(
                item.get("type") == ordinary_type for item in (previous or {}).get("active_signals", [])
            ):
                event = "DIVERGENCE_ESCALATION"
        events.append(
            {
                "symbol": symbol,
                "event": event,
                "type": signal["type"],
                "signature": signal["signature"],
                "pivots": signal["pivots"],
            }
        )
    return events


def empty_timeframe() -> dict[str, Any]:
    return {
        "bullish": None,
        "bullish_consecutive": None,
        "bearish": None,
        "bearish_consecutive": None,
        "pending": [],
        "pivots_used": {
            "confirmed_low_count": 0,
            "confirmed_high_count": 0,
            "latest_lows": [],
            "latest_highs": [],
        },
        "signals": [],
        "quality": "DATA_INSUFFICIENT",
        "completed_candles": 0,
    }


def analyze_asset(
    symbol: str,
    preferred_source: str | None,
    markets: list[SpotMarket],
    adapters: dict[str, PublicSpotAdapter],
    exchange_config: dict[str, Any],
    technical_config: dict[str, Any],
) -> dict[str, Any]:
    ordered = sort_markets(markets, exchange_config)
    distinct = []
    seen_exchanges = set()
    for market in ordered:
        if market.exchange not in seen_exchanges:
            distinct.append(market)
            seen_exchanges.add(market.exchange)
    candidates = sorted(
        distinct[:4], key=lambda item: 0 if item.exchange == preferred_source else 1
    )
    best = None
    rank = {"DATA_INSUFFICIENT": 0, "PARTIAL": 1, "COMPLETE": 2}
    for market in candidates:
        adapter = adapters[market.exchange]
        try:
            daily_candles = adapter.candles(
                market, "1d", int(technical_config["daily"]["history_target_completed_candles"]) + 8
            )
            weekly_candles = adapter.candles(
                market, "1w", int(technical_config["weekly"]["history_target_completed_candles"]) + 5
            )
            common = {
                "min_price_difference_pct": technical_config["min_price_difference_pct"],
                "min_rsi_difference": technical_config["min_rsi_difference"],
            }
            daily = detect_timeframe(
                symbol,
                daily_candles,
                "daily",
                {**technical_config["daily"], **common},
                int(technical_config["rsi_period"]),
            )
            weekly = detect_timeframe(
                symbol,
                weekly_candles,
                "weekly",
                {**technical_config["weekly"], **common},
                int(technical_config["rsi_period"]),
            )
        except Exception:
            continue
        quality = "COMPLETE" if daily["quality"] == weekly["quality"] == "COMPLETE" else (
            "PARTIAL"
            if daily["quality"] != "DATA_INSUFFICIENT" or weekly["quality"] != "DATA_INSUFFICIENT"
            else "DATA_INSUFFICIENT"
        )
        candidate = {"market": market, "daily": daily, "weekly": weekly, "data_quality": quality}
        if best is None or rank[quality] > rank[best["data_quality"]]:
            best = candidate
        if quality == "COMPLETE" and market.exchange == preferred_source:
            break
        if quality == "COMPLETE":
            break
    if best is None:
        return {
            "symbol": symbol,
            "daily": empty_timeframe(),
            "weekly": empty_timeframe(),
            "history_source": None,
            "source_changed_reason": None,
            "data_quality": "DATA_INSUFFICIENT",
        }
    selected = best["market"].exchange
    reason = None
    if preferred_source and selected != preferred_source:
        reason = "Preferred Stage B.1 history source lacked sufficient completed Daily/Weekly history"
    return {
        "symbol": symbol,
        "daily": best["daily"],
        "weekly": best["weekly"],
        "history_source": selected,
        "source_changed_reason": reason,
        "data_quality": best["data_quality"],
    }


def validate_pipeline(
    scan_status: dict[str, Any],
    snapshot: dict[str, Any],
    triggers: dict[str, Any],
    radar_state: dict[str, Any],
    technical_state: dict[str, Any],
) -> str:
    run_ids = {
        scan_status.get("run_id"),
        snapshot.get("run_id"),
        triggers.get("run_id"),
        radar_state.get("last_run_id"),
        technical_state.get("last_run_id"),
    }
    if None in run_ids or len(run_ids) != 1:
        raise ValueError("STAGE_INPUT_MISMATCH")
    if scan_status.get("scan_status") != "MARKET_DATA_COMPLETE":
        raise ValueError("Stage A is not MARKET_DATA_COMPLETE")
    if scan_status.get("market_data_returned") != scan_status.get("mapped_total"):
        raise ValueError("Stage A coverage is incomplete")
    return next(iter(run_ids))


def skipped(reason: str, output_path: Path, events_path: Path) -> dict[str, Any]:
    timestamp = utc_now()
    status = "STAGE_INPUT_MISMATCH" if "STAGE_INPUT_MISMATCH" in reason else "STAGE_B2_SKIPPED"
    atomic_write_json(
        output_path,
        {"generated_at": timestamp, "status": status, "reason": reason, "count": 0, "assets": []},
    )
    atomic_write_json(
        events_path,
        {
            "generated_at": timestamp,
            "status": status,
            "reason": reason,
            "count": 0,
            "events": [],
            "new_count": 0,
            "escalation_count": 0,
        },
    )
    return {"exit_code": 2, "skipped": True, "reason": reason}


def run_divergence(
    bootstrap: bool = False,
    *,
    technical_config_path: Path = TECHNICAL_CONFIG_PATH,
    exchange_config_path: Path = EXCHANGE_CONFIG_PATH,
    scan_status_path: Path = SCAN_STATUS_PATH,
    snapshot_path: Path = SNAPSHOT_PATH,
    triggers_path: Path = TRIGGERS_PATH,
    radar_state_path: Path = RADAR_STATE_PATH,
    technical_state_path: Path = TECHNICAL_STATE_PATH,
    divergence_state_path: Path = DIVERGENCE_STATE_PATH,
    output_path: Path = DIVERGENCE_OUTPUT_PATH,
    events_path: Path = DIVERGENCE_EVENTS_PATH,
    adapters_override: list[PublicSpotAdapter] | None = None,
    state_store: StateStore | None = None,
) -> dict[str, Any]:
    state_store = state_store or FileSystemStateStore()
    try:
        technical_config = read_json(technical_config_path)
        exchange_config = read_json(exchange_config_path)
        scan_status = read_json(scan_status_path)
        snapshot = read_json(snapshot_path)
        triggers = read_json(triggers_path)
        radar_state = state_store.load(radar_state_path)
        technical_state = state_store.load(technical_state_path)
        run_id = validate_pipeline(scan_status, snapshot, triggers, radar_state, technical_state)
    except Exception as exc:
        return skipped(str(exc), output_path, events_path)

    previous_assets = {}
    if not bootstrap:
        try:
            missing = object()
            previous_state = state_store.load(divergence_state_path, missing)
            if previous_state is missing:
                return skipped(
                    "Divergence state is not initialized; use --bootstrap explicitly",
                    output_path,
                    events_path,
                )
            if previous_state.get("schema_version") != 1:
                raise ValueError("divergence_state.json is invalid")
            previous_assets = previous_state.get("assets", {})
        except Exception as exc:
            return skipped(str(exc), output_path, events_path)

    try:
        adapter_list = adapters_override if adapters_override is not None else build_adapters(exchange_config)
    except Exception as exc:
        return skipped(str(exc), output_path, events_path)
    excluded = {str(item).casefold() for item in exchange_config.get("excluded_exchanges", [])}
    if any(adapter.name.casefold() in excluded for adapter in adapter_list):
        return skipped("Excluded history source adapter detected", output_path, events_path)

    active = {
        symbol: value for symbol, value in radar_state["assets"].items() if value.get("active") is True
    }
    canonical_symbols = set(radar_state["assets"])
    baseline_symbols = {
        symbol
        for symbol, entry in radar_state["assets"].items()
        if entry.get("baseline_required") is True
    }
    previous_assets = {
        symbol: entry
        for symbol, entry in previous_assets.items()
        if symbol in canonical_symbols and symbol not in baseline_symbols
    }
    targets = set(active)
    adapter_map = {adapter.name: adapter for adapter in adapter_list}
    markets_by_symbol = {symbol: [] for symbol in targets}
    source_errors = {}
    for adapter in adapter_list:
        try:
            for market in adapter.discover(targets, set(exchange_config.get("allowed_quotes", []))):
                if market.base in markets_by_symbol:
                    markets_by_symbol[market.base].append(market)
        except Exception as exc:
            source_errors[adapter.name] = str(exc)

    timestamp = utc_now()
    outputs = []
    next_assets = dict(previous_assets)
    events = []
    technical_assets = technical_state.get("assets", {})
    for symbol in sorted(active):
        preferred = technical_assets.get(symbol, {}).get("history_source")
        result = analyze_asset(
            symbol,
            preferred,
            markets_by_symbol[symbol],
            adapter_map,
            exchange_config,
            technical_config,
        )
        outputs.append(
            {
                "symbol": symbol,
                "daily": {key: value for key, value in result["daily"].items() if key not in {"signals", "quality", "completed_candles"}},
                "weekly": {key: value for key, value in result["weekly"].items() if key not in {"signals", "quality", "completed_candles"}},
                "history_source": result["history_source"],
                "source_changed_reason": result["source_changed_reason"],
                "data_quality": result["data_quality"],
            }
        )
        previous = previous_assets.get(symbol, {})
        if result["data_quality"] == "DATA_INSUFFICIENT" and previous:
            next_assets[symbol] = previous
            continue
        daily_state = timeframe_state(previous.get("daily"), result["daily"]["signals"])
        weekly_state = timeframe_state(previous.get("weekly"), result["weekly"]["signals"])
        next_assets[symbol] = {
            "coingecko_id": active[symbol].get("coingecko_id"),
            "history_source": result["history_source"],
            "source_changed_reason": result["source_changed_reason"],
            "data_quality": result["data_quality"],
            "last_seen_at": timestamp,
            "daily": daily_state,
            "weekly": weekly_state,
        }
        if not bootstrap and symbol not in baseline_symbols:
            events.extend(divergence_events(symbol, "daily", previous.get("daily"), result["daily"]["signals"]))
            events.extend(divergence_events(symbol, "weekly", previous.get("weekly"), result["weekly"]["signals"]))

    quality_counts = {
        quality: sum(item["data_quality"] == quality for item in outputs)
        for quality in ("COMPLETE", "PARTIAL", "DATA_INSUFFICIENT")
    }
    state_payload = {
        "schema_version": 1,
        "last_run_id": run_id,
        "updated_at": timestamp,
        "assets": next_assets,
    }
    output_payload = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "BOOTSTRAP" if bootstrap else "UPDATED",
        "attempted": len(active),
        "quality_counts": quality_counts,
        "source_errors": source_errors,
        "assets_baselined": len(set(active) & baseline_symbols),
        "count": len(outputs),
        "assets": outputs,
    }
    event_payload = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "BOOTSTRAP_SUPPRESSED" if bootstrap else "UPDATED",
        "count": len(events),
        "events": events,
        "new_count": sum(item["event"] == "DIVERGENCE_NEW" for item in events),
        "escalation_count": sum(item["event"] == "DIVERGENCE_ESCALATION" for item in events),
    }
    atomic_write_json(output_path, output_payload)
    atomic_write_json(events_path, event_payload)
    state_store.save_atomic(divergence_state_path, state_payload)
    return {
        "exit_code": 0,
        "skipped": False,
        "bootstrap": bootstrap,
        "run_id": run_id,
        "active": len(active),
        "quality_counts": quality_counts,
        "new_events": event_payload["new_count"],
        "escalation_events": event_payload["escalation_count"],
        "assets_baselined": len(set(active) & baseline_symbols),
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("ALTCOIN RADAR - STAGE B.2")
    print()
    if summary.get("skipped"):
        print(f"Stage B.2 status: {'STAGE_INPUT_MISMATCH' if 'STAGE_INPUT_MISMATCH' in summary['reason'] else 'SKIPPED'}")
        print(f"Reason: {summary['reason']}")
        return
    print(f"Run ID: {summary['run_id']}")
    print(f"Active coins attempted: {summary['active']}")
    print(f"Complete: {summary['quality_counts']['COMPLETE']}")
    print(f"Partial: {summary['quality_counts']['PARTIAL']}")
    print(f"Data insufficient: {summary['quality_counts']['DATA_INSUFFICIENT']}")
    print(f"New divergence events: {summary['new_events']}")
    print(f"Divergence escalation events: {summary['escalation_events']}")
    if summary.get("bootstrap"):
        print()
        print("Divergence state initialized")
        print("Notifications suppressed")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", action="store_true")
    args = parser.parse_args()
    try:
        summary = run_divergence(bootstrap=args.bootstrap)
    except Exception as exc:
        summary = skipped(str(exc), DIVERGENCE_OUTPUT_PATH, DIVERGENCE_EVENTS_PATH)
    print_summary(summary)
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())

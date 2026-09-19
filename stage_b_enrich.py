"""ALTCOIN RADAR Stage B.1: enrich active price triggers with Spot technical data."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

from radar_scan import OUTPUT_DIR, PROJECT_ROOT, atomic_write_json, utc_now
from radar_state import (
    SCAN_STATUS_PATH,
    SNAPSHOT_PATH,
    TRIGGERS_PATH,
    STATE_PATH as RADAR_STATE_PATH,
    read_json,
)
from stage_b_sources import Candle, PublicSpotAdapter, SpotMarket, build_adapters
from state_store import FileSystemStateStore, StateStore
from telemetry import record_asset_enrichment


CONFIG_PATH = PROJECT_ROOT / "config" / "exchange_priority.json"
TECHNICAL_STATE_PATH = PROJECT_ROOT / "state" / "technical_state.json"
STAGE_B_OUTPUT_PATH = OUTPUT_DIR / "latest_stage_b.json"
TECHNICAL_EVENTS_PATH = OUTPUT_DIR / "latest_technical_events.json"

TECHNICAL_NOTIFICATION_EVENTS = {
    "RSI_4H_EXTREME_HOT_NEW",
    "RSI_4H_EXTREME_COLD_NEW",
    "VOLUME_STRONG_NEW",
    "VOLUME_ESCALATION",
    "VOLUME_CONTRACTION_EXPANSION_NEW",
    "PRICE_UP_WITHOUT_VOLUME_NEW",
}
VOLUME_RANK = {"DATA_INSUFFICIENT": -1, "WEAK": 0, "NORMAL": 1, "ELEVATED": 2, "STRONG": 3}


def wilder_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period
    for index in range(period, len(changes)):
        average_gain = (average_gain * (period - 1) + gains[index]) / period
        average_loss = (average_loss * (period - 1) + losses[index]) / period
    if average_gain == 0 and average_loss == 0:
        return 50.0
    if average_loss == 0:
        return 100.0
    if average_gain == 0:
        return 0.0
    relative_strength = average_gain / average_loss
    return 100.0 - 100.0 / (1.0 + relative_strength)


def completed_candles(candles: list[Candle]) -> list[Candle]:
    return sorted((item for item in candles if item.complete), key=lambda item: item.open_ms)


def rsi_from_candles(candles: list[Candle], period: int = 14) -> float | None:
    return wilder_rsi([item.close for item in completed_candles(candles)], period)


def rsi_4h_state(value: float | None) -> str:
    if value is None:
        return "DATA_INSUFFICIENT"
    if value >= 80:
        return "EXTREME_HOT"
    if value <= 20:
        return "EXTREME_COLD"
    if 45 <= value <= 55:
        return "NEUTRAL"
    return "NORMAL"


def volume_status(value: float | None) -> str:
    if value is None:
        return "DATA_INSUFFICIENT"
    if value < 0.8:
        return "WEAK"
    if value < 1.5:
        return "NORMAL"
    if value < 2.0:
        return "ELEVATED"
    return "STRONG"


def volume_metrics(
    current_quote_volume: float | None,
    daily_candles: list[Candle],
    minimum: int = 7,
    target: int = 20,
) -> tuple[float | None, str, str]:
    historical = [
        item.quote_volume
        for item in completed_candles(daily_candles)
        if item.quote_volume is not None and item.quote_volume > 0
    ][-target:]
    if current_quote_volume is None or current_quote_volume <= 0 or len(historical) < minimum:
        return None, "DATA_INSUFFICIENT", "NONE"
    baseline = statistics.median(historical)
    if baseline <= 0:
        return None, "DATA_INSUFFICIENT", "NONE"
    ratio = current_quote_volume / baseline
    pattern = "NONE"
    if len(historical) >= 14 and ratio >= 1.5:
        recent_median = statistics.median(historical[-7:])
        prior_median = statistics.median(historical[:-7])
        if prior_median > 0 and recent_median <= prior_median * 0.7:
            pattern = "CONTRACTION_THEN_EXPANSION"
    return ratio, volume_status(ratio), pattern


def price_volume_note(change_24h: float | int | None, ratio: float | None) -> str:
    if change_24h is not None and change_24h >= 10 and ratio is not None and ratio < 1.0:
        return "PRICE_UP_WITHOUT_CLEAR_VOLUME"
    return "NONE"


def cross_exchange_confirmation(markets: list[SpotMarket]) -> tuple[str, list[dict[str, Any]]]:
    distinct: list[SpotMarket] = []
    seen = set()
    for market in markets:
        if market.exchange not in seen:
            seen.add(market.exchange)
            distinct.append(market)
        if len(distinct) == 3:
            break
    prices = [item.price for item in distinct if item.price > 0]
    rendered = [
        {
            "exchange": item.exchange,
            "pair": item.native_symbol,
            "quote": item.quote,
            "price": item.price,
        }
        for item in distinct
    ]
    if not prices:
        return "DATA_INSUFFICIENT", rendered
    if len(prices) == 1:
        return "SINGLE_SOURCE", rendered
    median_price = statistics.median(prices)
    difference = (max(prices) - min(prices)) / median_price * 100
    if difference <= 1:
        return "CONFIRMED", rendered
    if difference <= 3:
        return "MINOR_DIVERGENCE", rendered
    return "EXCHANGE_DIVERGENCE", rendered


def sort_markets(markets: list[SpotMarket], config: dict[str, Any]) -> list[SpotMarket]:
    quote_order = {name: index for index, name in enumerate(config.get("allowed_quotes", []))}
    exchange_order = {name: index for index, name in enumerate(config.get("tie_break_order", []))}
    return sorted(
        markets,
        key=lambda item: (
            -item.quote_volume,
            quote_order.get(item.quote, 999),
            exchange_order.get(item.exchange, 999),
            item.native_symbol,
        ),
    )


def active_signals(record: dict[str, Any]) -> list[str]:
    signals = []
    rsi_state = record.get("rsi_4h_state")
    if rsi_state == "EXTREME_HOT":
        signals.append("RSI_4H_EXTREME_HOT")
    elif rsi_state == "EXTREME_COLD":
        signals.append("RSI_4H_EXTREME_COLD")
    if record.get("volume_status") == "ELEVATED":
        signals.append("VOLUME_ELEVATED")
    elif record.get("volume_status") == "STRONG":
        signals.append("VOLUME_STRONG")
    if record.get("volume_pattern") == "CONTRACTION_THEN_EXPANSION":
        signals.append("VOLUME_CONTRACTION_EXPANSION")
    if record.get("volume_note") == "PRICE_UP_WITHOUT_CLEAR_VOLUME":
        signals.append("PRICE_UP_WITHOUT_VOLUME")
    return signals


def technical_state_entry(
    record: dict[str, Any], timestamp: str, price_episode_id: int | None
) -> dict[str, Any]:
    return {
        "live_source": record.get("live_source"),
        "history_source": record.get("history_source"),
        "rsi_4h": record.get("rsi_4h"),
        "rsi_1d": record.get("rsi_1d"),
        "rsi_1w": record.get("rsi_1w"),
        "rsi_4h_state": record.get("rsi_4h_state"),
        "volume_ratio": record.get("volume_ratio"),
        "volume_status": record.get("volume_status"),
        "volume_pattern": record.get("volume_pattern"),
        "volume_note": record.get("volume_note"),
        "range_24h_pct": record.get("range_24h_pct"),
        "turnover_ratio": record.get("turnover_ratio"),
        "cross_exchange_status": record.get("cross_exchange_status"),
        "data_quality": record.get("data_quality"),
        "last_seen_at": timestamp,
        "price_episode_id": price_episode_id,
        "active_technical_signals": active_signals(record),
    }


def evaluate_technical_events(
    symbol: str,
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    current_signals = set(current.get("active_technical_signals", []))
    previous_signals = set(previous.get("active_technical_signals", [])) if previous else set()

    def add(event: str, value: Any = None, **extra: Any) -> None:
        item = {"symbol": symbol, "event": event, "value": value, "notify": event in TECHNICAL_NOTIFICATION_EVENTS}
        item.update(extra)
        events.append(item)

    for signal, event in (
        ("RSI_4H_EXTREME_HOT", "RSI_4H_EXTREME_HOT_NEW"),
        ("RSI_4H_EXTREME_COLD", "RSI_4H_EXTREME_COLD_NEW"),
    ):
        if signal in current_signals and signal not in previous_signals:
            add(event, current.get("rsi_4h"))

    current_volume = current.get("volume_status", "DATA_INSUFFICIENT")
    previous_volume = previous.get("volume_status", "DATA_INSUFFICIENT") if previous else "DATA_INSUFFICIENT"
    if previous is None or previous_volume == "DATA_INSUFFICIENT":
        if current_volume == "ELEVATED":
            add("VOLUME_ELEVATED_NEW", current.get("volume_ratio"))
        elif current_volume == "STRONG":
            add("VOLUME_STRONG_NEW", current.get("volume_ratio"))
    elif (
        current_volume in {"ELEVATED", "STRONG"}
        and VOLUME_RANK[current_volume] > VOLUME_RANK.get(previous_volume, -1)
    ):
        add(
            "VOLUME_ESCALATION",
            current.get("volume_ratio"),
            previous_status=previous_volume,
            current_status=current_volume,
        )

    if (
        "VOLUME_CONTRACTION_EXPANSION" in current_signals
        and "VOLUME_CONTRACTION_EXPANSION" not in previous_signals
    ):
        add("VOLUME_CONTRACTION_EXPANSION_NEW", current.get("volume_ratio"))
    if "PRICE_UP_WITHOUT_VOLUME" in current_signals and "PRICE_UP_WITHOUT_VOLUME" not in previous_signals:
        add("PRICE_UP_WITHOUT_VOLUME_NEW", current.get("volume_ratio"))

    if previous is not None:
        old_sources = (previous.get("live_source"), previous.get("history_source"))
        new_sources = (current.get("live_source"), current.get("history_source"))
        if old_sources != new_sources:
            add("SOURCE_CHANGED", None, previous_sources=old_sources, current_sources=new_sources)

        cleared = set()
        if current.get("rsi_4h") is not None:
            cleared.update(
                signal
                for signal in previous_signals - current_signals
                if signal.startswith("RSI_4H_")
            )
        if current.get("volume_ratio") is not None:
            cleared.update(
                signal
                for signal in previous_signals - current_signals
                if signal.startswith("VOLUME_") or signal == "PRICE_UP_WITHOUT_VOLUME"
            )
        if cleared:
            add("TECHNICAL_SIGNAL_CLEARED", None, cleared_signals=sorted(cleared))
    return events


def _fetch_cached(
    adapter: PublicSpotAdapter,
    market: SpotMarket,
    timeframe: str,
    limit: int,
    cache: dict[tuple[str, str, str], list[Candle]],
) -> list[Candle]:
    key = (adapter.name, market.native_symbol, timeframe)
    if key not in cache:
        cache[key] = adapter.candles(market, timeframe, limit)
    return cache[key]


def enrich_asset(
    symbol: str,
    snapshot: dict[str, Any],
    radar_entry: dict[str, Any],
    markets: list[SpotMarket],
    adapters: dict[str, PublicSpotAdapter],
    config: dict[str, Any],
    candle_cache: dict[tuple[str, str, str], list[Candle]],
) -> dict[str, Any]:
    ordered = sort_markets(markets, config)
    live_market = ordered[0] if ordered else None
    history_market = None
    rsi_values = {"4h": None, "1d": None, "1w": None}
    period = int(config.get("rsi_period", 14))
    candle_target = int(config.get("candle_target", 100))

    for candidate in ordered:
        adapter = adapters[candidate.exchange]
        try:
            values = {
                timeframe: rsi_from_candles(
                    _fetch_cached(adapter, candidate, timeframe, candle_target + 2, candle_cache),
                    period,
                )
                for timeframe in ("4h", "1d", "1w")
            }
        except Exception:
            continue
        if all(value is not None for value in values.values()):
            history_market = candidate
            rsi_values = values
            break

    volume_ratio = None
    volume_state = "DATA_INSUFFICIENT"
    volume_pattern = "NONE"
    if live_market is not None:
        try:
            daily = _fetch_cached(
                adapters[live_market.exchange], live_market, "1d", 30, candle_cache
            )
            volume_ratio, volume_state, volume_pattern = volume_metrics(
                live_market.quote_volume,
                daily,
                int(config.get("volume_baseline_minimum", 7)),
                int(config.get("volume_baseline_target", 20)),
            )
        except Exception:
            pass

    cross_status, cross_prices = cross_exchange_confirmation(ordered)
    change_24h = snapshot.get("price_change_percentage_24h")
    record = {
        "symbol": symbol,
        "price": snapshot.get("current_price"),
        "change_1h": snapshot.get("price_change_percentage_1h"),
        "change_24h": change_24h,
        "range_24h_pct": snapshot.get("range_24h_pct"),
        "turnover_ratio": snapshot.get("turnover_ratio"),
        "rsi_4h": rsi_values["4h"],
        "rsi_1d": rsi_values["1d"],
        "rsi_1w": rsi_values["1w"],
        "rsi_4h_state": rsi_4h_state(rsi_values["4h"]),
        "volume_24h_quote": live_market.quote_volume if live_market else None,
        "volume_ratio": volume_ratio,
        "volume_status": volume_state,
        "volume_pattern": volume_pattern,
        "volume_note": price_volume_note(change_24h, volume_ratio),
        "live_source": live_market.exchange if live_market else None,
        "history_source": history_market.exchange if history_market else None,
        "cross_exchange_status": cross_status,
        "cross_exchange_prices": cross_prices,
        "data_quality": "DATA_INSUFFICIENT",
    }
    complete = (
        live_market is not None
        and live_market.quote_volume_exact
        and history_market is not None
        and all(value is not None for value in rsi_values.values())
        and volume_ratio is not None
        and cross_status in {"CONFIRMED", "MINOR_DIVERGENCE", "EXCHANGE_DIVERGENCE"}
    )
    if complete:
        record["data_quality"] = "COMPLETE"
    elif live_market is not None or history_market is not None:
        record["data_quality"] = "PARTIAL"
    return record


def validate_stage_a(
    scan_status: dict[str, Any],
    snapshot: dict[str, Any],
    triggers: dict[str, Any],
    radar_state: dict[str, Any],
) -> None:
    run_ids = {
        scan_status.get("run_id"),
        snapshot.get("run_id"),
        triggers.get("run_id"),
        radar_state.get("last_run_id"),
    }
    if None in run_ids or len(run_ids) != 1:
        raise ValueError("STAGE_INPUT_MISMATCH")
    if scan_status.get("scan_status") != "MARKET_DATA_COMPLETE":
        raise ValueError("Stage A is not MARKET_DATA_COMPLETE")
    if scan_status.get("market_data_returned") != scan_status.get("mapped_total"):
        raise ValueError("Stage A coverage is incomplete")
    if snapshot.get("count") != scan_status.get("market_data_returned"):
        raise ValueError("Stage A snapshot count mismatch")
    if snapshot.get("generated_at") != scan_status.get("scan_finished_at"):
        raise ValueError("Stage A snapshot is stale")
    if triggers.get("generated_at") != snapshot.get("generated_at"):
        raise ValueError("STAGE_INPUT_MISMATCH")
    if radar_state.get("schema_version") != 1 or not isinstance(radar_state.get("assets"), dict):
        raise ValueError("radar_state.json is invalid")


def skipped_outputs(reason: str, stage_b_path: Path, events_path: Path) -> dict[str, Any]:
    timestamp = utc_now()
    atomic_write_json(
        stage_b_path,
        {
            "generated_at": timestamp,
            "status": "STAGE_B_SKIPPED",
            "reason": reason,
            "count": 0,
            "assets": [],
        },
    )
    atomic_write_json(
        events_path,
        {
            "generated_at": timestamp,
            "status": "STAGE_B_SKIPPED",
            "reason": reason,
            "count": 0,
            "events": [],
            "notification_count": 0,
            "notification_events": [],
        },
    )
    return {"exit_code": 2, "skipped": True, "reason": reason}


def run_stage_b(
    bootstrap: bool = False,
    *,
    config_path: Path = CONFIG_PATH,
    scan_status_path: Path = SCAN_STATUS_PATH,
    snapshot_path: Path = SNAPSHOT_PATH,
    triggers_path: Path = TRIGGERS_PATH,
    radar_state_path: Path = RADAR_STATE_PATH,
    technical_state_path: Path = TECHNICAL_STATE_PATH,
    stage_b_path: Path = STAGE_B_OUTPUT_PATH,
    events_path: Path = TECHNICAL_EVENTS_PATH,
    adapters_override: list[PublicSpotAdapter] | None = None,
    state_store: StateStore | None = None,
) -> dict[str, Any]:
    state_store = state_store or FileSystemStateStore()
    try:
        config = read_json(config_path)
        scan_status = read_json(scan_status_path)
        snapshot = read_json(snapshot_path)
        triggers = read_json(triggers_path)
        radar_state = state_store.load(radar_state_path)
        validate_stage_a(scan_status, snapshot, triggers, radar_state)
    except Exception as exc:
        return skipped_outputs(str(exc), stage_b_path, events_path)

    previous_assets: dict[str, dict[str, Any]] = {}
    if not bootstrap:
        try:
            missing = object()
            previous_state = state_store.load(technical_state_path, missing)
            if previous_state is missing:
                return skipped_outputs(
                    "Technical state is not initialized; run stage_b_enrich.py --bootstrap explicitly",
                    stage_b_path,
                    events_path,
                )
            if previous_state.get("schema_version") != 1 or not isinstance(
                previous_state.get("assets"), dict
            ):
                raise ValueError("technical_state.json is invalid")
            previous_assets = previous_state["assets"]
        except Exception as exc:
            return skipped_outputs(str(exc), stage_b_path, events_path)

    active_entries = {
        symbol: entry for symbol, entry in radar_state["assets"].items() if entry.get("active") is True
    }
    snapshot_map = {item["symbol"]: item for item in snapshot.get("items", [])}
    missing_active = sorted(set(active_entries) - set(snapshot_map))
    if missing_active:
        return skipped_outputs(
            f"Active assets missing from Stage A snapshot: {missing_active}", stage_b_path, events_path
        )

    try:
        adapter_list = adapters_override if adapters_override is not None else build_adapters(config)
    except Exception as exc:
        return skipped_outputs(str(exc), stage_b_path, events_path)
    forbidden = {str(item).casefold() for item in config.get("excluded_exchanges", [])}
    if any(adapter.name.casefold() in forbidden for adapter in adapter_list):
        return skipped_outputs("Excluded exchange adapter detected", stage_b_path, events_path)

    target_symbols = set(active_entries)
    allowed_quotes = set(config.get("allowed_quotes", ["USDT", "USD", "USDC"]))
    markets_by_symbol = {symbol: [] for symbol in target_symbols}
    adapter_map = {adapter.name: adapter for adapter in adapter_list}
    source_errors: dict[str, str] = {}
    for adapter in adapter_list:
        try:
            for market in adapter.discover(target_symbols, allowed_quotes):
                if market.base in markets_by_symbol:
                    markets_by_symbol[market.base].append(market)
        except Exception as exc:
            source_errors[adapter.name] = str(exc)

    timestamp = utc_now()
    candle_cache: dict[tuple[str, str, str], list[Candle]] = {}
    stage_b_assets = []
    next_assets = dict(previous_assets)
    all_events: list[dict[str, Any]] = []

    for symbol, radar_entry in active_entries.items():
        asset_started = time.perf_counter()
        try:
            record = enrich_asset(
                symbol,
                snapshot_map[symbol],
                radar_entry,
                markets_by_symbol[symbol],
                adapter_map,
                config,
                candle_cache,
            )
        except Exception:
            record = {
                "symbol": symbol,
                "price": snapshot_map[symbol].get("current_price"),
                "change_1h": snapshot_map[symbol].get("price_change_percentage_1h"),
                "change_24h": snapshot_map[symbol].get("price_change_percentage_24h"),
                "range_24h_pct": snapshot_map[symbol].get("range_24h_pct"),
                "turnover_ratio": snapshot_map[symbol].get("turnover_ratio"),
                "rsi_4h": None,
                "rsi_1d": None,
                "rsi_1w": None,
                "rsi_4h_state": "DATA_INSUFFICIENT",
                "volume_24h_quote": None,
                "volume_ratio": None,
                "volume_status": "DATA_INSUFFICIENT",
                "volume_pattern": "NONE",
                "volume_note": "NONE",
                "live_source": None,
                "history_source": None,
                "cross_exchange_status": "DATA_INSUFFICIENT",
                "cross_exchange_prices": [],
                "data_quality": "DATA_INSUFFICIENT",
            }
        record_asset_enrichment(symbol, time.perf_counter() - asset_started)
        stage_b_assets.append(record)

        previous = previous_assets.get(symbol)
        if previous is not None and previous.get("price_episode_id") != radar_entry.get("episode_id"):
            previous = None
        if record["data_quality"] == "DATA_INSUFFICIENT" and previous is not None:
            next_assets[symbol] = previous
            continue
        current_state = technical_state_entry(record, timestamp, radar_entry.get("episode_id"))
        next_assets[symbol] = current_state
        if not bootstrap:
            all_events.extend(evaluate_technical_events(symbol, previous, current_state))

    stage_b_assets.sort(key=lambda item: item["symbol"])
    notification_events = [item for item in all_events if item.get("notify") is True]
    quality_counts = {
        quality: sum(item["data_quality"] == quality for item in stage_b_assets)
        for quality in ("COMPLETE", "PARTIAL", "DATA_INSUFFICIENT")
    }
    run_id = scan_status["run_id"]
    state_payload = {
        "schema_version": 1,
        "last_run_id": run_id,
        "updated_at": timestamp,
        "assets": next_assets,
    }
    stage_b_payload = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "BOOTSTRAP" if bootstrap else "UPDATED",
        "active_assets_attempted": len(active_entries),
        "quality_counts": quality_counts,
        "source_errors": source_errors,
        "count": len(stage_b_assets),
        "assets": stage_b_assets,
    }
    events_payload = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "BOOTSTRAP_SUPPRESSED" if bootstrap else "UPDATED",
        "count": len(all_events),
        "events": all_events,
        "notification_count": len(notification_events),
        "notification_events": notification_events,
    }
    atomic_write_json(stage_b_path, stage_b_payload)
    atomic_write_json(events_path, events_payload)
    state_store.save_atomic(technical_state_path, state_payload)
    return {
        "exit_code": 0,
        "skipped": False,
        "bootstrap": bootstrap,
        "active": len(active_entries),
        "quality_counts": quality_counts,
        "technical_events": len(all_events),
        "notification_events": len(notification_events),
        "source_errors": source_errors,
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("ALTCOIN RADAR - STAGE B.1")
    print()
    if summary.get("skipped"):
        print("Stage B status: STAGE_B_SKIPPED")
        print(f"Reason: {summary['reason']}")
        return
    print(f"Active coins attempted: {summary['active']}")
    print(f"Complete: {summary['quality_counts']['COMPLETE']}")
    print(f"Partial: {summary['quality_counts']['PARTIAL']}")
    print(f"Data insufficient: {summary['quality_counts']['DATA_INSUFFICIENT']}")
    print(f"Technical events: {summary['technical_events']}")
    print(f"Technical notification events: {summary['notification_events']}")
    if summary.get("bootstrap"):
        print()
        print("Technical state initialized")
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
        summary = run_stage_b(bootstrap=args.bootstrap)
    except Exception as exc:
        summary = skipped_outputs(str(exc), STAGE_B_OUTPUT_PATH, TECHNICAL_EVENTS_PATH)
    print_summary(summary)
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())

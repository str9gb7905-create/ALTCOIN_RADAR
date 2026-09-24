"""ALTCOIN RADAR Stage A.2 persistent signal state machine."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from radar_scan import OUTPUT_DIR, PROJECT_ROOT, atomic_write_json, trigger_flags, utc_now
from state_store import FileSystemStateStore, StateStore


STATE_PATH = PROJECT_ROOT / "state" / "radar_state.json"
IDENTITY_MIGRATIONS_PATH = PROJECT_ROOT / "config" / "identity_migrations.json"
SCAN_STATUS_PATH = OUTPUT_DIR / "scan_status.json"
SNAPSHOT_PATH = OUTPUT_DIR / "latest_snapshot.json"
TRIGGERS_PATH = OUTPUT_DIR / "latest_triggers.json"
EVENTS_PATH = OUTPUT_DIR / "latest_events.json"
NOTIFICATIONS_PATH = OUTPUT_DIR / "latest_notification_candidates.json"

EVENT_TYPES = {
    "NEW_TRIGGER",
    "ESCALATION",
    "DIRECTION_CHANGE",
    "REENTRY",
    "EXIT",
    "CONTINUING",
    "NONE",
}
NOTIFICATION_EVENTS = {"NEW_TRIGGER", "ESCALATION", "DIRECTION_CHANGE", "REENTRY"}
TIER_RANK = {"T0": 0, "T1": 1, "T2": 2, "T3": 3, "T4": 4, "T5": 5}
FLAG_CONDITIONS = (
    ("trigger_1h_up", "1H_UP"),
    ("trigger_1h_down", "1H_DOWN"),
    ("trigger_24h_up", "24H_UP"),
    ("trigger_24h_down", "24H_DOWN"),
)
RANGE_24H_METHOD = "RANGE_24H_EXTREMA"
PRICE_HISTORY_RETENTION_SECONDS = 24 * 60 * 60
MAX_TRIGGER_LEVEL_PERCENT = 1000


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def numeric(value: Any) -> float | int | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def parse_utc_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def update_price_history(
    previous: dict[str, Any] | None,
    price: Any,
    sample_time: str,
) -> list[dict[str, Any]]:
    """Append one scan price and retain the latest 24 hours of 15-minute samples."""
    current_price = numeric(price)
    current_time = parse_utc_timestamp(sample_time)
    if current_price is None or current_price <= 0 or current_time is None:
        return list((previous or {}).get("price_history", []))

    samples_by_time: dict[str, dict[str, Any]] = {}
    for item in (previous or {}).get("price_history", []):
        if not isinstance(item, dict):
            continue
        item_time = parse_utc_timestamp(item.get("timestamp"))
        item_price = numeric(item.get("price"))
        if item_time is None or item_price is None or item_price <= 0:
            continue
        age = (current_time - item_time).total_seconds()
        if 0 <= age <= PRICE_HISTORY_RETENTION_SECONDS:
            normalized = item_time.isoformat().replace("+00:00", "Z")
            samples_by_time[normalized] = {"timestamp": normalized, "price": item_price}

    normalized_time = current_time.isoformat().replace("+00:00", "Z")
    samples_by_time[normalized_time] = {"timestamp": normalized_time, "price": current_price}
    return sorted(samples_by_time.values(), key=lambda item: item["timestamp"])


def extrema_price_move(
    history: list[dict[str, Any]], previous_direction: str | None = None
) -> dict[str, Any]:
    """Measure the current price from the recent low and high, selecting the larger move."""
    if not history:
        return {
            "change_pct": None,
            "direction": "NONE",
            "reference_price": None,
            "reference_time": None,
            "recent_high": None,
            "recent_low": None,
        }

    current = history[-1]
    current_price = float(current["price"])
    recent_low = min(history, key=lambda item: (float(item["price"]), item["timestamp"]))
    recent_high = max(history, key=lambda item: (float(item["price"]), item["timestamp"]))
    up_change = (current_price / float(recent_low["price"]) - 1.0) * 100.0
    down_change = (current_price / float(recent_high["price"]) - 1.0) * 100.0

    if abs(up_change) == abs(down_change) and previous_direction in {"UP", "DOWN"}:
        direction = previous_direction
    else:
        direction = "UP" if abs(up_change) >= abs(down_change) else "DOWN"
    change = up_change if direction == "UP" else down_change
    reference = recent_low if direction == "UP" else recent_high
    return {
        "change_pct": change,
        "direction": direction,
        "reference_price": reference["price"],
        "reference_time": reference["timestamp"],
        "recent_high": recent_high["price"],
        "recent_low": recent_low["price"],
    }


def trigger_level(change_pct: Any) -> int:
    value = numeric(change_pct)
    if value is None or abs(float(value)) < 10:
        return 0
    return min(int(abs(float(value)) // 10) * 10, MAX_TRIGGER_LEVEL_PERCENT)


def abnormality_score(change_1h: Any, change_24h: Any) -> float:
    values = [abs(float(value)) for value in (change_1h, change_24h) if numeric(value) is not None]
    return max(values, default=0.0)


def severity_tier(score: float) -> str:
    if score >= 100:
        return "T5"
    if score >= 50:
        return "T4"
    if score >= 30:
        return "T3"
    if score >= 20:
        return "T2"
    if score >= 10:
        return "T1"
    return "T0"


def direction_from_conditions(conditions: list[str]) -> str:
    has_up = any(condition.endswith("_UP") for condition in conditions)
    has_down = any(condition.endswith("_DOWN") for condition in conditions)
    if has_up and has_down:
        return "MIXED"
    if has_up:
        return "UP"
    if has_down:
        return "DOWN"
    return "NONE"


def conditions_from_flags(flags: dict[str, Any]) -> list[str]:
    return [condition for flag, condition in FLAG_CONDITIONS if flags.get(flag) is True]


def signal_from_changes(
    symbol: str,
    change_1h: float | int | None,
    change_24h: float | int | None,
    price: float | int | None = None,
) -> dict[str, Any]:
    record = {
        "symbol": symbol,
        "price_change_percentage_1h": change_1h,
        "price_change_percentage_24h": change_24h,
    }
    conditions = conditions_from_flags(trigger_flags(record))
    score = abnormality_score(change_1h, change_24h)
    return {
        "symbol": symbol,
        "active": bool(conditions),
        "direction": direction_from_conditions(conditions),
        "severity_tier": severity_tier(score),
        "abnormality_score": score,
        "active_conditions": conditions,
        "price": price,
        "change_1h": change_1h,
        "change_24h": change_24h,
    }


def signal_from_outputs(snapshot: dict[str, Any], trigger: dict[str, Any] | None) -> dict[str, Any]:
    conditions = conditions_from_flags(trigger or {})
    change_1h = numeric(snapshot.get("price_change_percentage_1h"))
    change_24h = numeric(snapshot.get("price_change_percentage_24h"))
    score = abnormality_score(change_1h, change_24h)
    return {
        "symbol": snapshot["symbol"],
        "coingecko_id": snapshot.get("coingecko_id"),
        "active": bool(conditions),
        "direction": direction_from_conditions(conditions),
        "severity_tier": severity_tier(score),
        "abnormality_score": score,
        "active_conditions": conditions,
        "price": numeric(snapshot.get("current_price")),
        "change_1h": change_1h,
        "change_24h": change_24h,
    }


def one_hour_signal(current: dict[str, Any]) -> dict[str, Any]:
    """Build the notification signal; production uses the recent 24-hour range."""
    range_mode = "trigger_change_pct" in current
    change_1h = numeric(
        current.get("trigger_change_pct") if range_mode else current.get("change_1h")
    )
    if range_mode:
        level = trigger_level(change_1h)
        direction = current.get("trigger_direction") if level else "NONE"
        condition = {
            "UP": "FROM_24H_LOW_UP",
            "DOWN": "FROM_24H_HIGH_DOWN",
        }.get(direction)
        signal = {
            "symbol": current["symbol"],
            "active": level > 0 and condition is not None,
            "direction": direction,
            "severity_tier": severity_tier(abs(float(change_1h or 0))),
            "abnormality_score": abs(float(change_1h or 0)),
            "active_conditions": [condition] if condition else [],
            "price": current.get("price"),
            "change_1h": change_1h,
            "change_24h": None,
            "change_pct": change_1h,
            "threshold_level": level,
        }
    else:
        signal = signal_from_changes(
            current["symbol"], change_1h, None, current.get("price")
        )
        signal["threshold_level"] = trigger_level(change_1h)
    signal["coingecko_id"] = current.get("coingecko_id")
    signal["calculation_method"] = RANGE_24H_METHOD if range_mode else "SOURCE_1H"
    signal["reference_price"] = current.get("trigger_reference_price")
    signal["reference_time"] = current.get("trigger_reference_time")
    signal["window_end_time"] = current.get("trigger_current_time")
    return signal


def previous_one_hour_state(
    previous: dict[str, Any] | None,
    symbol: str,
    calculation_method: str,
    state_key: str = "notification_1h",
) -> dict[str, Any] | None:
    if previous is None:
        return None
    existing = previous.get(state_key)
    if isinstance(existing, dict) and isinstance(existing.get("active"), bool):
        if calculation_method == RANGE_24H_METHOD and existing.get(
            "calculation_method"
        ) != RANGE_24H_METHOD:
            return None
        return existing
    # Migrate the already-persisted combined state without repeating an active 1H alert.
    legacy = one_hour_signal({"symbol": symbol, **previous})
    legacy["episode_id"] = 1 if legacy["active"] else 0
    return legacy


def classify_event(previous: dict[str, Any] | None, current: dict[str, Any]) -> str:
    if previous is None:
        return "NEW_TRIGGER" if current["active"] else "NONE"

    was_active = previous.get("active") is True
    is_active = current["active"] is True
    if not was_active and is_active:
        return "REENTRY" if int(previous.get("episode_id", 0)) > 0 else "NEW_TRIGGER"
    if was_active and not is_active:
        return "EXIT"
    if not was_active and not is_active:
        return "NONE"

    if previous.get("direction") != current["direction"]:
        return "DIRECTION_CHANGE"
    previous_level = int(previous.get("threshold_level", 0) or 0)
    current_level = int(current.get("threshold_level", 0) or 0)
    if current_level > previous_level:
        return "ESCALATION"
    previous_rank = TIER_RANK.get(previous.get("severity_tier"), -1)
    current_rank = TIER_RANK[current["severity_tier"]]
    if current_rank > previous_rank:
        return "ESCALATION"
    return "CONTINUING"


def build_state_entry(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    event: str,
    timestamp: str,
    bootstrap: bool = False,
) -> dict[str, Any]:
    previous = previous or {}
    previous_episode = int(previous.get("episode_id", 0))
    if bootstrap:
        episode_id = 1 if current["active"] else 0
    elif event == "REENTRY":
        episode_id = previous_episode + 1
    elif event == "NEW_TRIGGER":
        episode_id = max(previous_episode, 0) + 1
    else:
        episode_id = previous_episode

    first_triggered_at = previous.get("first_triggered_at")
    if current["active"] and first_triggered_at is None:
        first_triggered_at = timestamp

    last_exit_at = previous.get("last_exit_at")
    if event == "EXIT":
        last_exit_at = timestamp

    last_notified_at = previous.get("last_notified_at")
    if not bootstrap and event in NOTIFICATION_EVENTS:
        last_notified_at = timestamp

    return {
        "coingecko_id": current.get("coingecko_id"),
        "active": current["active"],
        "direction": current["direction"],
        "severity_tier": current["severity_tier"],
        "abnormality_score": current["abnormality_score"],
        "active_conditions": current["active_conditions"],
        "price": current["price"],
        "change_1h": current["change_1h"],
        "change_24h": current["change_24h"],
        "episode_id": episode_id,
        "first_triggered_at": first_triggered_at,
        "last_seen_at": timestamp,
        "last_notified_at": last_notified_at,
        "last_exit_at": last_exit_at,
        "last_event": "NONE" if bootstrap else event,
        "baseline_required": bootstrap,
        **{
            key: current.get(key)
            for key in (
                "calculation_method",
                "reference_price",
                "reference_time",
                "window_end_time",
                "change_pct",
                "threshold_level",
            )
            if key in current
        },
    }


def load_identity_migrations(path: Path = IDENTITY_MIGRATIONS_PATH) -> dict[str, dict[str, str]]:
    payload = read_json(path)
    if payload.get("schema_version") != 1 or not isinstance(payload.get("migrations"), list):
        raise ValueError("identity_migrations.json is invalid")
    migrations: dict[str, dict[str, str]] = {}
    for item in payload["migrations"]:
        if not isinstance(item, dict):
            raise ValueError("identity_migrations.json contains a non-object migration")
        symbol = str(item.get("symbol", "")).upper()
        before = item.get("from_coingecko_id")
        after = item.get("to_coingecko_id")
        if not symbol or not isinstance(before, str) or not isinstance(after, str):
            raise ValueError("identity_migrations.json contains an incomplete migration")
        if symbol in migrations:
            raise ValueError(f"duplicate identity migration: {symbol}")
        migrations[symbol] = {"from": before, "to": after}
    return migrations


def identity_requires_baseline(
    symbol: str,
    previous: dict[str, Any] | None,
    current_coingecko_id: str | None,
    migrations: dict[str, dict[str, str]],
) -> bool:
    if previous is None:
        return True
    previous_id = previous.get("coingecko_id")
    if previous_id is not None:
        return previous_id != current_coingecko_id
    migration = migrations.get(symbol)
    return bool(migration and migration["to"] == current_coingecko_id)


def evaluate_transition(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    timestamp: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    event = classify_event(previous, current)
    if event not in EVENT_TYPES:
        raise ValueError(f"unsupported event: {event}")
    state_entry = build_state_entry(previous, current, event, timestamp)
    current_alert = one_hour_signal(current)
    alert_state_key = (
        "notification_move"
        if current_alert["calculation_method"] == RANGE_24H_METHOD
        else "notification_1h"
    )
    previous_alert = previous_one_hour_state(
        previous,
        current["symbol"],
        current_alert["calculation_method"],
        alert_state_key,
    )
    alert_event = classify_event(previous_alert, current_alert)
    alert_state = build_state_entry(previous_alert, current_alert, alert_event, timestamp)
    state_entry[alert_state_key] = alert_state
    event_record = {
        "symbol": current["symbol"],
        "event": event,
        "previous_tier": previous.get("severity_tier") if previous else None,
        "current_tier": current["severity_tier"],
        "previous_direction": previous.get("direction") if previous else "NONE",
        "current_direction": current["direction"],
        "previous_score": previous.get("abnormality_score") if previous else None,
        "current_score": current["abnormality_score"],
        "active_conditions": current["active_conditions"],
        "episode_id": state_entry["episode_id"],
    }
    candidate = None
    if alert_event in NOTIFICATION_EVENTS:
        candidate = {
            "symbol": current["symbol"],
            "event": alert_event,
            "price": current["price"],
            "change_1h": current_alert["change_1h"],
            "change_pct": current_alert.get("change_pct", current_alert["change_1h"]),
            "coingecko_change_1h": current.get("coingecko_change_1h"),
            "change_24h": current["change_24h"],
            "abnormality_score": current_alert["abnormality_score"],
            "severity_tier": current_alert["severity_tier"],
            "direction": current_alert["direction"],
            "active_conditions": current_alert["active_conditions"],
            "episode_id": alert_state["episode_id"],
            "calculation_method": current_alert["calculation_method"],
            "threshold_level": current_alert.get("threshold_level", 0),
            "reference_price": current_alert.get("reference_price"),
            "reference_time": current_alert.get("reference_time"),
            "window_end_time": current_alert.get("window_end_time"),
        }
    state_entry["last_notified_at"] = (
        timestamp if candidate is not None else previous.get("last_notified_at") if previous else None
    )
    return state_entry, event_record, candidate


def skipped_payload(
    reason: str, timestamp: str, run_id: str | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    events = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "STATE_UPDATE_SKIPPED",
        "reason": reason,
        "count": 0,
        "events": [],
    }
    notifications = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "STATE_UPDATE_SKIPPED",
        "reason": reason,
        "count": 0,
        "assets": [],
    }
    return events, notifications


def write_skipped(
    reason: str,
    events_path: Path,
    notifications_path: Path,
    run_id: str | None = None,
) -> dict[str, Any]:
    timestamp = utc_now()
    events, notifications = skipped_payload(reason, timestamp, run_id)
    atomic_write_json(events_path, events)
    atomic_write_json(notifications_path, notifications)
    return {"exit_code": 2, "skipped": True, "reason": reason}


def validate_complete_outputs(
    scan_status: dict[str, Any],
    snapshot: dict[str, Any],
    triggers: dict[str, Any],
) -> None:
    run_ids = {
        scan_status.get("run_id"),
        snapshot.get("run_id"),
        triggers.get("run_id"),
    }
    if None in run_ids or len(run_ids) != 1:
        raise ValueError("STAGE_INPUT_MISMATCH: Stage A run_id values do not match")
    if scan_status.get("scan_status") != "MARKET_DATA_COMPLETE":
        raise ValueError(f"scan status is {scan_status.get('scan_status')!r}")
    returned = scan_status.get("market_data_returned")
    mapped = scan_status.get("mapped_total")
    if returned != mapped:
        raise ValueError(f"market_data_returned ({returned}) != mapped_total ({mapped})")
    snapshot_items = snapshot.get("items")
    trigger_items = triggers.get("items")
    if not isinstance(snapshot_items, list) or snapshot.get("count") != len(snapshot_items):
        raise ValueError("snapshot count is invalid")
    if len(snapshot_items) != returned:
        raise ValueError("snapshot item count does not match completed scan")
    if not isinstance(trigger_items, list) or triggers.get("count") != len(trigger_items):
        raise ValueError("trigger count is invalid")
    if snapshot.get("generated_at") != triggers.get("generated_at"):
        raise ValueError("snapshot and trigger timestamps do not match")
    if snapshot.get("generated_at") != scan_status.get("scan_finished_at"):
        raise ValueError("scan output timestamp does not match scan status")

    snapshot_symbols = [item.get("symbol") for item in snapshot_items]
    trigger_symbols = [item.get("symbol") for item in trigger_items]
    if None in snapshot_symbols or len(snapshot_symbols) != len(set(snapshot_symbols)):
        raise ValueError("snapshot symbols are missing or duplicated")
    if None in trigger_symbols or len(trigger_symbols) != len(set(trigger_symbols)):
        raise ValueError("trigger symbols are missing or duplicated")
    unknown_triggers = sorted(set(trigger_symbols) - set(snapshot_symbols))
    if unknown_triggers:
        raise ValueError(f"triggers absent from snapshot: {unknown_triggers}")


def run_state_update(
    bootstrap: bool = False,
    *,
    scan_status_path: Path = SCAN_STATUS_PATH,
    snapshot_path: Path = SNAPSHOT_PATH,
    triggers_path: Path = TRIGGERS_PATH,
    state_path: Path = STATE_PATH,
    events_path: Path = EVENTS_PATH,
    notifications_path: Path = NOTIFICATIONS_PATH,
    identity_migrations_path: Path = IDENTITY_MIGRATIONS_PATH,
    state_store: StateStore | None = None,
) -> dict[str, Any]:
    state_store = state_store or FileSystemStateStore()
    try:
        scan_status = read_json(scan_status_path)
        if scan_status.get("scan_status") != "MARKET_DATA_COMPLETE" or (
            scan_status.get("market_data_returned") != scan_status.get("mapped_total")
        ):
            return write_skipped(
                "Stage A scan is not complete; persistent state was not modified",
                events_path,
                notifications_path,
                scan_status.get("run_id"),
            )
        snapshot = read_json(snapshot_path)
        triggers = read_json(triggers_path)
        identity_migrations = load_identity_migrations(identity_migrations_path)
        validate_complete_outputs(scan_status, snapshot, triggers)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return write_skipped(str(exc), events_path, notifications_path)

    previous_assets: dict[str, dict[str, Any]] = {}
    if not bootstrap:
        try:
            missing = object()
            previous_state = state_store.load(state_path, missing)
            if previous_state is missing:
                return write_skipped(
                    "State is not initialized; run radar_state.py --bootstrap explicitly",
                    events_path,
                    notifications_path,
                    scan_status.get("run_id"),
                )
            if previous_state.get("schema_version") != 1 or not isinstance(
                previous_state.get("assets"), dict
            ):
                raise ValueError("persistent state schema is invalid")
            previous_assets = previous_state["assets"]
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            return write_skipped(
                str(exc), events_path, notifications_path, scan_status.get("run_id")
            )

    timestamp = utc_now()
    sample_timestamp = snapshot["generated_at"]
    trigger_map = {item["symbol"]: item for item in triggers["items"]}
    next_assets: dict[str, dict[str, Any]] = {}
    events: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    baselined_symbols: list[str] = []

    for snapshot_item in snapshot["items"]:
        symbol = snapshot_item["symbol"]
        current = signal_from_outputs(snapshot_item, trigger_map.get(symbol))
        previous = previous_assets.get(symbol)
        selective_baseline = identity_requires_baseline(
            symbol, previous, current.get("coingecko_id"), identity_migrations
        )
        history_source = None if selective_baseline else previous
        price_history = update_price_history(
            history_source, current.get("price"), sample_timestamp
        )
        previous_move = (previous or {}).get("notification_move") or {}
        price_move = extrema_price_move(
            price_history, previous_move.get("direction")
        )
        current["coingecko_change_1h"] = current.get("change_1h")
        current["trigger_change_pct"] = price_move["change_pct"]
        current["trigger_direction"] = price_move["direction"]
        current["trigger_reference_price"] = price_move["reference_price"]
        current["trigger_reference_time"] = price_move["reference_time"]
        current["trigger_current_time"] = sample_timestamp
        if bootstrap or selective_baseline:
            next_assets[symbol] = build_state_entry(
                None, current, "NONE", timestamp, bootstrap=True
            )
            next_assets[symbol]["notification_move"] = build_state_entry(
                None, one_hour_signal(current), "NONE", timestamp, bootstrap=True
            )
            next_assets[symbol]["price_history"] = price_history
            next_assets[symbol]["trigger_change_pct"] = price_move["change_pct"]
            next_assets[symbol]["recent_high"] = price_move["recent_high"]
            next_assets[symbol]["recent_low"] = price_move["recent_low"]
            next_assets[symbol]["coingecko_change_1h"] = current.get(
                "coingecko_change_1h"
            )
            baselined_symbols.append(symbol)
            continue
        next_state, event_record, candidate = evaluate_transition(
            previous, current, timestamp
        )
        next_state["baseline_required"] = False
        next_state["price_history"] = price_history
        next_state["trigger_change_pct"] = price_move["change_pct"]
        next_state["recent_high"] = price_move["recent_high"]
        next_state["recent_low"] = price_move["recent_low"]
        next_state["coingecko_change_1h"] = current.get("coingecko_change_1h")
        next_assets[symbol] = next_state
        events.append(event_record)
        if candidate is not None:
            candidates.append(candidate)

    candidates.sort(key=lambda item: item["abnormality_score"], reverse=True)
    run_id = scan_status["run_id"]
    state_payload = {
        "schema_version": 1,
        "last_run_id": run_id,
        "updated_at": timestamp,
        "assets": next_assets,
    }
    event_payload = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "BOOTSTRAP" if bootstrap else "UPDATED",
        "count": len(events),
        "events": events,
    }
    notification_payload = {
        "run_id": run_id,
        "generated_at": timestamp,
        "status": "BOOTSTRAP_SUPPRESSED" if bootstrap else "UPDATED",
        "count": len(candidates),
        "assets": candidates,
    }

    # State is replaced last so a failed output write cannot consume an event silently.
    atomic_write_json(events_path, event_payload)
    atomic_write_json(notifications_path, notification_payload)
    state_store.save_atomic(state_path, state_payload)

    counts = {event: 0 for event in EVENT_TYPES}
    for item in events:
        counts[item["event"]] += 1
    return {
        "exit_code": 0,
        "skipped": False,
        "bootstrap": bootstrap,
        "scan_status": scan_status["scan_status"],
        "assets_evaluated": len(next_assets),
        "active": sum(item["active"] for item in next_assets.values()),
        "notification_candidates": len(candidates),
        "assets_baselined": len(baselined_symbols),
        "baselined_symbols": sorted(baselined_symbols),
        "counts": counts,
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("ALTCOIN RADAR - STATE")
    print()
    if summary.get("skipped"):
        print("State update: STATE_UPDATE_SKIPPED")
        print(f"Reason: {summary['reason']}")
        return
    print(f"Scan status: {summary['scan_status']}")
    print(f"Assets evaluated: {summary['assets_evaluated']}")
    print()
    if summary.get("bootstrap"):
        print("State initialized")
        print("Notifications suppressed")
        print(f"Currently active: {summary['active']}")
        print("Notification candidates: 0")
        return
    counts = summary["counts"]
    print(f"New triggers: {counts['NEW_TRIGGER']}")
    print(f"Escalations: {counts['ESCALATION']}")
    print(f"Direction changes: {counts['DIRECTION_CHANGE']}")
    print(f"Reentries: {counts['REENTRY']}")
    print(f"Exits: {counts['EXIT']}")
    print(f"Continuing: {counts['CONTINUING']}")
    print()
    print(f"Notification candidates: {summary['notification_candidates']}")
    if summary["notification_candidates"] == 0:
        print("NO NEW NOTIFICATION EVENT")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="explicitly initialize state and suppress current notifications",
    )
    args = parser.parse_args()
    try:
        summary = run_state_update(bootstrap=args.bootstrap)
    except Exception as exc:
        summary = write_skipped(str(exc), EVENTS_PATH, NOTIFICATIONS_PATH)
    print_summary(summary)
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())

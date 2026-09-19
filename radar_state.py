"""ALTCOIN RADAR Stage A.2 persistent signal state machine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from radar_scan import OUTPUT_DIR, PROJECT_ROOT, atomic_write_json, trigger_flags, utc_now
from state_store import FileSystemStateStore, StateStore


STATE_PATH = PROJECT_ROOT / "state" / "radar_state.json"
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


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def numeric(value: Any) -> float | int | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


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
        "active": bool(conditions),
        "direction": direction_from_conditions(conditions),
        "severity_tier": severity_tier(score),
        "abnormality_score": score,
        "active_conditions": conditions,
        "price": numeric(snapshot.get("current_price")),
        "change_1h": change_1h,
        "change_24h": change_24h,
    }


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
    }


def evaluate_transition(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    timestamp: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    event = classify_event(previous, current)
    if event not in EVENT_TYPES:
        raise ValueError(f"unsupported event: {event}")
    state_entry = build_state_entry(previous, current, event, timestamp)
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
    if event in NOTIFICATION_EVENTS:
        candidate = {
            "symbol": current["symbol"],
            "event": event,
            "price": current["price"],
            "change_1h": current["change_1h"],
            "change_24h": current["change_24h"],
            "abnormality_score": current["abnormality_score"],
            "severity_tier": current["severity_tier"],
            "direction": current["direction"],
            "active_conditions": current["active_conditions"],
            "episode_id": state_entry["episode_id"],
        }
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
    trigger_map = {item["symbol"]: item for item in triggers["items"]}
    next_assets: dict[str, dict[str, Any]] = {}
    events: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []

    for snapshot_item in snapshot["items"]:
        symbol = snapshot_item["symbol"]
        current = signal_from_outputs(snapshot_item, trigger_map.get(symbol))
        if bootstrap:
            next_assets[symbol] = build_state_entry(None, current, "NONE", timestamp, bootstrap=True)
            continue
        next_state, event_record, candidate = evaluate_transition(
            previous_assets.get(symbol), current, timestamp
        )
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
